# Copyright (C) 2005-2009 Jelmer Vernooij <jelmer@samba.org>
 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Subversion Meta-Revisions. This is where all the magic happens. """

from subvertpy import (
        properties,
        )

from bzrlib import (
        errors as bzr_errors,
        ui,
        )
from bzrlib.foreign import (
        ForeignRevision,
        )
from bzrlib.revision import (
        NULL_REVISION, 
        )
from bzrlib.trace import (
        warning,
        )
from bzrlib.plugins.svn import (
        changes, 
        errors as svn_errors, 
        logwalker,
        )
from bzrlib.plugins.svn.logwalker import (
        CachingLogWalker,
        )
from bzrlib.plugins.svn.mapping import (
        estimate_bzr_ancestors, 
        find_mapping_fileprops,
        find_mapping_revprops,
        get_roundtrip_ancestor_revids,
        is_bzr_revision_revprops, 
        is_bzr_revision_fileprops, 
        parse_svn_revprops,
        SVN_REVPROP_BZR_SIGNATURE, 
        SVN_PROP_BZR_REVPROP_REDIRECT,
        SVN_REVPROP_BZR_ROOT,
        )
from bzrlib.plugins.svn.svk import (
        estimate_svk_ancestors,
        parse_svk_feature, 
        svk_features_merged_since, 
        SVN_PROP_SVK_MERGE, 
        )

import bisect
from collections import defaultdict, deque
from functools import partial
from itertools import ifilter, imap


_warned_slow_revprops = False

def warn_slow_revprops():
    global _warned_slow_revprops
    if not _warned_slow_revprops:
        warning("Upgrade to Subversion 1.5 or higher for faster retrieving of revision properties.")
        _warned_slow_revprops = True


class MetaHistoryIncomplete(Exception):
    """No revision metadata branch."""


def full_paths(find_children, paths, bp, from_bp, from_rev):
    """Generate the changes creating a specified branch path.

    :param find_children: Function that recursively lists all children 
                          of a path in a revision.
    :param paths: Paths dictionary to update
    :param bp: Branch path to create.
    :param from_bp: Path to look up children in
    :param from_rev: Revision to look up children in.
    """
    pb = ui.ui_factory.nested_progress_bar()
    try:
        for c in find_children(from_bp, from_rev, pb):
            paths[changes.rebase_path(c, from_bp, bp)] = ('A', None, -1)
    finally:
        pb.finished()
    return paths


class RevisionMetadata(object):
    """Object describing a revision with bzr semantics in a Subversion 
    repository.
    
    Tries to be as lazy as possible - data is not retrieved or calculated 
    from other known data before contacting the Subversions server.
    """

    def __init__(self, repository, check_revprops, get_fileprops_fn, logwalker, 
                 uuid, branch_path, revnum, paths, revprops, 
                 changed_fileprops=None, fileprops=None, 
                 metaiterator=None):
        self.repository = repository
        self.check_revprops = check_revprops
        self._get_fileprops_fn = get_fileprops_fn
        self._log = logwalker
        self.branch_path = branch_path
        self._paths = paths
        self.revnum = revnum
        self._revprops = revprops
        self._changed_fileprops = changed_fileprops
        self._fileprops = fileprops
        self._is_bzr_revision = None
        self._direct_lhs_parent_known = False
        self._consider_bzr_fileprops = None
        self._consider_bzr_revprops = None
        self._consider_svk_fileprops = None
        self._estimated_fileprop_ancestors = {}
        self.metaiterators = set()
        if metaiterator is not None:
            self.metaiterators.add(metaiterator)
        self.uuid = uuid
        self.children = set()

    def __eq__(self, other):
        return (type(self) == type(other) and 
                self.get_foreign_revid() == other.get_foreign_revid())

    def __ne__(self, other):
        return not self.__eq__(other)

    def __cmp__(self, other):
        return cmp((self.uuid, self.revnum, self.branch_path),
                   (other.uuid, other.revnum, other.branch_path))

    def __repr__(self):
        return "<RevisionMetadata for revision %d, path %s in repository %r>" % (self.revnum, self.branch_path, self.uuid)

    def changes_branch_root(self):
        """Check whether the branch root was modified in this revision.
        """
        if self.knows_changed_fileprops():
            return self.get_changed_fileprops() != {}
        return self.branch_path in self.get_paths()

    def get_foreign_revid(self):
        """Return the foreign revision id for this revision.
        
        :return: Tuple with uuid, branch path and revision number.
        """
        return (self.uuid, self.branch_path, self.revnum)

    def get_paths(self, mapping=None):
        """Fetch the changed paths dictionary for this revision.
        """
        if self._paths is None:
            self._paths = self._log.get_revision_paths(self.revnum)
        if mapping is not None and mapping.restricts_branch_paths:
            next = changes.find_prev_location(self._paths, self.branch_path, 
                                              self.revnum)
            if next is not None and not mapping.is_branch_or_tag(next[0]):
                # Make it look like the branch started here if the mapping 
                # (anything < v4) restricts what paths can be valid branches
                paths = dict(self._paths.items())
                lazypaths = logwalker.lazy_dict(paths, full_paths, 
                    self.repository.find_children, paths, 
                    self.branch_path, next[0], next[1])
                paths[self.branch_path] = ('A', None, -1)
                return lazypaths
        return self._paths

    def get_revision_id(self, mapping):
        """Determine the revision id for this revision.
        """
        if mapping.roundtripping:
            # See if there is a bzr:revision-id revprop set
            (_, revid) = self._import_from_props(mapping, 
                    mapping.get_revision_id_fileprops,
                    mapping.get_revision_id_revprops,
                    (None, None), self.consider_bzr_fileprops)
        else:
            revid = None

        # Or generate it
        if revid is None:
            return mapping.revision_id_foreign_to_bzr(self.get_foreign_revid())

        return revid

    def get_tag_revmeta(self, mapping):
        """Return the revmeta object for the revision that should be 
        referenced when this revision is considered a tag.
        
        This will return the original location if a tag was created by 
        copying from another branch into a tag path and there were 
        no additional changes. Doing this makes it more likely that tags 
        are on the mainline and therefore show up with a revision 
        number in 'bzr tags'.
        """
        if self.changes_branch_root():
            # This tag was (recreated) here, so unless anything else under this 
            # tag changed
            if not changes.changes_children(self.get_paths(), self.branch_path):
                lhs_parent_revmeta = self.get_lhs_parent_revmeta(mapping)
                if lhs_parent_revmeta is not None:
                    return lhs_parent_revmeta
        return self

    def get_fileprops(self):
        """Get the file properties set on the branch root.
        """
        if self._fileprops is None:
            # Argh, this should've just been a simple tail recursion call, but 
            # Python doesn't handle those very well..
            lm = self
            todo = set()
            while not lm.changes_branch_root() and lm._fileprops is None:
                todo.add(lm)
                lm = lm.get_direct_lhs_parent_revmeta()
            if lm._fileprops is None:
                lm._fileprops = self._get_fileprops_fn(lm.branch_path, 
                                                       lm.revnum)
            for r in todo:
                r._fileprops = lm._fileprops
        return self._fileprops

    def get_revprops(self):
        """Get the revision properties set on the revision."""
        if self._revprops is None:
            self._revprops = self._log.revprop_list(self.revnum)

        return self._revprops

    def knows_changed_fileprops(self):
        """Check whether the changed file properties can be cheaply retrieved."""
        if self._changed_fileprops is None:
            return False
        changed_fileprops = self.get_changed_fileprops()
        return isinstance(changed_fileprops, dict) or changed_fileprops.is_loaded

    def knows_fileprops(self):
        """Check whether the file properties can be cheaply retrieved."""
        fileprops = self.get_fileprops()
        return isinstance(fileprops, dict) or fileprops.is_loaded

    def knows_revprops(self):
        """Check whether all revision properties can be cheaply retrieved."""
        revprops = self.get_revprops()
        return isinstance(revprops, dict) or revprops.is_loaded

    def get_previous_fileprops(self):
        """Return the file properties set on the branch root before this 
        revision."""
        prev = self.get_direct_lhs_parent_revmeta()
        if prev is None:
            return {}
        return prev.get_fileprops()

    def get_changed_fileprops(self):
        """Determine the file properties changed in this revision."""
        if self._changed_fileprops is None:
            if self.changes_branch_root():
                self._changed_fileprops = logwalker.lazy_dict({}, properties.diff, self.get_fileprops(), self.get_previous_fileprops())
            else:
                self._changed_fileprops = {}
        return self._changed_fileprops

    def _set_direct_lhs_parent_revmeta(self, parent_revmeta):
        """Set the direct left-hand side parent. 

        :note: Should only be called once, later callers are only allowed 
            to specify the same lhs parent.
        """
        if (self._direct_lhs_parent_known and 
            self._direct_lhs_parent_revmeta != parent_revmeta):
            raise AssertionError("Tried registering %r as parent while %r already was parent for %r" % (parent_revmeta, self._direct_lhs_parent_revmeta, self))
        self._direct_lhs_parent_known = True
        self._direct_lhs_parent_revmeta = parent_revmeta
        if parent_revmeta is not None:
            parent_revmeta.children.add(self)

    def get_direct_lhs_parent_revmeta(self):
        """Find the direct left hand side parent of this revision.
        
        This ignores mapping restrictions (invalid paths, hidden revisions).
        """
        if self._direct_lhs_parent_known:
            return self._direct_lhs_parent_revmeta
        for metaiterator in self.metaiterators:
            # Perhaps the metaiterator already has the parent?
            try:
                self._direct_lhs_parent_revmeta = metaiterator.get_lhs_parent(self)
                self._direct_lhs_parent_known = True
                return self._direct_lhs_parent_revmeta
            except StopIteration:
                self._direct_lhs_parent_revmeta = None
                self._direct_lhs_parent_known = True
                return self._direct_lhs_parent_revmeta
            except MetaHistoryIncomplete:
                pass
        iterator = self.repository._revmeta_provider.iter_reverse_branch_changes(self.branch_path, 
            self.revnum, to_revnum=0, limit=500)
        firstrevmeta = iterator.next()
        assert self == firstrevmeta
        try:
            self._direct_lhs_parent_revmeta = iterator.next()
        except StopIteration:
            self._direct_lhs_parent_revmeta = None
        self._direct_lhs_parent_known = True
        return self._direct_lhs_parent_revmeta

    def get_lhs_parent_revmeta(self, mapping):
        """Get the revmeta object for the left hand side parent.

        :note: Returns None when there is no parent (parent is NULL_REVISION)
        """
        assert mapping.is_branch_or_tag(self.branch_path), \
                "%s not valid in %r" % (self.branch_path, mapping)
        def get_next_parent(nm):
            pm = nm.get_direct_lhs_parent_revmeta()
            if pm is None or mapping.is_branch_or_tag(pm.branch_path):
                return pm
            else:
                return None
        nm = get_next_parent(self)
        while nm is not None and nm.is_hidden(mapping):
            nm = get_next_parent(nm)
        return nm

    def get_appropriate_mappings(self, newest_allowed):
        """Find the mapping that's most appropriate for this revision, 
        taking into account that it shouldn't be newer than 'max_mapping'.

        :return: Tuple with mapping for current revmeta and mapping for 
                 lhs parent revmeta
        """
        original = self.get_original_mapping()
        if original is None:
            return (newest_allowed, newest_allowed)
        lhs_revid = self._get_stored_lhs_parent_revid(original)
        if lhs_revid is None:
            lhs = original
        else:
            try:
                _, lhs = original.revision_id_bzr_to_foreign(lhs_revid)
            except bzr_errors.InvalidRevisionId:
                lhs = original
            else:
                pass # TODO: Make sure lhs <= original
        # TODO: Make sure original <= newest_allowed
        return (original, lhs)

    def get_original_mapping(self):
        """Find the original mapping that was used to store this revision
        or None if it is not a bzr-svn revision.
        """
        return self._import_from_props(None,
                find_mapping_fileprops,
                find_mapping_revprops,
                None, self.consider_bzr_fileprops,
                revprops_acceptable=lambda revprops: revprops.get(SVN_REVPROP_BZR_ROOT) == self.branch_path,
                revprops_sufficient=lambda x: True)

    def _get_stored_lhs_parent_revid(self, mapping):
        return self._import_from_props(mapping, 
                mapping.get_lhs_parent_fileprops,
                mapping.get_lhs_parent_revprops, 
                None, self.consider_bzr_fileprops)

    def get_lhs_parent_revid(self, mapping):
        """Find the revid of the left hand side parent of this revision."""
        # Sometimes we can retrieve the lhs parent from the revprop data
        lhs_parent = self._get_stored_lhs_parent_revid(mapping)
        if lhs_parent is not None:
            return lhs_parent
        parentrevmeta = self.get_lhs_parent_revmeta(mapping)
        if parentrevmeta is None:
            return NULL_REVISION
        lhs_mapping = parentrevmeta.get_original_mapping()
        if lhs_mapping is None:
            lhs_mapping = mapping
        return parentrevmeta.get_revision_id(lhs_mapping)

    def _fold_children_fileprops(self, get_memoized, calc_from_child, 
                                 calc_final, memoize):
        """Like foldr() in haskell except it works over (some) line 
        of descendants of this RevisionMetadata object. It continues 
        walking the descendants until it either finds one 
        that knows its file properties, already knows whatever has to 
        be calculated or until there are no more descendants.

        :param get_memoized(x) -> object: Function that returns the memoized 
            value for a given RevisionMetadata object or None if not memoized 
            yet.
        :param calc_from_child(x, child_val) -> object: Function that 
            calculates the value for x from a child value.
        :param calc_final(props): Determine the actual value from a file 
            properties dictionary.
        :param memoize(x, val): Memoize value for x
        """
        # FIXME: Use BFS here rather than DFS ?
        if get_memoized(self) is not None:
            # Simple shortcut..
            return get_memoized(self)
        lm = self
        todo = list()
        while (lm.children and not lm.knows_fileprops() and 
               get_memoized(lm) is None):
            todo.append(lm)
            lm = iter(lm.children).next()
        if get_memoized(lm) is not None:
            val = get_memoized(lm)
        else:
            val = calc_final(lm.get_fileprops())
        memoize(lm, val)
        while todo:
            lm = todo.pop()
            val = calc_from_child(lm, val)
            memoize(lm, val)
        assert lm == self, "%r != %r" % (lm, self)
        return val

    def _estimate_fileprop_ancestors(self, key, estimate_fn):
        """Count the number of lines in file properties, estimating how many 
        still exist in the worst case in a revision."""
        def calc_from_child(x, val):
            if val == 0:
                return 0
            if x.changes_branch_root():
                val -= 1
            if val == 0:
                val = estimate_fn(x.get_fileprops())
            return val

        def memoize(x, val):
            x._estimated_fileprop_ancestors[key] = val

        def get_memoized(x):
            return x._estimated_fileprop_ancestors.get(key)

        return self._fold_children_fileprops(
                get_memoized=get_memoized,
                calc_from_child=calc_from_child,
                calc_final=estimate_fn,
                memoize=memoize)

    def estimate_bzr_fileprop_ancestors(self):
        """Estimate how many ancestors with bzr fileprops this revision has.

        """
        return self._estimate_fileprop_ancestors("bzr:", estimate_bzr_ancestors)

    def estimate_svk_fileprop_ancestors(self):
        """Estimate how many svk ancestors this revision has."""
        return self._estimate_fileprop_ancestors("svk:merge", estimate_svk_ancestors)

    def estimate_bzr_hidden_fileprop_ancestors(self):
        return self._estimate_fileprop_ancestors("bzr:hidden", estimate_bzr_ancestors)

    def is_changes_root(self):
        """Check whether this revisions root is the root of the changes 
        in this svn revision.
        
        This is a requirement for revisions pushed with bzr-svn using 
        file properties.
        """
        return changes.changes_root(self.get_paths().keys()) == self.branch_path

    def changes_outside_root(self):
        """Check if there are any changes in this svn revision not under 
        this revmeta's root."""
        return changes.changes_outside_branch_path(self.branch_path, self.get_paths().keys())

    def is_hidden(self, mapping):
        """Check whether this revision should be hidden from Bazaar history."""
        if not mapping.supports_hidden:
            return False
        return self._import_from_props(mapping, 
                mapping.is_bzr_revision_hidden_fileprops,
                mapping.is_bzr_revision_hidden_revprops,
                False, consider_fileprops_fn=self.consider_bzr_hidden_fileprops)

    def get_distance_to_null(self, mapping):
        """Return the number of revisions between this one and the left hand 
        side NULL_REVISION, if known.
        """
        if mapping.roundtripping:
            (bzr_revno, _) = self._import_from_props(mapping, 
                    mapping.get_revision_id_fileprops,
                    mapping.get_revision_id_revprops,
                    (None, None), self.consider_bzr_fileprops)
            if bzr_revno is not None:
                return bzr_revno
        return None

    def get_hidden_lhs_ancestors_count(self, mapping):
        """Get the number of hidden ancestors this revision has."""
        if not mapping.supports_hidden:
            return 0
        return mapping.get_hidden_lhs_ancestors_count(self.get_fileprops())

    def get_revno(self, mapping):
        extra = 0
        total_hidden = None
        lm = self
        while lm and mapping.is_branch_or_tag(lm.branch_path):
            (mapping, lhs_mapping) = lm.get_appropriate_mappings(mapping)
            ret = lm.get_distance_to_null(mapping)
            if ret is not None:
                return ret + extra - (total_hidden or 0)
            if total_hidden is None:
                total_hidden = lm.get_hidden_lhs_ancestors_count(mapping)
            extra += 1
            lm = lm.get_direct_lhs_parent_revmeta()
            mapping = lhs_mapping
        return extra - (total_hidden or 0)

    def get_rhs_parents(self, mapping):
        """Determine the right hand side parents for this revision.

        """
        def consider_fileprops():
            return (self.consider_bzr_fileprops() or 
                    self.consider_svk_fileprops())

        def get_svk_merges(changed_fileprops):
            """Check what SVK revisions were merged in this revision."""
            previous, current = changed_fileprops.get(SVN_PROP_SVK_MERGE, ("", ""))
            ret = []
            for feature in svk_features_merged_since(current, previous or ""):
                # We assume svk:merge is only relevant on non-bzr-svn revisions. 
                # If this is a bzr-svn revision, the bzr-svn properties 
                # would be parsed instead.
                revid = svk_feature_to_revision_id(feature, mapping)
                if revid is not None:
                    ret.append(revid)

            return tuple(ret)

        def get_fileprops(fileprops):
            return (mapping.get_rhs_parents_fileprops(fileprops) or 
                    get_svk_merges(fileprops))

        return self._import_from_props(mapping,
            get_fileprops,
            mapping.get_rhs_parents_revprops, (), 
            consider_fileprops)

    def get_parent_ids(self, mapping):
        """Return the parent ids for this revision. """
        lhs_parent = self.get_lhs_parent_revid(mapping)

        if lhs_parent == NULL_REVISION:
            return (NULL_REVISION,)
        else:
            return (lhs_parent,) + self.get_rhs_parents(mapping)

    def get_signature(self):
        """Obtain the signature text for this revision, if any.

        :note: Will use the cached revision properties, which 
               may not necessarily be up to date.
        """
        return self.get_revprops().get(SVN_REVPROP_BZR_SIGNATURE)

    def get_revision(self, mapping):
        """Create a revision object for this revision.

        :param mapping: Mapping to use
        """
        parent_ids = self.get_parent_ids(mapping)

        if parent_ids == (NULL_REVISION,):
            parent_ids = ()
        rev = ForeignRevision(foreign_revid=self.get_foreign_revid(),
                              mapping=mapping, 
                              revision_id=self.get_revision_id(mapping), 
                              parent_ids=parent_ids)

        parse_svn_revprops(self.get_revprops(), rev)
        self._import_from_props(mapping, 
            lambda changed_fileprops: mapping.import_revision_fileprops(changed_fileprops, rev),
            lambda revprops: mapping.import_revision_revprops(revprops, rev),
            False, self.consider_bzr_fileprops, 
            lambda x: True)

        rev.svn_meta = self

        return rev

    def _import_from_props(self, mapping, fileprop_fn, revprop_fn, default,
                           consider_fileprops_fn, 
                           revprops_acceptable=None,
                           revprops_sufficient=None):
        can_use_revprops = (mapping is None or mapping.can_use_revprops)
        can_use_fileprops = (mapping is None or mapping.can_use_fileprops)
        if revprops_acceptable is None:
            def revprops_acceptable(revprops):
                return (mapping.get_branch_root(revprops) == self.branch_path and 
                        mapping.get_repository_uuid(revprops) in (None, self.uuid))
        if revprops_sufficient is None:
            def revprops_sufficient(revprops):
                return mapping.revprops_complete(revprops)

        # Check revprops if self.knows_revprops() and can_use_revprops
        if can_use_revprops and self.knows_revprops():
            revprops = self.get_revprops()
            if revprops_acceptable(revprops):
                ret = revprop_fn(revprops)
                if revprops_sufficient(revprops):
                    return ret
            can_use_revprops = False

        can_use_fileprops = can_use_fileprops and self.is_changes_root()

        # Check changed_fileprops if self.knows_changed_fileprops() and 
        # can_use_fileprops
        if can_use_fileprops and self.knows_changed_fileprops():
            ret = fileprop_fn(self.get_changed_fileprops())
            if ret != default:
                return ret
            can_use_fileprops = False

        # No point in looking any further if file properties should be there
        if (mapping is not None and mapping.must_use_fileprops and
            not can_use_fileprops):
            return default

        # Check revprops if the last descendant has bzr:check-revprops set;
        #   if it has and the revnum there is < self.revnum
        if can_use_revprops and self.consider_bzr_revprops():
            if self.revnum > 0:
                # revision 0 is not included by svn_ra_get_log()
                warn_slow_revprops()
            revprops = self.get_revprops()
            if revprops_acceptable(revprops):
                ret = revprop_fn(revprops)
                if revprops_sufficient(revprops):
                    return ret

        # Check whether we should consider file properties at all 
        # for this revision, if we should -> check fileprops
        if can_use_fileprops and consider_fileprops_fn():
            ret = fileprop_fn(self.get_changed_fileprops())
            if ret != default:
                return ret

        return default

    def get_fileid_overrides(self, mapping):
        """Find the file id override map for this revision."""
        return self._import_from_props(mapping, 
            mapping.import_fileid_map_fileprops, 
            mapping.import_fileid_map_revprops, {}, self.consider_bzr_fileprops)

    def get_text_revisions(self, mapping):
        """Return text revision override map for this revision."""
        return self._import_from_props(mapping,
            mapping.import_text_revisions_fileprops,
            mapping.import_text_revisions_revprops, {}, 
            self.consider_bzr_fileprops)

    def get_text_parents(self, mapping):
        """Return text revision override map for this revision."""
        return self._import_from_props(mapping,
            mapping.import_text_parents_fileprops, 
            mapping.import_text_parents_revprops, {}, 
            self.consider_bzr_fileprops)

    def consider_bzr_revprops(self):
        """See if bzr revision properties should be checked at all.

        """
        if self._consider_bzr_revprops is not None:
            return self._consider_bzr_revprops
        if self._log._transport.has_capability("commit-revprops") == False:
            # Server doesn't support setting revision properties
            self._consider_bzr_revprops = False
        elif self._log._transport.has_capability("log-revprops") is None:
            # Client doesn't know log-revprops capability
            self._consider_bzr_revprops = True
        elif self.changes_outside_root():
            self._consider_bzr_revprops = False
        else:
            # Check nearest descendant with bzr:see-revprops set
            # and return True if revnum in that property < self.revnum
            revprop_redirect = self._get_revprop_redirect_revnum()
            self._consider_bzr_revprops = (revprop_redirect >= 0 and
                                          revprop_redirect <= self.revnum)
        return self._consider_bzr_revprops

    def _get_revprop_redirect_revnum(self):
        def get_memoized(x):
            return getattr(x, "_revprop_redirect_revnum", None)
        def memoize(x, val):
            x._revprop_redirect_revnum = val
        def calc(fileprops):
            if SVN_PROP_BZR_REVPROP_REDIRECT in fileprops:
                return int(fileprops[SVN_PROP_BZR_REVPROP_REDIRECT])
            return -1
        return self._fold_children_fileprops(
                get_memoized=get_memoized,
                calc_from_child=lambda x, val: val,
                calc_final=calc,
                memoize=memoize)

    def consider_bzr_hidden_fileprops(self):
        return (self.estimate_bzr_hidden_fileprop_ancestors() > 0) 

    def consider_bzr_fileprops(self):
        """See if any bzr file properties should be checked at all.

        This will try to avoid extra network traffic if at all possible.
        """
        if self._consider_bzr_fileprops is not None:
            return self._consider_bzr_fileprops
        self._consider_bzr_fileprops = (self.estimate_bzr_fileprop_ancestors() > 0)
        return self._consider_bzr_fileprops

    def consider_svk_fileprops(self):
        """See if svk:merge should be checked at all for this revision.

        This will try to avoid extra network traffic if at all possible.
        """
        return (self.changes_branch_root() and 
                self.estimate_svk_fileprop_ancestors() > 0)

    def get_roundtrip_ancestor_revids(self):
        """Return the number of fileproperty roundtrip ancestors.
        """
        return iter(get_roundtrip_ancestor_revids(self.get_fileprops()))

    def __hash__(self):
        return hash((self.__class__, self.uuid, self.branch_path, self.revnum))


class CachingRevisionMetadata(RevisionMetadata):
    """Wrapper around RevisionMetadata that stores some results in a cache."""

    def __init__(self, repository, *args, **kwargs):
        super(CachingRevisionMetadata, self).__init__(repository, *args, 
            **kwargs)
        self._parents_cache = getattr(self.repository._real_parents_provider, 
                                      "_cache", None)
        self._revid_cache = self.repository.revmap.cache
        self._revid = None

    def get_revision_id(self, mapping):
        """Find the revision id of a revision."""
        if self._revid is not None:
            return self._revid
        # Look in the cache to see if it already has a revision id
        self._revid = self._revid_cache.lookup_branch_revnum(self.revnum, 
                                                             self.branch_path, 
                                                             mapping.name)
        if self._revid is not None:
            return self._revid

        self._revid = super(CachingRevisionMetadata, self).get_revision_id(
            mapping)

        self._revid_cache.insert_revid(self._revid, self.branch_path, 
                                       self.revnum, self.revnum, mapping.name)
        self._revid_cache.commit_conditionally()
        return self._revid

    def get_parent_ids(self, mapping):
        """Find the parent ids of a revision."""
        myrevid = self.get_revision_id(mapping)

        if self._parents_cache is not None:
            parent_ids = self._parents_cache.lookup_parents(myrevid)
            if parent_ids is not None:
                return parent_ids

        parent_ids = super(CachingRevisionMetadata, self).get_parent_ids(
            mapping)

        self._parents_cache.insert_parents(myrevid, parent_ids)

        return parent_ids


def svk_feature_to_revision_id(feature, mapping):
    """Convert a SVK feature to a revision id for this repository.

    :param feature: SVK feature.
    :return: revision id.
    """
    try:
        (uuid, bp, revnum) = parse_svk_feature(feature)
    except svn_errors.InvalidPropertyValue:
        return None
    if not mapping.is_branch_or_tag(bp):
        return None
    return mapping.revision_id_foreign_to_bzr((uuid, bp, revnum))


class ListBuildingIterator(object):
    """Simple iterator that iterates over a list, and calling an iterator 
    once all items in the list have been iterated.

    The list may be updated while the iterator is running.
    """

    def __init__(self, base_list, it):
        self.base_list = base_list
        self.i = -1
        self.it = it

    def next(self):
        """Return the next item."""
        self.i+=1
        try:
            return self.base_list[self.i]
        except IndexError:
            return self.it()


class RevisionMetadataBranch(object):
    """Describes a Bazaar-like branch in a Subversion repository."""

    def __init__(self, revmeta_provider=None, history_limit=None):
        self._revs = []
        self._revnums = []
        self._history_limit = history_limit
        self._revmeta_provider = revmeta_provider
        self._get_next = None

    def __eq__(self, other):
        return (type(self) == type(other) and 
                self._history_limit == other._history_limit and
                ((self._revs == [] and other._revs == []) or 
                 (self._revs != [] and other._revs != [] and self._revs[0] == other._revs[0])))

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        if len(self._revs) == 0:
            return hash((type(self), self._history_limit))
        return hash((type(self), self._history_limit, self._revs[0]))

    def __repr__(self):
        if len(self._revs) == 0:
            return "<Empty RevisionMetadataBranch>"
        else:
            return "<RevisionMetadataBranch starting at %s revision %d>" % (self._revs[0].branch_path, self._revs[0].revnum)

    def __iter__(self):
        return ListBuildingIterator(self._revs, self.next)

    def fetch_until(self, revnum):
        """Fetch at least all revisions until revnum."""
        while len(self._revnums) == 0 or self._revnums[0] > revnum:
            try:
                self.next()
            except MetaHistoryIncomplete:
                return
            except StopIteration:
                return

    def next(self):
        if self._get_next is None:
            raise MetaHistoryIncomplete()
        if self._history_limit and len(self._revs) >= self._history_limit:
            raise MetaHistoryIncomplete()
        try:
            ret = self._get_next()
        except StopIteration:
            raise
        return ret

    def _index(self, revmeta):
        """Find the location of a revmeta object, counted from the 
        most recent revision."""
        i = len(self._revs) - bisect.bisect_right(self._revnums, revmeta.revnum)
        assert i == len(self._revs) or self._revs[i] == revmeta
        return i

    def get_lhs_parent(self, revmeta):
        """Find the left hand side of a revision using revision metadata.

        :note: Will return None if no LHS parent can be found, this 
            doesn't necessarily mean there is no LHS parent.
        """
        i = self._index(revmeta)
        try:
            return self._revs[i+1]
        except IndexError:
            return self.next()

    def append(self, revmeta):
        """Append a revision metadata object to this branch."""
        assert len(self._revs) == 0 or self._revs[-1].revnum > revmeta.revnum,\
                "%r > %r" % (self._revs[-1].revnum, revmeta.revnum)
        self._revs.append(revmeta)
        self._revnums.insert(0, revmeta.revnum)


class RevisionMetadataBrowser(object):
    """Object can that can iterate over the meta revisions in a 
    revision range.

    """

    def __init__(self, prefixes, from_revnum, to_revnum, layout, provider,
            project=None, pb=None):
        """Create a new browser

        :param prefixes: Prefixes of branches over which to iterate
        :param from_revnum: Start side of revnum
        :param to_revnum: Stop side of revnum
        :param layout: Layout
        :param provider: Object with get_revision and iter_changes functions
        :param project: Project name
        :param pb: Optional progress bar
        """
        self.prefixes = prefixes
        self.from_revnum = from_revnum
        self.to_revnum = to_revnum
        self._last_revnum = None
        self.layout = layout
        self._metabranches = defaultdict(RevisionMetadataBranch)
        self._metabranches_history = defaultdict(lambda: defaultdict(set))
        self._unusual = set()
        self._unusual_history = defaultdict(set)
        self._provider = provider
        self._actions = []
        self._iter = iter(self.do(project, pb))

    def __iter__(self):
        return ListBuildingIterator(self._actions, self.next)

    def __repr__(self):
        return "<RevisionMetadataBrowser from %d to %d, layout: %r>" % (self.from_revnum, self.to_revnum, self.layout)

    def __eq__(self, other):
        return (type(self) == type(other) and 
                self.from_revnum == other.from_revnum and 
                self.to_revnum == other.to_revnum and
                self.prefixes == other.prefixes and
                self.layout == other.layout)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((type(self), self.from_revnum, self.to_revnum, tuple(self.prefixes), hash(self.layout)))

    def get_lhs_parent(self, revmeta):
        """Find the left hand side parent of a revision metadata object."""
        while not revmeta._direct_lhs_parent_known:
            try:
                self.next()
            except StopIteration:
                if self.to_revnum > 0:
                    raise MetaHistoryIncomplete()
                if not any([x for x in self.prefixes if revmeta.branch_path.startswith(x+"/") or x == revmeta.branch_path or x == ""]):
                    raise MetaHistoryIncomplete()
                raise AssertionError("Unable to find direct lhs parent for %r" % revmeta)
        return revmeta._direct_lhs_parent_revmeta

    def fetch_until(self, revnum):
        """Fetch at least all revisions until revnum."""
        try:
            while self._last_revnum is None or self._last_revnum > revnum:
                self.next()
        except StopIteration:
            return

    def next(self):
        ret = self._iter.next()
        self._actions.append(ret)
        return ret

    def _process_new_rev(self, bp, mb, revnum, paths, revprops):
        """Process the changes for a new revision and update the 
        branches it is on.
        
        :param bp: Branch path
        :param mb: Metabranch object
        :param revnum: Revision number
        :param paths: Paths dictionary with changes
        :param revprops: Revision properties for the svn revision
        :return: RevisionMetadata object
        """
        revmeta = self._provider.get_revision(bp, revnum, paths, revprops, 
                                              metaiterator=self)
        assert revmeta is not None
        children = set([c._revs[-1] for c in self._metabranches_history[revnum][bp]])
        del self._metabranches_history[revnum][bp]
        if mb._revs:
            children.add(mb._revs[-1])
        mb.append(revmeta)
        for c in children:
            c._set_direct_lhs_parent_revmeta(revmeta)
        return revmeta

    def _remove_metabranch(self, name, revnum):
        del self._metabranches[name]
        if name in self._metabranches_history[revnum]:
            del self._metabranches_history[revnum][name]

    def do(self, project=None, pb=None):
        for (paths, revnum, revprops) in self._provider._log.iter_changes(
                self.prefixes, self.from_revnum, self.to_revnum, pb=pb):
            bps = {}
            deletes = []
            if pb:
                pb.update("discovering revisions", self.to_revnum-revnum, 
                          self.from_revnum-self.to_revnum)

            # Import all metabranches_history where key > revnum
            for x in [r for r in self._metabranches_history if r > revnum]:
                for bp in self._metabranches_history[x].keys():
                    self._metabranches_history[revnum][bp].update(self._metabranches_history[x][bp])
                    del self._metabranches_history[x][bp]
                del self._metabranches_history[x]
            for bp, mbs in self._metabranches_history[revnum].iteritems():
                if not bp in self._metabranches:
                    self._metabranches[bp] = iter(mbs).next()
            self._unusual.update(self._unusual_history[revnum])

            for p in sorted(paths):
                action = paths[p][0]

                try:
                    (_, bp, ip) = self.layout.split_project_path(p, project)
                except svn_errors.NotSvnBranchPath:
                    pass
                else:
                    if action != 'D' or ip != "":
                        bps[bp] = self._metabranches[bp]
                for u in self._unusual:
                    if (p == u and not action in ('D', 'R')) or p.startswith("%s/" % u):
                        bps[u] = self._metabranches[u]
                if action in ('R', 'D') and (
                    self.layout.is_branch_or_tag(p, project) or 
                    self.layout.is_branch_or_tag_parent(p, project)):
                    deletes.append(p)

            # Mention deletes
            for d in deletes:
                yield ("delete", p)

            # Report the new revisions
            for bp, mb in bps.items():
                revmeta = self._process_new_rev(bp, mb, revnum, paths, revprops)
                if (bp in paths and paths[bp][0] in ('A', 'R') and 
                    paths[bp][1] is None):
                    revmeta._set_direct_lhs_parent_revmeta(None)
                yield "revision", revmeta
            
            # Apply renames and the like for the next round
            for new_name, old_name, old_rev in changes.apply_reverse_changes(
                self._metabranches.keys(), paths):
                if new_name in self._unusual:
                    self._unusual.remove(new_name)
                if old_name is None: 
                    # didn't exist previously
                    if new_name in self._metabranches:
                        if self._metabranches[new_name]._revs:
                            rev = self._metabranches[new_name]._revs[-1]
                            if rev.branch_path != new_name:
                                raise AssertionError("Revision %d has invalid branch path %s, expected %s" % (revnum, rev.branch_path, new_name))
                            rev._set_direct_lhs_parent_revmeta(None)
                        self._remove_metabranch(new_name, revnum)
                else:
                    data = self._metabranches[new_name]
                    self._remove_metabranch(new_name, revnum)
                    self._metabranches_history[old_rev][old_name].add(data)
                    if not self.layout.is_branch_or_tag(old_name, project):
                        self._unusual_history[old_rev].add(old_name)
            self._last_revnum = revnum


def filter_revisions(it):
    """Filter out all revisions out of a stream with changes."""
    for kind, rev in it:
        if kind == "revision":
            yield rev


def restrict_prefixes(prefixes, prefix):
    """Trim a list of prefixes down as much as possible."""
    ret = set()
    for p in prefixes:
        if prefix == "" or p == prefix or p.startswith(prefix+"/"):
            ret.add(p)
        elif prefix.startswith(p+"/") or p == "":
            ret.add(prefix)
    return ret


class RevisionMetadataProvider(object):
    """A RevisionMetadata provider."""

    def __init__(self, repository, cache, check_revprops):
        self._revmeta_cache = {}
        self.repository = repository
        self._get_fileprops_fn = self.repository.branchprop_list.get_properties
        self._log = repository._log
        self.check_revprops = check_revprops
        self._open_metaiterators = []
        if cache:
            self._revmeta_cls = CachingRevisionMetadata
        else:
            self._revmeta_cls = RevisionMetadata

    def create_revision(self, path, revnum, changes=None, revprops=None, 
                        changed_fileprops=None, fileprops=None, 
                        metaiterator=None):
        return self._revmeta_cls(self.repository, self.check_revprops, 
                                 self._get_fileprops_fn, self._log, 
                                 self.repository.uuid, path, revnum, changes, 
                                 revprops, changed_fileprops=changed_fileprops,
                                 fileprops=fileprops, metaiterator=metaiterator)

    def lookup_revision(self, path, revnum, revprops=None):
        """Lookup a revision, optionally checking whether there are any 
        unchecked metaiterators that perhaps contain the revision."""
        # finish fetching any open revisionmetadata branches for 
        # which the latest fetched revnum > revnum
        for mb in self._open_metaiterators:
            if (path, revnum) in self._revmeta_cache:
                break
            mb.fetch_until(revnum)
        return self.get_revision(path, revnum, revprops=revprops)


    def finish_metaiterators(self):
        for mb in self._open_metaiterators:
            mb.fetch_until(0)

    def get_revision(self, path, revnum, changes=None, revprops=None, 
                     changed_fileprops=None, fileprops=None, metaiterator=None):
        """Return a RevisionMetadata object for a specific svn (path,revnum)."""
        assert isinstance(path, str)
        assert isinstance(revnum, int)

        if (path, revnum) in self._revmeta_cache:
            cached = self._revmeta_cache[path,revnum]
            if changes is not None:
                cached.paths = changes
            if cached._changed_fileprops is None:
                cached._changed_fileprops = changed_fileprops
            if cached._fileprops is None:
                cached._fileprops = fileprops
            if metaiterator is not None:
                cached.metaiterators.add(metaiterator)
            return self._revmeta_cache[path,revnum]

        ret = self.create_revision(path, revnum, changes, revprops, 
                                   changed_fileprops, fileprops, metaiterator)
        self._revmeta_cache[path,revnum] = ret
        return ret

    def iter_changes(self, branch_path, from_revnum, to_revnum, pb=None, 
                     limit=0):
        """Iterate over all revisions backwards.
        
        :return: iterator that returns tuples with branch path, 
            changed paths, revision number, changed file properties and 
        """
        assert isinstance(branch_path, str)
        assert from_revnum >= to_revnum

        bp = branch_path
        i = 0

        # Limit can't be passed on directly to LogWalker.iter_changes() 
        # because we're skipping some revs
        # TODO: Rather than fetching everything if limit == 2, maybe just 
        # set specify an extra X revs just to be sure?
        for (paths, revnum, revprops) in self._log.iter_changes([branch_path], 
            from_revnum, to_revnum, pb=pb):
            assert bp is not None
            next = changes.find_prev_location(paths, bp, revnum)
            assert revnum > 0 or bp == ""
                    
            if changes.changes_path(paths, bp, False):
                yield (bp, paths, revnum, revprops)
                i += 1

            if next is None:
                bp = None
            else:
                bp = next[0]

    def iter_reverse_branch_changes(self, branch_path, from_revnum, to_revnum, 
                                    pb=None, limit=0):
        """Return all the changes that happened in a branch 
        until branch_path,revnum. 

        :return: iterator that returns RevisionMetadata objects.
        """
        history_iter = self.iter_changes(branch_path, from_revnum, 
                                         to_revnum, pb=pb, limit=limit)
        def convert((bp, paths, revnum, revprops)):
            ret = self.get_revision(bp, revnum, paths, revprops, 
                                    metaiterator=metabranch)
            if metabranch._revs:
                metabranch._revs[-1]._set_direct_lhs_parent_revmeta(ret)
            metabranch.append(ret)
            return ret
        metabranch = RevisionMetadataBranch(self, limit)
        metabranch._get_next = imap(convert, history_iter).next
        self._open_metaiterators.append(metabranch)
        return metabranch

    def iter_all_revisions(self, layout, check_unusual_path, from_revnum, 
                           to_revnum=0, project=None, pb=None):
        """Iterate over all RevisionMetadata objects in a repository.

        :param layout: Repository layout to use
        :param check_unusual_path: Check whether to keep branch

        Layout decides which ones to pick up.
        """
        return filter_revisions(self.iter_all_changes(layout, check_unusual_path, from_revnum, to_revnum, project, pb))

    def iter_all_changes(self, layout, check_unusual_path, from_revnum, 
                         to_revnum=0, project=None, pb=None, prefix=None):
        """Iterate over all RevisionMetadata objects and branch removals 
        in a repository.

        :param layout: Repository layout to use
        :param check_unusual_path: Check whether to keep branch

        Layout decides which ones to pick up.
        """
        assert from_revnum >= to_revnum
        if check_unusual_path is None:
            check_unusual_path = lambda x: True
        if project is not None:
            prefixes = layout.get_project_prefixes(project)
        else:
            prefixes = [""]

        if prefix is not None:
            prefixes = list(restrict_prefixes(prefixes, prefix))
        
        browser = RevisionMetadataBrowser(prefixes, from_revnum, to_revnum, 
                                          layout, self, project, pb=pb)
        self._open_metaiterators.append(browser)
        for kind, item in browser:
            if kind != "revision" or check_unusual_path(item.branch_path):
                yield kind, item


def iter_with_mapping(it, mapping):
    """Iterate through a stream of RevisionMetadata objects, newest first and 
    add the appropriate mapping.
    """
    for revmeta in it:
        (mapping, lhs_mapping) = revmeta.get_appropriate_mappings(mapping)
        if not mapping.is_branch_or_tag(revmeta.branch_path):
            return
        yield revmeta, mapping
        mapping = lhs_mapping



