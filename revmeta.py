# Copyright (C) 2005-2009 Jelmer Vernooij <jelmer@samba.org>

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Subversion Meta-Revisions. This is where all the magic happens. """

try:
    from collections import defaultdict
except ImportError:
    from bzrlib.plugins.svn.pycompat import defaultdict

try:
    any
except NameError:
    from bzrlib.plugins.svn.pycompat import any

import bisect
from itertools import (
    imap,
    )
from subvertpy import (
    properties,
    )

from bzrlib import (
    errors as bzr_errors,
    trace,
    ui,
    )
from bzrlib.foreign import (
    ForeignRevision,
    )
from bzrlib.revision import (
    NULL_REVISION,
    )
from bzrlib.plugins.svn import (
    changes,
    errors as svn_errors,
    util,
    )
from bzrlib.plugins.svn.mapping import (
    SVN_PROP_BZR_HIDDEN,
    SVN_PROP_BZR_REVPROP_REDIRECT,
    SVN_REVPROP_BZR_ROOT,
    SVN_REVPROP_BZR_SIGNATURE,
    estimate_bzr_ancestors,
    find_mapping_fileprops,
    find_mapping_revprops,
    get_roundtrip_ancestor_revids,
    parse_svn_revprops,
    revprops_complete,
    )
from bzrlib.plugins.svn.svk import (
    estimate_svk_ancestors,
    parse_svk_feature,
    svk_features_merged_since,
    SVN_PROP_SVK_MERGE,
    )
from bzrlib.plugins.svn.util import (
    ListBuildingIterator,
    )

# Maximum number of revisions to browse for a cached copy of the branch
# file properties
MAX_FILEPROP_SHARED = 5000

_warned_slow_revprops = False

def warn_slow_revprops(config, server):
    global _warned_slow_revprops
    if _warned_slow_revprops:
        return

    try:
        warn_upgrade = config.get_bool("warn-upgrade")
    except KeyError:
        warn_upgrade = True
    if warn_upgrade:
        if server:
            trace.warning("Upgrade server to svn 1.5 or higher for faster retrieving of revision properties.")
        else:
            trace.warning("Upgrade to svn 1.5 or higher for faster retrieving of revision properties.")
        _warned_slow_revprops = True


class MetaHistoryIncomplete(Exception):
    """No revision metadata branch: %(msg)s"""

    def __init__(self, msg):
        self.msg = msg


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
        self._direct_lhs_parent_known = False
        self._consider_bzr_fileprops = None
        self._consider_bzr_revprops = None
        self._estimated_fileprop_ancestors = {}
        self.metaiterators = set()
        if metaiterator is not None:
            self.metaiterators.add(metaiterator)
        self.uuid = uuid
        self.children = set()

    def __eq__(self, other):
        return (type(self) == type(other) and
                self.revnum == other.revnum and
                self.branch_path == other.branch_path and
                self.uuid == other.uuid)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __cmp__(self, other):
        return cmp((self.uuid, self.revnum, self.branch_path),
                   (other.uuid, other.revnum, other.branch_path))

    def __repr__(self):
        return "<%s for revision %d, path %s in repository %r>" % (self.__class__.__name__, self.revnum, self.branch_path, self.uuid)

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

    def get_paths(self):
        """Fetch the changed paths dictionary for this revision.
        """
        if self._paths is None:
            self._paths = self._log.get_revision_paths(self.revnum)
        return self._paths

    def get_revision_info(self, mapping):
        return self._import_from_props(mapping,
                mapping.get_revision_id_fileprops,
                mapping.get_revision_id_revprops,
                (None, None, None), self.consider_bzr_fileprops)

    def get_revision_id(self, mapping):
        """Determine the revision id for this revision.
        """
        if mapping.roundtripping:
            # See if there is a bzr:revision-id revprop set
            (_, revid, _) = self.get_revision_info(mapping)
        else:
            revid = None

        # Or generate it
        if revid is None:
            revid = mapping.revision_id_foreign_to_bzr(self.get_foreign_revid())

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
            while (not lm.changes_branch_root() and lm._fileprops is None and
                   len(todo) < MAX_FILEPROP_SHARED):
                todo.add(lm)
                nlm = lm.get_direct_lhs_parent_revmeta()
                assert nlm is not None, \
                        "no lhs parent revmeta found for %r" % lm
                lm = nlm
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
        return (isinstance(changed_fileprops, dict) or
                changed_fileprops.is_loaded)

    def knows_fileprops(self):
        """Check whether the file properties can be cheaply retrieved."""
        if self._fileprops is None:
            return False
        fileprops = self.get_fileprops()
        return isinstance(fileprops, dict) or fileprops.is_loaded

    def knows_revprops(self):
        """Check whether all revision properties can be cheaply retrieved."""
        if self._revprops is None:
            return False
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
                self._changed_fileprops = util.lazy_dict({}, properties.diff,
                    self.get_fileprops(), self.get_previous_fileprops())
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
            self.revnum, to_revnum=0, limit=0)
        firstrevmeta = iterator.next()
        assert self == firstrevmeta, "Expected %r got %r" % (self, firstrevmeta)
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
        # Make sure original <= newest_allowed
        if original.newer_than(newest_allowed):
            original = newest_allowed
        if lhs.newer_than(original):
            lhs = original
        return (original, lhs)

    def get_original_mapping(self):
        """Find the original mapping that was used to store this revision
        or None if it is not a bzr-svn revision.
        """
        def revprops_acceptable(revprops):
            return revprops.get(SVN_REVPROP_BZR_ROOT) == self.branch_path
        return self._import_from_props(None,
                find_mapping_fileprops,
                find_mapping_revprops,
                None, self.consider_bzr_fileprops,
                revprops_acceptable=revprops_acceptable)

    def get_implicit_lhs_parent_revid(self, mapping):
        parentrevmeta = self.get_lhs_parent_revmeta(mapping)
        if parentrevmeta is None:
            return NULL_REVISION
        lhs_mapping = parentrevmeta.get_original_mapping()
        if lhs_mapping is None or lhs_mapping.newer_than(mapping):
            lhs_mapping = mapping
        return parentrevmeta.get_revision_id(lhs_mapping)

    def get_stored_lhs_parent_revid(self, mapping):
        return self._get_stored_lhs_parent_revid(mapping)

    def _get_stored_lhs_parent_revid(self, mapping):
        return self._import_from_props(mapping,
                mapping.get_lhs_parent_fileprops,
                mapping.get_lhs_parent_revprops,
                None, self.consider_bzr_fileprops)

    def get_lhs_parent_revid(self, mapping):
        """Find the revid of the left hand side parent of this revision."""
        # Sometimes we can retrieve the lhs parent from the revprop data
        lhs_parent = self.get_stored_lhs_parent_revid(mapping)
        if lhs_parent is not None:
            return lhs_parent
        return self.get_implicit_lhs_parent_revid(mapping)

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
        return self._estimate_fileprop_ancestors("svk:merge",
            estimate_svk_ancestors)

    def estimate_bzr_hidden_fileprop_ancestors(self):
        return self._estimate_fileprop_ancestors(SVN_PROP_BZR_HIDDEN,
            estimate_bzr_ancestors)

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
        return changes.changes_outside_branch_path(self.branch_path,
            self.get_paths().keys())

    def is_hidden(self, mapping):
        """Check whether this revision should be hidden from Bazaar history."""
        if not mapping.supports_hidden:
            return False
        if not self.changes_branch_root():
            return False
        (bzr_revno, revid, hidden) = self.get_revision_info(mapping)
        return hidden

    def get_distance_to_null(self, mapping):
        """Return the stored number of revisions between this one and the
        left hand side NULL_REVISION, if known.
        """
        if mapping.roundtripping:
            (bzr_revno, revid, hidden) = self.get_revision_info(mapping)
            if bzr_revno is not None:
                return bzr_revno
        return None

    def get_hidden_lhs_ancestors_count(self, mapping):
        """Get the number of hidden ancestors this revision has."""
        if not mapping.supports_hidden:
            return 0
        return mapping.get_hidden_lhs_ancestors_count(self.get_fileprops())

    def get_revno(self, mapping):
        """Determine the Bazaar revision number for this revision.

        :param mapping: Mapping to use
        :return: Bazaar revision number
        """
        extra = 0
        total_hidden = None
        lm = self
        pb = ui.ui_factory.nested_progress_bar()
        try:
            while lm and mapping.is_branch_or_tag(lm.branch_path):
                pb.update("determining revno", self.revnum-lm.revnum,
                          self.revnum)
                (mapping, lhs_mapping) = lm.get_appropriate_mappings(mapping)
                ret = lm.get_distance_to_null(mapping)
                if ret is not None:
                    return ret + extra - (total_hidden or 0)
                if total_hidden is None:
                    total_hidden = lm.get_hidden_lhs_ancestors_count(mapping)
                extra += 1
                lm = lm.get_direct_lhs_parent_revmeta()
                mapping = lhs_mapping
        finally:
            pb.finished()
        return extra - (total_hidden or 0)

    def get_rhs_parents(self, mapping):
        """Determine the right hand side parent ids for this revision.

        :param mapping: 
        """
        return self._get_rhs_parents(mapping)

    def _get_rhs_parents(self, mapping):
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
        """Return the parent ids for this revision.

        :param mapping: Mapping to use.
        """
        lhs_parent = self.get_lhs_parent_revid(mapping)

        if lhs_parent == NULL_REVISION:
            return (NULL_REVISION,)
        else:
            return (lhs_parent,) + self._get_rhs_parents(mapping)

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
        """Import some round-tripped metadata from this revision.

        This tries as hard as possible to not fetch any data that shouldn't
        be considered.

        :param mapping: Mapping to use
        :param fileprop_fn: Function to interpret changed file properties
        :param revprop_fn: Function to interpret revision properties
        :param default: Default value if nothing else works
        :param consider_fileprops_fn: Function that checks whether
            file properties should be considered.
        :param revprops_acceptable:
        :param revprops_sufficient:
        """
        can_use_revprops = (mapping is None or mapping.can_use_revprops)
        can_use_fileprops = (mapping is None or mapping.can_use_fileprops)
        if revprops_acceptable is None:
            def revprops_acceptable(revprops):
                return (mapping.get_branch_root(revprops) == self.branch_path and
                        mapping.get_repository_uuid(revprops) in (None, self.uuid))
        if revprops_sufficient is None:
            revprops_sufficient = revprops_complete

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
        if (can_use_revprops and not self.knows_revprops() and
            self.consider_bzr_revprops()):
            log_revprops_capab = self._log._transport.has_capability("log-revprops")
            if log_revprops_capab in (None, False):
                warn_slow_revprops(self.repository.get_config(),
                                   log_revprops_capab == False)
            revprops = self.get_revprops()
            if revprops_acceptable(revprops):
                ret = revprop_fn(revprops)
                if revprops_sufficient(revprops):
                    return ret

        # Check whether we should consider file properties at all
        # for this revision, if we should -> check fileprops
        if (can_use_fileprops and not self.knows_changed_fileprops() and
            consider_fileprops_fn()):
            ret = fileprop_fn(self.get_changed_fileprops())
            if ret != default:
                return ret

        return default

    def get_fileid_overrides(self, mapping):
        """Find the file id override map for this revision.

        :param mapping: Mapping to use
        """
        return self._import_from_props(mapping,
            mapping.import_fileid_map_fileprops,
            mapping.import_fileid_map_revprops, {}, self.consider_bzr_fileprops)

    def get_text_revisions(self, mapping):
        """Return text revision override map for this revision.

        :param mapping: Mapping to use
        """
        return self._import_from_props(mapping,
            mapping.import_text_revisions_fileprops,
            mapping.import_text_revisions_revprops, {},
            self.consider_bzr_fileprops)

    def consider_bzr_revprops(self):
        """See if bzr revision properties should be checked at all.

        """
        if self._consider_bzr_revprops is not None:
            return self._consider_bzr_revprops
        if self._log._transport.has_capability("commit-revprops") == False:
            # Server doesn't support setting revision properties
            self._consider_bzr_revprops = False
        elif self._log._transport.has_capability("log-revprops") == True:
            self._consider_bzr_revprops = True
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
        """Retrieve the file property references the first revision
        with revision properties.
        """
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
        """See if this is a hidden bzr revision in file properties."""
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
        return hash((self.__class__, self.get_foreign_revid()))


class CachingRevisionMetadata(RevisionMetadata):
    """Wrapper around RevisionMetadata that stores some results in a cache."""

    def __init__(self, repository, *args, **kwargs):
        self.base = super(CachingRevisionMetadata, self)
        self.base.__init__(repository, *args,
            **kwargs)
        self._parents_cache = getattr(self.repository._real_parents_provider,
                                      "_cache", None)
        self._revid_cache = self.repository.revmap.cache
        self._revinfo_cache = self.repository.revinfo_cache
        self._revision_info = {}
        self._original_mapping = None
        self._original_mapping_set = False
        self._stored_lhs_parent_revid = {}

    def _update_cache(self, mapping):
        if (self.get_original_mapping() is not None and
            self._revision_info[mapping][1] is not None):
            self._revid_cache.insert_revid(self._revision_info[mapping][1],
                self.branch_path, self.revnum, self.revnum, mapping.name)
            self._revid_cache.commit_conditionally()
        self._revinfo_cache.insert_revision(self.get_foreign_revid(), mapping,
            self._revision_info[mapping],
            self._stored_lhs_parent_revid[mapping])
        self._revinfo_cache.commit_conditionally()

    def _determine(self, mapping):
        self._revision_info[mapping] = self.base.get_revision_info(mapping)
        self._stored_lhs_parent_revid[mapping] = self.base._get_stored_lhs_parent_revid(mapping)

    def _retrieve(self, mapping):
        assert mapping is not None
        (self._revision_info[mapping],
                self._stored_lhs_parent_revid[mapping]) = \
                 self._revinfo_cache.get_revision(self.get_foreign_revid(),
                                                  mapping)

    def get_original_mapping(self):
        if self._original_mapping_set:
            return self._original_mapping
        try:
            self._original_mapping = self._revinfo_cache.get_original_mapping(
                self.get_foreign_revid())
        except KeyError:
            self._original_mapping = self.base.get_original_mapping()
            self._revinfo_cache.set_original_mapping(self.get_foreign_revid(),
                    self._original_mapping)
        self._original_mapping_set = True
        return self._original_mapping

    def get_stored_lhs_parent_revid(self, mapping):
        try:
            return self._stored_lhs_parent_revid[mapping]
        except KeyError:
            pass
        try:
            self._retrieve(mapping)
        except KeyError:
            self._determine(mapping)
            self._update_cache(mapping)
        return self._stored_lhs_parent_revid[mapping]

    def _get_stored_lhs_parent_revid(self, mapping):
        try:
            return self._stored_lhs_parent_revid[mapping]
        except KeyError:
            pass
        try:
            self._retrieve(mapping)
        except KeyError:
            return self.base._get_stored_lhs_parent_revid(mapping)
        else:
            return self._stored_lhs_parent_revid[mapping]

    def get_revision_info(self, mapping):
        try:
            return self._revision_info[mapping]
        except KeyError:
            pass
        try:
            self._retrieve(mapping)
        except KeyError:
            self._determine(mapping)
            self._update_cache(mapping)
        return self._revision_info[mapping]

    def get_rhs_parents(self, mapping):
        return self.get_parent_ids(mapping)[1:]

    def get_parent_ids(self, mapping):
        """Find the parent ids of a revision."""
        myrevid = self.get_revision_id(mapping)
        if self._parents_cache is not None:
            parent_ids = self._parents_cache.lookup_parents(myrevid)
            if parent_ids is not None:
                return parent_ids
        parent_ids = self.base.get_parent_ids(mapping)
        self._parents_cache.insert_parents(myrevid, parent_ids)
        self._parents_cache.commit_conditionally()
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


class RevisionMetadataBranch(object):
    """Describes a Bazaar-like branch in a Subversion repository."""

    def __init__(self, revmeta_provider=None, history_limit=None):
        self._revs = []
        self._revnums = []
        self._history_limit = history_limit
        self._revmeta_provider = revmeta_provider
        self._get_next = None

    def __len__(self):
        return len(self._revs)

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
            raise MetaHistoryIncomplete("No function to retrieve next revision")
        if self._history_limit and len(self._revs) >= self._history_limit:
            raise MetaHistoryIncomplete("Limited to %d revisions" % self._history_limit)
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
    revision range, under a specific path.

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
        if prefixes in ([""], None):
            self.from_prefixes = None
        else:
            self.from_prefixes = [prefix.strip("/") for prefix in prefixes]
        self.from_revnum = from_revnum
        self.to_revnum = to_revnum
        self._last_revnum = None
        self.layout = layout
        # Two-dimensional dictionary for each set of revision meta
        # branches that exist *after* a revision
        self._pending_ancestors = defaultdict(lambda: defaultdict(set))
        if self.from_prefixes is None:
            self._pending_prefixes = None
            self._prefixes = None
        else:
            self._pending_prefixes = defaultdict(set)
            self._prefixes = set(self.from_prefixes)
        self._ancestors = defaultdict(set)
        self._unusual = set()
        self._unusual_history = defaultdict(set)
        self._provider = provider
        self._actions = []
        self._iter_log = self._provider._log.iter_changes(self.from_prefixes, self.from_revnum, self.to_revnum, pb=pb)
        self._project = project
        self._pb = pb
        self._iter = self.do()

    def __iter__(self):
        return ListBuildingIterator(self._actions, self.next)

    def __repr__(self):
        return "<RevisionMetadataBrowser from %d to %d, layout: %r>" % (self.from_revnum, self.to_revnum, self.layout)

    def __eq__(self, other):
        return (type(self) == type(other) and
                self.from_revnum == other.from_revnum and
                self.to_revnum == other.to_revnum and
                self.from_prefixes == other.from_prefixes and
                self.layout == other.layout)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        if self.from_prefixes is None:
            prefixes = None
        else:
            prefixes = tuple(self.from_prefixes)
        return hash((type(self), self.from_revnum, self.to_revnum, prefixes, hash(self.layout)))

    def under_prefixes(self, path, prefixes):
        if prefixes is None:
            return True
        return any([x for x in prefixes if path.startswith(x+"/") or x == path or x == ""])

    def get_lhs_parent(self, revmeta):
        """Find the *direct* left hand side parent of a revision metadata object.

        :param revmeta: RevisionMetadata object
        :return: RevisionMetadata object for the *direct* left hand side parent
        """
        while not revmeta._direct_lhs_parent_known:
            try:
                self.next()
            except StopIteration:
                if self.to_revnum > 0 or self.from_prefixes:
                    raise MetaHistoryIncomplete("Reached revision 0 or outside of prefixes.")
                raise AssertionError("Unable to find direct lhs parent for %r" % revmeta)
        return revmeta._direct_lhs_parent_revmeta

    def fetch_until(self, revnum):
        """Fetch at least all revisions until revnum.

        :param revnum: Revision until which to fetch.
        """
        try:
            while self._last_revnum is None or self._last_revnum > revnum:
                self.next()
        except StopIteration:
            return

    def next(self):
        """Return the next action.

        :return: Tuple with action string and object.
        """
        ret = self._iter.next()
        self._actions.append(ret)
        return ret

    def do(self):
        """Yield revisions and deleted branches.

        This is where the *real* magic happens.
        """
        for (paths, revnum, revprops) in self._iter_log:
            assert revnum <= self.from_revnum
            if self._pb:
                self._pb.update("discovering revisions",
                        abs(self.from_revnum-revnum),
                        abs(self.from_revnum-self.to_revnum))

            if self._last_revnum is not None:
                # Import all metabranches_history where key > revnum
                for x in xrange(self._last_revnum, revnum-1, -1):
                    for bp in self._pending_ancestors[x].keys():
                        self._ancestors[bp].update(self._pending_ancestors[x][bp])
                    del self._pending_ancestors[x]
                    self._unusual.update(self._unusual_history[x])
                    del self._unusual_history[x]
                    if self._prefixes is not None:
                        self._prefixes.update(self._pending_prefixes[x])

            # Eliminate anything that's not under prefixes/
            if self._prefixes is not None:
                for bp in self._ancestors.keys():
                    if not self.under_prefixes(bp, self._prefixes):
                        del self._ancestors[bp]
                        self._unusual.discard(bp)

            changed_bps = set()
            deletes = []

            if paths == {}:
                paths = {"": ("M", None, -1)}

            # Find out what branches have changed
            for p in sorted(paths):
                if self._prefixes is not None and not self.under_prefixes(p, self._prefixes):
                    continue
                action = paths[p][0]
                try:
                    (_, bp, ip) = self.layout.split_project_path(p, self._project)
                except svn_errors.NotSvnBranchPath:
                    pass
                else:
                    # Did something change inside a branch?
                    if action != 'D' or ip != "":
                        changed_bps.add(bp)
                for u in self._unusual:
                    if (p == u and not action in ('D', 'R')) or p.startswith("%s/" % u):
                        changed_bps.add(u)
                if action in ('R', 'D') and (
                    self.layout.is_branch_or_tag(p, self._project) or
                    self.layout.is_branch_or_tag_parent(p, self._project)):
                    deletes.append(p)

            # Mention deletes
            for d in deletes:
                yield ("delete", (p, revnum))

            # Dictionary with the last revision known for each branch
            # Report the new revisions
            for bp in changed_bps:
                revmeta = self._provider.get_revision(bp, revnum, paths,
                    revprops, metaiterator=self)
                assert revmeta is not None
                for c in self._ancestors[bp]:
                    c._set_direct_lhs_parent_revmeta(revmeta)
                del self._ancestors[bp]
                self._ancestors[bp] = set([revmeta])
                # If this branch was started here, terminate it
                if (bp in paths and paths[bp][0] in ('A', 'R') and
                    paths[bp][1] is None):
                    revmeta._set_direct_lhs_parent_revmeta(None)
                    del self._ancestors[bp]
                yield "revision", revmeta

            # Apply reverse renames and the like for the next round
            for new_name, old_name, old_rev in changes.apply_reverse_changes(
                self._ancestors.keys(), paths):
                self._unusual.discard(new_name)
                if old_name is None:
                    # Didn't exist previously, mark as beginning and remove.
                    for rev in self._ancestors[new_name]:
                        if rev.branch_path != new_name:
                            raise AssertionError("Revision %d has invalid branch path %s, expected %s" % (revnum, rev.branch_path, new_name))
                        rev._set_direct_lhs_parent_revmeta(None)
                    del self._ancestors[new_name]
                else:
                    # was originated somewhere else
                    data = self._ancestors[new_name]
                    del self._ancestors[new_name]
                    self._pending_ancestors[old_rev][old_name].update(data)
                    if not self.layout.is_branch_or_tag(old_name, self._project):
                        self._unusual_history[old_rev].add(old_name)

            # Update the prefixes, if necessary
            if self._prefixes:
                for new_name, old_name, old_rev in changes.apply_reverse_changes(
                    self._prefixes, paths):
                    if old_name is None:
                        # Didn't exist previously, terminate prefix
                        self._prefixes.discard(new_name)
                        if len(self._prefixes) == 0:
                            return
                    else:
                        self._prefixes.discard(new_name)
                        self._pending_prefixes[old_rev].add(old_name)

            self._last_revnum = revnum


def filter_revisions(it):
    """Filter out all revisions out of a stream with changes."""
    for kind, rev in it:
        if kind == "revision":
            yield rev


def restrict_prefixes(prefixes, prefix):
    """Trim a list of prefixes down as much as possible.

    :param prefixes: List of prefixes to check
    :param prefix: Prefix to restrict to
    :return: Set with the remaining prefixes
    """
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
        """Create a new RevisionMetadata instance, assuming this
        revision isn't cached yet.
        """
        return self._revmeta_cls(self.repository, self.check_revprops,
                                 self._get_fileprops_fn, self._log,
                                 self.repository.uuid, path, revnum, changes,
                                 revprops, changed_fileprops=changed_fileprops,
                                 fileprops=fileprops, metaiterator=metaiterator)

    def add_metaiterator(self, iterator):
        self._open_metaiterators.append(iterator)

    def lookup_revision(self, path, revnum, revprops=None):
        """Lookup a revision, optionally checking whether there are any
        unchecked metaiterators that perhaps contain the revision."""
        if not getattr(self._log, "cache", False):
            # finish fetching any open revisionmetadata branches for
            # which the latest fetched revnum > revnum
            for mb in self._open_metaiterators:
                if self.in_cache(path, revnum):
                    break
                mb.fetch_until(revnum)
        return self.get_revision(path, revnum, revprops=revprops)

    def finish_metaiterators(self):
        for mb in self._open_metaiterators:
            mb.fetch_until(0)

    def in_cache(self, path, revnum):
        return (path, revnum) in self._revmeta_cache

    def get_revision(self, path, revnum, changes=None, revprops=None,
                     changed_fileprops=None, fileprops=None, metaiterator=None):
        """Return a RevisionMetadata object for a specific svn (path,revnum)."""
        assert isinstance(path, str)
        assert isinstance(revnum, int)

        if self.in_cache(path, revnum):
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
        self.add_metaiterator(metabranch)
        return metabranch

    def iter_all_revisions(self, layout, check_unusual_path, from_revnum,
                           to_revnum=0, project=None, pb=None):
        """Iterate over all RevisionMetadata objects in a repository.

        :param layout: Repository layout to use
        :param check_unusual_path: Check whether to keep branch

        Layout decides which ones to pick up.
        """
        return filter_revisions(self.iter_all_changes(layout,
            check_unusual_path, from_revnum, to_revnum, project, pb))

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
        self.add_metaiterator(browser)
        for kind, item in browser:
            if kind != "revision" or check_unusual_path(item.branch_path):
                yield kind, item
