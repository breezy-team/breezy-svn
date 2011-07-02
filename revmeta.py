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
    SVN_REVPROP_BZR_TESTAMENT,
    estimate_bzr_ancestors,
    find_mapping_fileprops,
    find_mapping_revprops,
    get_roundtrip_ancestor_revids,
    parse_svn_revprops,
    revprops_complete,
    )
from bzrlib.plugins.svn.metagraph import (
    MetaRevision,
    MetaRevisionGraph,
    )
from bzrlib.plugins.svn.svk import (
    estimate_svk_ancestors,
    parse_svk_feature,
    svk_features_merged_since,
    SVN_PROP_SVK_MERGE,
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


class BzrMetaRevision(object):
    """Object describing a revision with bzr semantics in a Subversion
    repository.

    Tries to be as lazy as possible - data is not retrieved or calculated
    from other known data before contacting the Subversions server.
    """

    __slots__ = ('_get_fileprops_fn',
                 '_changed_fileprops', '_fileprops',
                 '_consider_bzr_fileprops',
                 '_consider_bzr_revprops', '_estimated_fileprop_ancestors',
                 'children', 'metarev', 'provider',
                 '_revprop_redirect_revnum')

    def __init__(self, provider, get_fileprops_fn,
                 metarev, changed_fileprops=None, fileprops=None):
        self.metarev = metarev
        self.provider = provider
        self._get_fileprops_fn = get_fileprops_fn
        self._changed_fileprops = changed_fileprops
        self._fileprops = fileprops
        self._consider_bzr_fileprops = None
        self._consider_bzr_revprops = None
        self._estimated_fileprop_ancestors = {}

    def __eq__(self, other):
        return isinstance(other, BzrMetaRevision) and self.metarev == other.metarev

    def changes_branch_root(self):
        """Check whether the branch root was modified in this revision.
        """
        if self.knows_changed_fileprops():
            return self.get_changed_fileprops() != {}
        return self.metarev.branch_path in self.metarev.paths

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
            revid = mapping.revision_id_foreign_to_bzr(self.metarev.get_foreign_revid())

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
            if not changes.changes_children(self.metarev.paths, self.metarev.branch_path):
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
                lm._fileprops = self._get_fileprops_fn(lm.metarev.branch_path,
                    lm.metarev.revnum)
            for r in todo:
                r._fileprops = lm._fileprops
        return self._fileprops

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

    def get_direct_lhs_parent_revmeta(self):
        """Find the direct left hand side parent of this revision.

        This ignores mapping restrictions (invalid paths, hidden revisions).
        """
        metarev = self.metarev.get_lhs_parent()
        if metarev is None:
            return None
        return self.provider._revmeta_cls(self.provider,
                                 self._get_fileprops_fn,
                                 metarev)

    def get_lhs_parent_revmeta(self, mapping):
        """Get the revmeta object for the left hand side parent.

        :note: Returns None when there is no parent (parent is NULL_REVISION)
        """
        assert mapping.is_branch_or_tag(self.metarev.branch_path), \
                "%s not valid in %r" % (self.metarev.branch_path, mapping)
        def get_next_parent(nm):
            pm = nm.get_direct_lhs_parent_revmeta()
            if pm is None or mapping.is_branch_or_tag(pm.metarev.branch_path):
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
            return revprops.get(SVN_REVPROP_BZR_ROOT) == self.metarev.branch_path
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
        """Retrieve the revision id of the base revision (lhs parent).

        :note: Returns None if the base revision id was not explicitly stored.
        """
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

    @property
    def children(self):
        ret = set()
        for metarev in self.metarev.children:
            ret.add(self.provider._revmeta_cls(self.provider,
                             self._get_fileprops_fn,
                             metarev))
        return ret

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
            while lm and mapping.is_branch_or_tag(lm.metarev.branch_path):
                pb.update("determining revno", self.metarev.revnum-lm.metarev.revnum,
                          self.metarev.revnum)
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

        return self._import_from_props(mapping, get_fileprops,
            mapping.get_rhs_parents_revprops, (), consider_fileprops)

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
        return self.metarev.revprops.get(SVN_REVPROP_BZR_SIGNATURE)

    def get_testament(self):
        return self.metarev.revprops.get(SVN_REVPROP_BZR_TESTAMENT)

    def get_revision(self, mapping):
        """Create a revision object for this revision.

        :param mapping: Mapping to use
        """
        parent_ids = self.get_parent_ids(mapping)

        if parent_ids == (NULL_REVISION,):
            parent_ids = ()
        rev = ForeignRevision(foreign_revid=self.metarev.get_foreign_revid(),
                              mapping=mapping,
                              revision_id=self.get_revision_id(mapping),
                              parent_ids=parent_ids)

        parse_svn_revprops(self.metarev.revprops, rev)
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
                return (mapping.get_branch_root(revprops) == self.metarev.branch_path and
                        mapping.get_repository_uuid(revprops) in (None, self.metarev.uuid))
        if revprops_sufficient is None:
            revprops_sufficient = revprops_complete

        # Check revprops if self.knows_revprops() and can_use_revprops
        if can_use_revprops and self.metarev.knows_revprops():
            revprops = self.metarev.revprops
            if revprops_acceptable(revprops):
                ret = revprop_fn(revprops)
                if revprops_sufficient(revprops):
                    return ret
            can_use_revprops = False

        can_use_fileprops = can_use_fileprops and self.metarev.is_changes_root()

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
        if (can_use_revprops and not self.metarev.knows_revprops() and
            self.consider_bzr_revprops()):
            log_revprops_capab = self.provider._log._transport.has_capability(
                "log-revprops")
            if log_revprops_capab in (None, False):
                warn_slow_revprops(self.provider.repository.get_config(),
                                   log_revprops_capab == False)
            revprops = self.metarev.revprops
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
        if self.provider._log._transport.has_capability("commit-revprops") == False:
            # Server doesn't support setting revision properties
            self._consider_bzr_revprops = False
        elif self.provider._log._transport.has_capability("log-revprops") == True:
            self._consider_bzr_revprops = True
        elif self.provider._log._transport.has_capability("log-revprops") is None:
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


class CachingBzrMetaRevision(BzrMetaRevision):
    """Wrapper around BzrMetaRevision that stores some results in a cache."""

    __slots__ = ('base', '_revid_cache', '_revinfo_cache', '_revision_info',
                 '_original_mapping', '_original_mapping_set',
                 '_stored_lhs_parent_revid', '_parents_cache')

    def __init__(self, provider, *args, **kwargs):
        self.base = super(CachingBzrMetaRevision, self)
        self.base.__init__(provider, *args, **kwargs)
        self._parents_cache = getattr(
            self.provider.repository._real_parents_provider, "_cache", None)
        self._revid_cache = self.provider.repository.revmap.cache
        self._revinfo_cache = self.provider.repository.revinfo_cache
        self._revision_info = {}
        self._original_mapping = None
        self._original_mapping_set = False
        self._stored_lhs_parent_revid = {}

    def _update_cache(self, mapping):
        if (self.get_original_mapping() is not None and
            self._revision_info[mapping][1] is not None):
            self._revid_cache.insert_revid(self._revision_info[mapping][1],
                self.branch_path, self.revnum, self.revnum, mapping.name)
        self._revinfo_cache.insert_revision(self.metarev.get_foreign_revid(), mapping,
            self._revision_info[mapping],
            self._stored_lhs_parent_revid[mapping])

    def _determine(self, mapping):
        self._revision_info[mapping] = self.base.get_revision_info(mapping)
        self._stored_lhs_parent_revid[mapping] = self.base._get_stored_lhs_parent_revid(mapping)

    def _retrieve(self, mapping):
        assert mapping is not None
        (self._revision_info[mapping],
                self._stored_lhs_parent_revid[mapping]) = \
                 self._revinfo_cache.get_revision(self.metarev.get_foreign_revid(),
                                                  mapping)

    def get_original_mapping(self):
        if self._original_mapping_set:
            return self._original_mapping
        try:
            self._original_mapping = self._revinfo_cache.get_original_mapping(
                self.metarev.get_foreign_revid())
        except KeyError:
            self._original_mapping = self.base.get_original_mapping()
            self._revinfo_cache.set_original_mapping(self.metarev.get_foreign_revid(),
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


class RevisionMetadataProvider(object):
    """A RevisionMetadata provider."""

    def __init__(self, repository, cache):
        self._revmeta_cache = {}
        self.repository = repository
        self._get_fileprops_fn = self.repository.branchprop_list.get_properties
        self.lookup_bzr_revision_id = repository.lookup_bzr_revision_id
        self._log = repository._log
        self._graph = MetaRevisionGraph(self._log)
        if cache:
            self._revmeta_cls = CachingBzrMetaRevision
        else:
            self._revmeta_cls = BzrMetaRevision

    def create_revision(self, path, revnum, changes=None, revprops=None,
                        changed_fileprops=None, fileprops=None,
                        metaiterator=None):
        """Create a new RevisionMetadata instance, assuming this
        revision isn't cached yet.
        """
        metarev = MetaRevision(self._graph,
            self.repository.uuid, path, revnum, paths=changes,
            revprops=revprops)
        return self._revmeta_cls(self,
                                 self._get_fileprops_fn,
                                 metarev,
                                 changed_fileprops=changed_fileprops,
                                 fileprops=fileprops)

    def lookup_revision(self, path, revnum, revprops=None):
        """Lookup a revision, optionally checking whether there are any
        unchecked metaiterators that perhaps contain the revision."""
        if not getattr(self._log, "cache", False):
            # finish fetching any open revisionmetadata branches for
            # which the latest fetched revnum > revnum
            for mb in self._graph._open_metaiterators:
                if self.in_cache(path, revnum):
                    break
                mb.fetch_until(revnum)
        return self.get_revision(path, revnum, revprops=revprops)

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
                cached.metarev._paths = changes
            if cached._changed_fileprops is None:
                cached._changed_fileprops = changed_fileprops
            if cached._fileprops is None:
                cached._fileprops = fileprops
            if metaiterator is not None:
                cached.metarev.metaiterators.add(metaiterator)
            return self._revmeta_cache[path,revnum]

        ret = self.create_revision(path, revnum, changes, revprops,
                                   changed_fileprops, fileprops, metaiterator)
        self._revmeta_cache[path,revnum] = ret
        return ret

    def iter_reverse_branch_changes(self, branch_path, from_revnum, to_revnum,
                                    pb=None, limit=0):
        for metarev in self._graph.iter_reverse_branch_changes(
                branch_path, from_revnum, to_revnum, pb=pb, limit=limit):
            yield self._revmeta_cls(self,
                                 self._get_fileprops_fn,
                                 metarev)

    def iter_all_revisions(self, layout, check_unusual_path, from_revnum,
                           to_revnum=0, project=None, pb=None):
        for metarev in self._graph.iter_all_revisions(
            layout, check_unusual_path, from_revnum, to_revnum, project, pb):
            yield self._revmeta_cls(self, self._get_fileprops_fn, metarev)

    def iter_all_changes(self, layout, check_unusual_path, from_revnum,
                         to_revnum=0, project=None, pb=None, prefix=None):
        for kind, item in self._graph.iter_all_changes(layout,
            check_unusual_path, from_revnum, to_revnum, project, pb, prefix):
            if kind == "revision":
                yield "revision", self._revmeta_cls(self,
                    self._get_fileprops_fn, item)
            else:
                yield kind, item

    def _iter_reverse_revmeta_mapping_ancestry(self, branch_path, revnum,
            mapping, lhs_history=None, pb=None):
        """Iterate over the (revmeta, mapping) entries for the ancestry
        of a specified path.

        """
        todo = []
        processed = set()
        def update_todo(todo, it):
            for entry in it:
                if entry in processed:
                    return
                todo.append(entry)
                processed.add(entry)
        if lhs_history is None:
            update_todo(todo, self._iter_reverse_revmeta_mapping_history(
                branch_path, revnum, to_revnum=0, mapping=mapping, pb=pb))
        else:
            update_todo(todo, lhs_history)
        i = 0
        while todo:
            entry = todo.pop()
            if pb is not None:
                pb.update("finding rhs ancestors", i, i+len(todo))
            i += 1
            (revmeta, mapping) = entry
            yield entry
            for rhs_parent_revid in revmeta.get_rhs_parents(mapping):
                try:
                    (rhs_parent_foreign_revid, rhs_parent_mapping) = self.lookup_bzr_revision_id(rhs_parent_revid, foreign_sibling=revmeta.metarev.get_foreign_revid())
                except bzr_errors.NoSuchRevision:
                    pass
                else:
                    (_, rhs_parent_bp, rhs_parent_revnum) = rhs_parent_foreign_revid
                    update_todo(todo,
                            self._iter_reverse_revmeta_mapping_history(rhs_parent_bp,
                                rhs_parent_revnum, to_revnum=0,
                                mapping=mapping, pb=pb))

    def _iter_reverse_revmeta_mapping_history(self, branch_path, revnum,
        to_revnum, mapping, pb=None, limit=0):
        """Iterate over the history of a RevisionMetadata object.

        This will skip hidden revisions.
        """
        def get_iter(branch_path, revnum):
            return self.iter_reverse_branch_changes(
                branch_path, revnum, to_revnum=to_revnum, pb=pb, limit=limit)
        assert mapping is not None
        expected_revid = None
        it = get_iter(branch_path, revnum)
        while it:
            try:
                revmeta = it.next()
            except StopIteration:
                revid = None
                foreign_sibling = None
            else:
                (mapping, lhs_mapping) = revmeta.get_appropriate_mappings(
                    mapping)
                if lhs_mapping.newer_than(mapping):
                    raise AssertionError(
                        "LHS mapping %r newer than %r" %
                            (lhs_mapping, mapping))
                if revmeta.is_hidden(mapping):
                    mapping = lhs_mapping
                    continue
                revid = revmeta.get_revision_id(mapping)
                foreign_sibling = revmeta.metarev.get_foreign_revid()
            if expected_revid is not None and revid != expected_revid:
                # Need to restart, branch root has changed
                if expected_revid == NULL_REVISION:
                    break
                foreign_revid, mapping = self.lookup_bzr_revision_id(
                    expected_revid, foreign_sibling=foreign_sibling)
                (_, branch_path, revnum) = foreign_revid
                it = get_iter(branch_path, revnum)
            elif revid is not None:
                if not mapping.is_branch_or_tag(revmeta.metarev.branch_path):
                    return
                yield revmeta, mapping
                expected_revid = revmeta._get_stored_lhs_parent_revid(mapping)
                mapping = lhs_mapping
            else:
                break


class RevisionInfoCache(object):

    def set_original_mapping(self, foreign_revid, original_mapping):
        raise NotImplementedError(self.set_original_mapping)

    def insert_revision(self, foreign_revid, mapping, revinfo,
            stored_lhs_parent_revid):
        """Insert a revision to the cache.

        :param foreign_revid: Foreign revision id
        :param mapping: Mapping used
        :param revinfo: Tuple with (revno, revid, hidden)
        :param stored_lhs_parent_revid: Stored lhs parent revision
        """
        raise NotImplementedError(self.insert_revision)

    def get_revision(self, foreign_revid, mapping):
        """Get the revision metadata info for a (foreign_revid, mapping) tuple.

        :param foreign_revid: Foreign revision id
        :param mapping: Mapping
        :return: Tuple with (stored revno, revid, hidden),
            stored_lhs_parent_revid
        """
        raise NotImplementedError(self.get_revision)

    def get_original_mapping(self, foreign_revid):
        """Find the original mapping for a revision.

        :param foreign_revid: Foreign revision id
        :return: Mapping object or None
        """
        raise NotImplementedError(self.get_original_mapping)
