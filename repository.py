# Copyright (C) 2006-2011 Jelmer Vernooij <jelmer@samba.org>

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
"""Subversion repository access."""

try:
    from collections import defaultdict
except ImportError:
    from bzrlib.plugins.svn.pycompat import defaultdict

from itertools import chain
import subvertpy

from bzrlib import (
    branch,
    errors as bzr_errors,
    graph,
    transactions,
    ui,
    urlutils,
    )
from bzrlib.foreign import (
    ForeignRepository,
    )
from bzrlib.inventory import (
    Inventory,
    )
from bzrlib.repository import (
    Repository,
    RepositoryFormat,
    needs_read_lock,
    )
try:
    from bzrlib.revisiontree import InventoryRevisionTree
except ImportError:# bzr < 2.4
    from bzrlib.revisiontree import RevisionTree as InventoryRevisionTree
from bzrlib.revision import (
    NULL_REVISION,
    ensure_null,
    )
from bzrlib.trace import (
    note,
    )
from bzrlib.transport import (
    Transport,
    )

from bzrlib.plugins.svn import (
    cache,
    changes,
    errors,
    layout,
    logwalker,
    revmeta,
    )
from bzrlib.plugins.svn.branchprops import (
    PathPropertyProvider,
    )
from bzrlib.plugins.svn.fileids import (
    CachingFileIdMapStore,
    FileIdMapStore,
    simple_apply_changes,
    )
from bzrlib.plugins.svn.layout.standard import (
    WildcardLayout,
    )
from bzrlib.plugins.svn.layout.guess import (
    repository_guess_layout,
    )
from bzrlib.plugins.svn.mapping import (
    SVN_REVPROP_BZR_SIGNATURE,
    BzrSvnMapping,
    foreign_vcs_svn,
    find_mappings_fileprops,
    find_mapping_revprops,
    is_bzr_revision_revprops,
    mapping_registry,
    parse_svn_dateprop,
    )
from bzrlib.plugins.svn.parents import (
    DiskCachingParentsProvider,
    )
from bzrlib.plugins.svn.revids import (
    DiskCachingRevidMap,
    MemoryCachingRevidMap,
    RevidMap,
    )
from bzrlib.plugins.svn.tree import (
    SvnRevisionTree,
    )
from bzrlib.plugins.svn.versionedfiles import (
    SvnTexts,
    )

LAYOUT_SOURCE_GUESSED = 'guess'
LAYOUT_SOURCE_CONFIG = 'config'
LAYOUT_SOURCE_REGISTRY = 'registry'
LAYOUT_SOURCE_OVERRIDDEN = 'overridden'
LAYOUT_SOURCE_MAPPING_MANDATED = 'mapping-mandated'

class DummyLockableFiles(object):

    def __init__(self, transport):
        self._transport = transport

    def create_lock(self):
        pass

    def break_lock(self):
        pass

    def leave_in_place(self):
        pass

    def dont_leave_in_place(self):
        pass

    def lock_write(self, token=None):
        pass

    def lock_read(self):
        pass

    def unlock(self):
        pass

    def is_locked(self):
        """Return true if this LockableFiles group is locked"""
        return False

    def get_physical_lock_status(self):
        """Return physical lock status.

        Returns true if a lock is held on the transport. If no lock is held, or
        the underlying locking mechanism does not support querying lock
        status, false is returned.
        """
        return False

    def get_transaction(self):
        """Return the current active transaction.

        If no transaction is active, this returns a passthrough object
        for which all data is immediately flushed and no caching happens.
        """
        return transactions.PassThroughTransaction()


class SvnRepositoryFormat(RepositoryFormat):
    """Repository format for Subversion repositories (accessed using svn_ra).
    """

    rich_root_data = True
    supports_tree_reference = False
    _serializer = None
    supports_leaving_lock = False
    fast_deltas = True
    supports_full_versioned_files = False
    supports_funky_characters = True
    supports_external_lookups = False
    revision_graph_can_have_wrong_parents = False

    @property
    def _matchingbzrdir(self):
        from bzrlib.plugins.svn.remote import SvnRemoteFormat
        return SvnRemoteFormat()

    def __init__(self):
        super(SvnRepositoryFormat, self).__init__()

    def get_format_description(self):
        return "Subversion Repository"

    def network_name(self):
        return "subversion"

    def initialize(self, controldir, shared=False, _internal=False):
        from bzrlib.plugins.svn.remote import SvnRemoteAccess
        if not isinstance(controldir, SvnRemoteAccess):
            raise bzr_errors.UninitializableFormat(self)
        return controldir.open_repository()

    def get_foreign_tests_repository_factory(self):
        from bzrlib.plugins.svn.tests.test_repository import ForeignTestsRepositoryFactory
        return ForeignTestsRepositoryFactory()

    def check_conversion_target(self, target_repo_format):
        if not target_repo_format.rich_root_data:
            raise bzr_errors.BadConversionTarget(
                'Does not support rich root data.', target_repo_format)


class SubversionRepositoryCheckResult(branch.BranchCheckResult):
    """Results of checking a Subversion repository."""

    def __init__(self, repository):
        self.repository = repository
        self.checked_rev_cnt = 0
        self.checked_roundtripped_cnt = 0
        self.hidden_rev_cnt = 0
        self.inconsistent_stored_lhs_parent = 0
        self.invalid_fileprop_cnt = 0
        self.text_revision_in_parents_cnt = 0
        self.paths_not_under_branch_root = 0
        self.different_uuid_cnt = 0
        self.multiple_mappings_cnt = 0
        self.inconsistent_fileprop_revprop_cnt = 0
        self.newer_mapping_parents = 0
        self.ghost_revisions = set()
        self.invalid_text_parents_len = 0
        self.invalid_text_revisions = 0

    def report_results(self, verbose):
        note('checked repository %s format %s',
             self.repository.bzrdir.root_transport,
             self.repository._format)
        note('%6d revisions', self.checked_rev_cnt)
        if self.checked_roundtripped_cnt > 0:
            note('%6d revisions originating in bzr',
                self.checked_roundtripped_cnt)
        if self.hidden_rev_cnt > 0:
            note('%6d hidden bzr-created revisions', self.hidden_rev_cnt)
        if self.inconsistent_stored_lhs_parent > 0:
            note('%6d inconsistent stored left-hand side parent revision ids',
                self.inconsistent_stored_lhs_parent)
        if self.invalid_fileprop_cnt > 0:
            note('%6d invalid bzr-related file properties',
                 self.invalid_fileprop_cnt)
        if self.paths_not_under_branch_root > 0:
            note('%6d files were modified that were not under the branch root',
                 self.paths_not_under_branch_root)
        if self.different_uuid_cnt > 0:
            note('%6d revisions originating in a different repository',
                 self.different_uuid_cnt)
        if self.multiple_mappings_cnt > 0:
            note('%6d revisions with multiple mappings',
                 self.multiple_mappings_cnt)
        if self.inconsistent_fileprop_revprop_cnt > 0:
            note('%6d revisions with file and revision properties that are inconsistent',
                self.inconsistent_fileprop_revprop_cnt)
        if self.newer_mapping_parents > 0:
            note('%6d revisions with newer mappings than their children',
                 self.newer_mapping_parents)
        if len(self.ghost_revisions) > 0:
            note('%6d ghost revisions', len(self.ghost_revisions))
        if self.invalid_text_parents_len > 0:
            note('%6d text parents with invalid number',
                self.invalid_text_parents_len)
        if self.invalid_text_revisions > 0:
            note('%6d invalid text revisions', self.invalid_text_revisions)

    def check_revmeta(self, revmeta):
        self.checked_rev_cnt += 1
        found_fileprops = 0
        # Check for multiple mappings
        try:
            fileprop_mappings = find_mappings_fileprops(
                revmeta.get_changed_fileprops())
        except subvertpy.SubversionException, (_, num):
            if num == subvertpy.ERR_FS_NOT_DIRECTORY:
                return
            raise
        if len(fileprop_mappings) > 1:
            self.multiple_mappings_cnt += 1
        mapping = revmeta.get_original_mapping()
        if mapping is None:
            return
        if (len(fileprop_mappings) > 0 and
            find_mapping_revprops(revmeta.revprops) not in
                (None, fileprop_mappings[0])):
            self.inconsistent_fileprop_revprop_cnt += 1
        self.checked_roundtripped_cnt += 1
        if revmeta.is_hidden(mapping):
            self.hidden_rev_cnt += 1

        mapping.check_fileprops(revmeta.get_changed_fileprops(), self)
        mapping.check_revprops(revmeta.revprops, self)

        for parent_revid in revmeta.get_rhs_parents(mapping):
            if not self.repository.has_revision(parent_revid):
                self.ghost_revisions.add(parent_revid)

        # TODO: Check that stored revno matches actual revision number

        # Check all paths are under branch root
        branch_root = mapping.get_branch_root(revmeta.revprops)
        if branch_root is not None:
            for p in revmeta.paths:
                if not changes.path_is_child(branch_root, p):
                    self.paths_not_under_branch_root += 1

        original_uuid = mapping.get_repository_uuid(revmeta.revprops)
        if (original_uuid is not None and
            original_uuid != self.repository.uuid):
            self.different_uuid_cnt += 1

        lhs_parent_revmeta = revmeta.get_lhs_parent_revmeta(mapping)
        if lhs_parent_revmeta is not None:
            lhs_parent_mapping = lhs_parent_revmeta.get_original_mapping()
            if lhs_parent_mapping is None:
                lhs_parent_mapping = mapping
            elif lhs_parent_mapping.newer_than(mapping):
                self.newer_mapping_parents += 1
                lhs_parent_mapping = mapping
            expected_lhs_parent_revid = lhs_parent_revmeta.get_revision_id(
                lhs_parent_mapping)
            if revmeta.get_stored_lhs_parent_revid(mapping) not in (None, expected_lhs_parent_revid):
                self.inconsistent_stored_lhs_parent += 1
        self.check_texts(revmeta, mapping)

    def check_texts(self, revmeta, mapping):
        # Check for inconsistencies in text file ids/revisions
        text_revisions = revmeta.get_text_revisions(mapping)
        text_ids = revmeta.get_fileid_overrides(mapping)
        fileid_map = self.repository.get_fileid_map(revmeta, mapping)
        path_changes = revmeta.paths
        for path in set(text_ids.keys() + text_revisions.keys()):
            full_path = urlutils.join(revmeta.branch_path, path)
            # TODO Check consistency against parent data
        ghost_parents = False
        parent_revmetas = []
        parent_mappings = []
        parent_fileid_maps = []
        for revid in revmeta.get_parent_ids(mapping):
            try:
                parent_revmeta, parent_mapping = self.repository._get_revmeta(
                    revid)
            except bzr_errors.NoSuchRevision:
                ghost_parents = True
            else:
                parent_revmetas.append(parent_revmeta)
                parent_mappings.append(parent_mapping)
                parent_fileid_map = self.repository.get_fileid_map(
                    parent_revmeta, parent_mapping)
                parent_fileid_maps.append(parent_fileid_map)
        for path, text_revision in text_revisions.iteritems():
            # Every text revision either has to match the actual revision's
            # revision id (if it was last changed there) or the text revisions
            # in one of the parents.
            fileid = fileid_map.lookup(mapping, path)[0]
            parent_text_revisions = []
            for parent_fileid_map, parent_mapping in zip(parent_fileid_maps, parent_mappings):
                parent_text_revisions.append(parent_fileid_map.reverse_lookup(parent_mapping, fileid))
            if (text_revision != revmeta.get_revision_id(mapping) and
                    not ghost_parents and
                    not text_revision in parent_text_revisions):
                self.invalid_text_revisions += 1


class SvnRepository(ForeignRepository):
    """
    Provides a simplified interface to a Subversion repository
    by using the RA (remote access) API from subversion
    """

    chk_bytes = None

    def __init__(self, bzrdir, transport, branch_path=None):
        from bzrlib.plugins.svn import lazy_register_optimizers
        lazy_register_optimizers()
        self.vcs = foreign_vcs_svn
        _revision_store = None

        assert isinstance(transport, Transport)

        control_files = DummyLockableFiles(transport)
        Repository.__init__(self, SvnRepositoryFormat(), bzrdir, control_files)

        self._cached_revnum = None
        self._lock_mode = None
        self._lock_count = 0
        self._layout = None
        self._layout_source = None
        self._guessed_layout = None
        self._guessed_appropriate_layout = None
        self.transport = transport
        self.uuid = transport.get_uuid()
        assert self.uuid is not None
        self.base = transport.base
        assert self.base is not None
        self._serializer = None
        self.get_config().add_location(self.base)
        self._log = logwalker.LogWalker(transport=transport)
        self.fileid_map = FileIdMapStore(simple_apply_changes, self)
        self.revmap = RevidMap(self)
        self._default_mapping = None
        self._hinted_branch_path = branch_path
        self._real_parents_provider = self
        self._cached_tags = {}
        self._cache_obj = None

        use_cache = self.get_config().get_use_cache()

        if use_cache is None:
            # TODO: Don't enable log cache in some situations, e.g.
            # for large repositories ?
            if self.base.startswith("file://"):
                # Default to no log caching for local connections
                use_cache = set(["fileids", "revids"])
            else:
                use_cache = set(["fileids", "revids", "revinfo", "log"])

        if use_cache:
            self._cache_obj = cache.get_cache(self.uuid)

        if "log" in use_cache:
            log_cache = self._cache_obj.open_logwalker()
            if log_cache.last_revnum() > self.get_latest_revnum():
                errors.warn_uuid_reuse(self.uuid, self.base)
            self._log = logwalker.CachingLogWalker(self._log,
                log_cache)

        if "fileids" in use_cache:
            self.fileid_map = CachingFileIdMapStore(
                self._cache_obj.open_fileid_map(), self.fileid_map)

        if "revids" in use_cache:
            self.revmap = DiskCachingRevidMap(self.revmap,
                self._cache_obj.open_revid_map())
            self._real_parents_provider = DiskCachingParentsProvider(
                self._real_parents_provider, self._cache_obj.open_parents())
        else:
            self.revmap = MemoryCachingRevidMap(self.revmap)

        if "revinfo" in use_cache:
            self.revinfo_cache = self._cache_obj.open_revision_cache()
        else:
            self.revinfo_cache = None

        self._parents_provider = graph.CachingParentsProvider(
            self._real_parents_provider)
        self.texts = SvnTexts(self)
        self.revisions = None
        self.inventories = None
        self.signatures = None

        self.branchprop_list = PathPropertyProvider(self._log)

        self._revmeta_provider = revmeta.RevisionMetadataProvider(self,
                self.revinfo_cache is not None)

    def revision_graph_can_have_wrong_parents(self):
        # DEPRECATED in bzr 2.4
        return False

    def get_transaction(self):
        """See Repository.get_transaction()."""
        raise NotImplementedError(self.get_transaction)

    def lock_read(self):
        if self._lock_mode:
            assert self._lock_mode in ('r', 'w')
            self._lock_count += 1
        else:
            self._lock_mode = 'r'
            self._lock_count = 1

    def unlock(self):
        """See Branch.unlock()."""
        self._lock_count -= 1
        if self._lock_count == 0:
            if self._cache_obj is not None:
                self._cache_obj.commit()
            self._lock_mode = None
            self._clear_cached_state()

    def reconcile(self, other=None, thorough=False):
        """Reconcile this repository."""
        from bzrlib.plugins.svn.reconcile import RepoReconciler
        reconciler = RepoReconciler(self, thorough=thorough)
        reconciler.reconcile()
        return reconciler

    def _clear_cached_state(self, revnum=None):
        self._cached_tags = {}
        self._cached_revnum = revnum
        self._layout = None
        self._layout_source = None
        self._parents_provider = graph.CachingParentsProvider(
            self._real_parents_provider)

    def lock_write(self):
        """See Branch.lock_write()."""
        if self._lock_mode:
            assert self._lock_mode == 'w'
            self._lock_count += 1
        else:
            self._lock_mode = 'w'
            self._lock_count = 1

    def is_write_locked(self):
        return (self._lock_mode == 'w')

    @errors.convert_svn_error
    def get_latest_revnum(self):
        """Return the youngest revnum in the Subversion repository.

        Will be cached when there is a read lock open.
        """
        if self._lock_mode in ('r','w') and self._cached_revnum is not None:
            return self._cached_revnum
        self._cached_revnum = self.transport.get_latest_revnum()
        return self._cached_revnum

    def item_keys_introduced_by(self, revision_ids, _files_pb=None):
        """See Repository.item_keys_introduced_by()."""
        fileids = defaultdict(set)

        for count, (revid, d) in enumerate(zip(revision_ids,
            self.get_deltas_for_revisions(self.get_revisions(revision_ids)))):
            if _files_pb is not None:
                _files_pb.update("fetch revisions for texts", count,
                    len(revision_ids))
            for c in d.added + d.modified:
                fileids[c[1]].add(revid)
            for c in d.renamed:
                fileids[c[2]].add(revid)

        for fileid, altered_versions in fileids.iteritems():
            yield ("file", fileid, altered_versions)

        # We're done with the files_pb.  Note that it finished by the caller,
        # just as it was created by the caller.
        del _files_pb

        yield ("inventory", None, revision_ids)

        # signatures
        # XXX: Note ATM no callers actually pay attention to this return
        #      instead they just use the list of revision ids and ignore
        #      missing sigs. Consider removing this work entirely
        revisions_with_signatures = set()
        for revid in revision_ids:
            if self.has_signature_for_revision_id(revid):
                revisions_with_signatures.add(revid)
        yield ("signatures", None, revisions_with_signatures)
        yield ("revisions", None, revision_ids)

    @needs_read_lock
    def gather_stats(self, revid=None, committers=None):
        """See Repository.gather_stats()."""
        result = {}
        def revdate(revnum):
            revprops = self._log.revprop_list(revnum)
            return parse_svn_dateprop(revprops[subvertpy.properties.PROP_REVISION_DATE])
        if committers is not None and revid is not None:
            all_committers = set()
            for rev in self.get_revisions(filter(lambda r: r is not None and r != NULL_REVISION, self.has_revisions(self.get_ancestry(revid)))):
                if rev.committer != '':
                    all_committers.add(rev.committer)
            result['committers'] = len(all_committers)
        result['firstrev'] = revdate(0)
        result['latestrev'] = revdate(self.get_latest_revnum())
        # Approximate number of revisions
        result['revisions'] = self.get_latest_revnum()+1
        result['svn-uuid'] = self.uuid
        result['svn-last-revnum'] = self.get_latest_revnum()
        return result

    def _properties_to_set(self, mapping, target_config):
        """Determine what sort of custom properties to set when
        committing a new round-tripped revision.

        :return: tuple with two booleans: whether to use revision properties
            and whether to use file properties.
        """
        supports_custom_revprops = self.transport.has_capability(
            "commit-revprops")
        if supports_custom_revprops and mapping.can_use_revprops:
            use_revprops = True
            use_fileprops = mapping.must_use_fileprops
        else:
            use_revprops = False
            use_fileprops = mapping.can_use_fileprops
        if (use_fileprops and
            not target_config.get_allow_metadata_in_fileprops()):
            raise errors.RequiresMetadataInFileProps()
        return (use_revprops, use_fileprops)

    def get_mapping_class(self):
        config_mapping_name = self.get_config().get_default_mapping()
        if config_mapping_name is not None:
            return mapping_registry.get(config_mapping_name)
        return mapping_registry.get_default()

    def get_mapping(self):
        """Get the default mapping that is used for this repository."""
        if self._default_mapping is None:
            mappingcls = self.get_mapping_class()
            self._default_mapping = mappingcls.from_repository(self,
                self._hinted_branch_path)
        return self._default_mapping

    def _make_parents_provider(self):
        """See Repository._make_parents_provider()."""
        return self._parents_provider

    def get_deltas_for_revisions(self, revisions):
        """See Repository.get_deltas_for_revisions()."""
        """See Repository.get_deltas_for_revisions()."""
        for revision in revisions:
            yield self.get_delta_for_revision(revision)

    def get_revision_delta(self, revid):
        return self.get_delta_for_revision(self.get_revision(revid))

    def get_delta_for_revision(self, revision):
        """See Repository.get_delta_for_revision()."""
        parentrevmeta = revision.svn_meta.get_lhs_parent_revmeta(
            revision.mapping)
        from bzrlib.plugins.svn.fetch import TreeDeltaBuildEditor
        if parentrevmeta is None:
            parentfileidmap = {}
            parent_branch_path = revision.svn_meta.branch_path
            parentrevnum = revision.svn_meta.revnum
            start_empty = True
        else:
            parentfileidmap = self.get_fileid_map(parentrevmeta,
                revision.mapping)
            parent_branch_path = parentrevmeta.branch_path
            parentrevnum = parentrevmeta.revnum
            start_empty = False
        editor = TreeDeltaBuildEditor(revision.svn_meta, revision.mapping,
            self.get_fileid_map(revision.svn_meta, revision.mapping),
            parentfileidmap)
        conn = self.transport.get_connection(parent_branch_path)
        try:
            reporter = conn.do_diff(revision.svn_meta.revnum, "",
                    urlutils.join(self.transport.get_svn_repos_root(),
                        revision.svn_meta.branch_path).rstrip("/"), editor,
                    True, True, False)
            try:
                reporter.set_path("", parentrevnum, start_empty)
                reporter.finish()
            except:
                reporter.abort()
                raise
        finally:
            self.transport.add_connection(conn)
        return editor.delta

    def _annotate(self, path, revnum, fileid, revid, mapping):
        assert type(path) is str
        from bzrlib.plugins.svn.annotate import Annotater
        annotater = Annotater(self, mapping, fileid, revid)
        annotater.check_file_revs(path, revnum)
        return annotater.get_annotated()

    def set_layout(self, layout):
        """Set the layout that should be used by default by this instance."""
        self.get_mapping().check_layout(self, layout)
        self._layout = layout
        self._layout_source = LAYOUT_SOURCE_OVERRIDDEN

    def store_layout(self, layout):
        """Permanantly store the layout for this repository."""
        self.set_layout(layout)
        self.get_config().set_layout(layout)
        self._layout_source = LAYOUT_SOURCE_CONFIG

    def get_layout(self):
        """Determine layout to use for this repository.

        This will use whatever layout the user has specified, or
        otherwise the layout that was guessed by bzr-svn.
        """
        return self.get_layout_source()[0]

    def get_layout_source(self):
        if self._layout is None:
            self._layout_source = LAYOUT_SOURCE_MAPPING_MANDATED
            self._layout = self.get_mapping().get_mandated_layout(self)
        if self._layout is None:
            layoutname = self.get_config().get_layout()
            if layoutname is not None:
                self._layout_source = LAYOUT_SOURCE_CONFIG
                self._layout = layout.layout_registry.get(layoutname)()
        if self._layout is None:
            branches = self.get_config().get_branches()
            tags = self.get_config().get_tags()
            if branches is not None:
                self._layout_source = LAYOUT_SOURCE_CONFIG
                self._layout = WildcardLayout(branches, tags or [])
        if self._layout is None:
            self._layout = layout.repository_registry.get(self.uuid)
            self._layout_source = LAYOUT_SOURCE_REGISTRY
        if self._layout is None:
            if self._guessed_appropriate_layout is None:
                self._find_guessed_layout()
            self._layout_source = LAYOUT_SOURCE_GUESSED
            self._layout = self._guessed_appropriate_layout
        return self._layout, self._layout_source

    def _find_guessed_layout(self):
        # Retrieve guessed-layout from config and see if it accepts
        # self._hinted_branch_path
        layoutname = self.get_config().get_guessed_layout()
        if layoutname is not None:
            config_guessed_layout = layout.layout_registry.get(layoutname)()
            if (self._hinted_branch_path is None or
                config_guessed_layout.is_branch(self._hinted_branch_path)):
                self._guessed_layout = config_guessed_layout
                self._guessed_appropriate_layout = config_guessed_layout
                return
        else:
            config_guessed_layout = None
        # No guessed layout stored yet or it doesn't accept
        # self._hinted_branch_path
        revnum = self.get_latest_revnum()
        (self._guessed_layout,
            self._guessed_appropriate_layout) = repository_guess_layout(self,
                    revnum, self._hinted_branch_path)
        if self._guessed_layout != config_guessed_layout and revnum > 200:
            self.get_config().set_guessed_layout(self._guessed_layout)

    def get_guessed_layout(self):
        """Retrieve the layout bzr-svn deems most appropriate for this repo.
        """
        if self._guessed_layout is None:
            # Assume whatever is registered is appropriate
            self._guessed_layout = layout.repository_registry.get(self.uuid)
        if self._guessed_layout is None:
            self._guessed_layout = self.get_mapping().get_guessed_layout(self)
        if self._guessed_layout is None:
            self._find_guessed_layout()
        return self._guessed_layout

    def _warn_if_deprecated(self): # for bzr < 2.4
        # This class isn't deprecated
        pass

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.base)

    def _check(self, revision_ids=None, callback_refs=None, check_repo=True):
        ret = SubversionRepositoryCheckResult(self)
        pb = ui.ui_factory.nested_progress_bar()
        try:
            if revision_ids is not None:
                for i, revid in enumerate(revision_ids):
                    pb.update("checking revisions", i, len(revision_ids))
                    revmeta, mapping = self._get_revmeta(revid)
                    ret.check_revmeta(revmeta)
            else:
                last_revnum = self.get_latest_revnum()
                for revmeta in self._revmeta_provider.iter_all_revisions(
                        self.get_layout(), self.get_mapping().is_branch_or_tag,
                        last_revnum):
                    pb.update("checking revisions",
                        last_revnum-revmeta.revnum, last_revnum)
                    ret.check_revmeta(revmeta)
        finally:
            pb.finished()
        return ret

    def get_inventory(self, revision_id):
        """See Repository.get_inventory()."""
        raise NotImplementedError(self.get_inventory)

    def _iter_inventories(self, revision_ids, ordering):
        raise NotImplementedError(self._iter_inventories)

    def get_fileid_map(self, revmeta, mapping):
        return self.fileid_map.get_map(revmeta.get_foreign_revid(), mapping)

    def all_revision_ids(self, layout=None, mapping=None):
        """Find all revision ids in this repository, using the specified or
        default mapping.

        :note: This will use the standard layout to find the revisions,
               any revisions using non-standard branch locations (even
               if part of the ancestry of valid revisions) won't be
               returned.
        """
        if mapping is None:
            mapping = self.get_mapping()
        if layout is None:
            layout = self.get_layout()
        for revmeta in self._revmeta_provider.iter_all_revisions(layout,
                mapping.is_branch_or_tag, self.get_latest_revnum()):
            if revmeta.is_hidden(mapping):
                continue
            yield revmeta.get_revision_id(mapping)

    def set_make_working_trees(self, new_value):
        """See Repository.set_make_working_trees()."""
        pass # ignored, nowhere to store it...

    def make_working_trees(self):
        """See Repository.make_working_trees().

        Always returns False, as working trees are never created inside
        Subversion repositories.
        """
        return False

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
                    rhs_parent_foreign_revid, rhs_parent_mapping = self.lookup_bzr_revision_id(rhs_parent_revid, foreign_sibling=revmeta.get_foreign_revid())
                except bzr_errors.NoSuchRevision:
                    pass
                else:
                    (_, rhs_parent_bp, rhs_parent_revnum) = rhs_parent_foreign_revid
                    update_todo(todo, self._iter_reverse_revmeta_mapping_history(rhs_parent_bp, rhs_parent_revnum, to_revnum=0, mapping=mapping, pb=pb))

    def _iter_reverse_revmeta_mapping_history(self, branch_path, revnum,
        to_revnum, mapping, pb=None, limit=0):
        """Iterate over the history of a RevisionMetadata object.

        This will skip hidden revisions.
        """
        def get_iter(branch_path, revnum):
            return self._revmeta_provider.iter_reverse_branch_changes(
                branch_path, revnum, to_revnum=to_revnum, pb=pb, limit=limit)
        assert mapping is not None
        expected_revid = None
        it = iter(get_iter(branch_path, revnum))
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
                foreign_sibling = revmeta.get_foreign_revid()
            if expected_revid is not None and revid != expected_revid:
                # Need to restart, branch root has changed
                if expected_revid == NULL_REVISION:
                    break
                foreign_revid, mapping = self.lookup_bzr_revision_id(
                    expected_revid, foreign_sibling=foreign_sibling)
                (_, branch_path, revnum) = foreign_revid
                it = get_iter(branch_path, revnum)
            elif revid is not None:
                if not mapping.is_branch_or_tag(revmeta.branch_path):
                    return
                yield revmeta, mapping
                expected_revid = revmeta._get_stored_lhs_parent_revid(mapping)
                mapping = lhs_mapping
            else:
                break

    def iter_reverse_revision_history(self, revision_id, pb=None, limit=0,
                                      project=None):
        """Iterate backwards through revision ids in the lefthand history

        :param revision_id: The revision id to start with.  All its lefthand
            ancestors will be traversed.
        """
        if revision_id in (None, NULL_REVISION):
            return
        foreign_revid, mapping = self.lookup_bzr_revision_id(revision_id,
            project=project)
        (uuid, branch_path, revnum) = foreign_revid
        assert uuid == self.uuid
        for revmeta, mapping in self._iter_reverse_revmeta_mapping_history(
            branch_path, revnum, to_revnum=0, mapping=mapping, pb=pb,
            limit=limit):
            if revmeta.is_hidden(mapping):
                continue
            yield revmeta.get_revision_id(mapping)

    def get_ancestry(self, revision_id, topo_sorted=True):
        """See Repository.get_ancestry().
        """
        ancestry = []
        graph = self.get_graph()
        for rev, parents in graph.iter_ancestry([revision_id]):
            if rev == NULL_REVISION:
                rev = None
            ancestry.append(rev)
        ancestry.reverse()
        return ancestry

    def get_known_graph_ancestry(self, keys):
        """Get a KnownGraph instance with the ancestry of keys."""
        # most basic implementation is a loop around get_parent_map
        pending = set(keys)
        parent_map = {}
        while pending:
            this_parent_map = self.get_parent_map(pending)
            parent_map.update(this_parent_map)
            pending = set()
            map(pending.update, this_parent_map.itervalues())
            pending = pending.difference(parent_map)
        kg = graph.KnownGraph(parent_map)
        return kg

    def has_revisions(self, revision_ids):
        ret = set()
        for revision_id in revision_ids:
            if self.has_revision(revision_id):
                ret.add(revision_id)
        return ret

    def has_revision(self, revision_id, project=None):
        """See Repository.has_revision()."""
        if revision_id in (None, NULL_REVISION):
            return True

        try:
            foreign_revid, _ = self.lookup_bzr_revision_id(revision_id,
                project=project)
        except bzr_errors.NoSuchRevision:
            return False

        return self.has_foreign_revision(foreign_revid)

    def has_foreign_revision(self, (uuid, path, revnum)):
        if uuid != self.uuid:
            return False
        try:
            kind = self.transport.check_path(path, revnum)
        except subvertpy.SubversionException, (_, num):
            if num == subvertpy.ERR_FS_NO_SUCH_REVISION:
                return False
            raise
        else:
            return (kind == subvertpy.NODE_DIR)

    def revision_trees(self, revids):
        """See Repository.revision_trees()."""
        # TODO: Use diffs
        for revid in revids:
            yield self.revision_tree(revid)

    def revision_tree(self, revision_id):
        """See Repository.revision_tree()."""
        if revision_id in (NULL_REVISION, None):
            inventory = Inventory(root_id=None)
            inventory.revision_id = revision_id
            return InventoryRevisionTree(self, inventory, revision_id)

        return SvnRevisionTree(self, revision_id)

    def get_parent_map(self, revids):
        """See Repository.get_parent_map()."""
        parent_map = {}
        for revision_id in revids:
            if revision_id == NULL_REVISION:
                parent_map[revision_id] = ()
                continue
            try:
                revmeta, mapping = self._get_revmeta(ensure_null(revision_id))
            except bzr_errors.NoSuchRevision:
                continue
            else:
                parent_map[revision_id] = revmeta.get_parent_ids(mapping)
        return parent_map

    def _get_revmeta(self, revision_id):
        foreign_revid, mapping = self.lookup_bzr_revision_id(revision_id)
        (uuid, branch, revnum) = foreign_revid
        revmeta = self._revmeta_provider.lookup_revision(branch, revnum)
        return revmeta, mapping

    def get_revision(self, revision_id):
        """See Repository.get_revision."""
        if not revision_id or not isinstance(revision_id, str):
            raise bzr_errors.InvalidRevisionId(revision_id=revision_id,
                branch=self)

        revmeta, mapping = self._get_revmeta(revision_id)
        return revmeta.get_revision(mapping)

    def get_revisions(self, revision_ids):
        """See Repository.get_revisions()."""
        # TODO: More efficient implementation?
        return map(self.get_revision, revision_ids)

    def add_revision(self, rev_id, rev, inv=None, config=None):
        """See Repository.add_revision()."""
        raise NotImplementedError(self.add_revision)

    def upgrade_foreign_revision_id(self, foreign_revid, newest_allowed):
        try:
            return self.lookup_foreign_revision_id(foreign_revid,
                newest_allowed)
        except errors.DifferentSubversionRepository:
            return None

    def lookup_foreign_revision_id(self, foreign_revid, newest_allowed):
        (uuid, path, revnum) = foreign_revid
        if uuid != self.uuid:
            raise errors.DifferentSubversionRepository(uuid, self.uuid)
        assert isinstance(newest_allowed, BzrSvnMapping)

        revmeta = self._revmeta_provider.lookup_revision(path, revnum)
        mapping = revmeta.get_appropriate_mappings(newest_allowed)[0]

        return revmeta.get_revision_id(mapping)

    def generate_revision_id(self, revnum, path, mapping):
        """Generate an unambiguous revision id.

        :param revnum: Subversion revision number.
        :param path: Branch path.
        :param mapping: Mapping to use.

        :return: New revision id.
        """
        assert isinstance(path, str)
        assert isinstance(revnum, int)
        foreign_revid = (self.uuid, path, revnum)
        return self.lookup_foreign_revision_id(foreign_revid, mapping)

    def lookup_bzr_revision_id(self, revid, layout=None, ancestry=None,
                           project=None, foreign_sibling=None):
        """Parse an existing Subversion-based revision id.

        :param revid: The revision id.
        :param layout: Optional repository layout to use when searching for
                       revisions
        :raises: NoSuchRevision
        :return: Tuple with foreign revision id and mapping.
        """
        # FIXME: Use ancestry
        # If there is no entry in the map, walk over all branches:
        # Try a simple parse
        try:
            foreign_revid, mapping = mapping_registry.parse_revision_id(revid)
            (uuid, branch_path, revnum) = foreign_revid
            assert isinstance(branch_path, str)
            assert isinstance(mapping, BzrSvnMapping)
            if uuid == self.uuid:
                return (self.uuid, branch_path, revnum), mapping
            # If the UUID doesn't match, this may still be a valid revision
            # id; a revision from another SVN repository may be pushed into
            # this one.
        except bzr_errors.InvalidRevisionId:
            pass

        if layout is None:
            layout = self.get_layout()
        if foreign_sibling is not None and project is None:
            try:
                project = layout.parse(foreign_sibling[1])[1]
            except errors.NotSvnBranchPath:
                project = None
        return self.revmap.get_branch_revnum(revid, layout, project)

    def seen_bzr_revprops(self):
        """Check for the presence of bzr-specific custom revision properties.
        """
        if self.transport.has_capability("commit-revprops") == False:
            return False
        for revmeta in self._revmeta_provider.iter_all_revisions(
                self.get_layout(), None, self.get_latest_revnum()):
            if is_bzr_revision_revprops(revmeta.revprops):
                return True
        return False

    def get_config(self):
        return self.bzrdir.get_config()

    def has_signature_for_revision_id(self, revision_id):
        """Check whether a signature exists for a particular revision id.

        :param revision_id: Revision id for which the signatures should be
            looked up.
        :return: False, as no signatures are stored for revisions in Subversion
            at the moment.
        """
        try:
            revmeta, mapping = self._get_revmeta(revision_id)
        except bzr_errors.NoSuchRevision:
            return False
        # Make sure revprops are fresh, not cached:
        revmeta._revprops = self.transport.revprop_list(revmeta.revnum)
        return revmeta.get_signature() is not None

    def get_signature_text(self, revision_id):
        """Return the signature text for a particular revision.

        :param revision_id: Id of the revision for which to return the
                            signature.
        :raises NoSuchRevision: Always
        """
        revmeta, mapping = self._get_revmeta(revision_id)
        # Make sure revprops are fresh, not cached:
        revmeta._revprops = self.transport.revprop_list(revmeta.revnum)
        signature = revmeta.get_signature()
        if signature is None:
            raise bzr_errors.NoSuchRevision(self, revision_id)
        return signature

    def add_signature_text(self, revision_id, signature):
        """Add a signature text to an existing revision."""
        foreign_revid, mapping = self.lookup_bzr_revision_id(revision_id)
        (uuid, path, revnum) = foreign_revid
        try:
            self.transport.change_rev_prop(revnum, SVN_REVPROP_BZR_SIGNATURE,
                signature)
        except subvertpy.SubversionException, (_, num):
            if num == subvertpy.ERR_REPOS_DISABLED_FEATURE:
                raise errors.RevpropChangeFailed(SVN_REVPROP_BZR_SIGNATURE)
            raise

    @needs_read_lock
    def find_branches(self, using=False, layout=None, revnum=None):
        """Find branches underneath this repository.

        This will include branches inside other branches.
        """
        from bzrlib.plugins.svn.branch import SvnBranch # avoid circular imports
        # All branches use this repository, so the using argument can be
        # ignored.
        if layout is None:
            layout = self.get_layout()
        if revnum is None:
            revnum = self.get_latest_revnum()

        branches = []
        pb = ui.ui_factory.nested_progress_bar()
        try:
            for project, bp, nick, has_props in layout.get_branches(self,
                    revnum, pb=pb):
                branches.append(SvnBranch(self, bp, _skip_check=True))
        finally:
            pb.finished()
        return branches

    @needs_read_lock
    def find_tags_between(self, project, layout, mapping, from_revnum,
                          to_revnum, tags=None):
        if tags is None:
            tags = {}
        assert from_revnum <= to_revnum
        pb = ui.ui_factory.nested_progress_bar()
        try:
            entries = []
            for kind, item in self._revmeta_provider.iter_all_changes(layout,
                    mapping.is_branch_or_tag, to_revnum, from_revnum,
                    project=project):
                if kind == "revision":
                    pb.update("discovering tags", to_revnum-item.revnum,
                        to_revnum-from_revnum)
                    if layout.is_tag(item.branch_path):
                        entries.append((kind, (item.branch_path, (item.revnum, item.get_tag_revmeta(mapping)))))
                else:
                    entries.append((kind, item))
            for kind, item in reversed(entries):
                if kind == "revision":
                    (branch_path, tag) = item
                    tags[branch_path] = tag
                elif kind == "delete":
                    (path, revnum) = item
                    for t, (lastrevnum, tagrevmeta) in tags.items():
                        if (changes.path_is_child(t, path) and
                            revnum > lastrevnum):
                            del tags[t]
        finally:
            pb.finished()

        ret = {}
        for path, (lastrevnum, revmeta) in tags.iteritems():
            name = layout.get_tag_name(path, project)
            # Layout wasn't able to determine tag name from path
            if name is None:
                continue
            ret[name] = revmeta
        return ret

    @needs_read_lock
    def find_tags(self, project, layout=None, mapping=None, revnum=None):
        """Find tags underneath this repository for the specified project.

        :param layout: Repository layout to use
        :param revnum: Revision number in which to check, None for latest.
        :param project: Name of the project to find tags for. None for all.
        :return: Dictionary mapping tag names to revision ids.
        """
        if revnum is None:
            revnum = self.get_latest_revnum()

        if layout is None:
            layout = self.get_layout()

        if mapping is None:
            mapping = self.get_mapping()

        if not layout.supports_tags():
            return {}

        if not layout in self._cached_tags:
            self._cached_tags[layout] = self.find_tags_between(project=project,
                    layout=layout, mapping=mapping, from_revnum=0,
                    to_revnum=revnum)
        return self._cached_tags[layout]

    def find_branchpaths(self, layout, from_revnum, to_revnum, project=None):
        """Find all branch paths that were changed in the specified revision
        range.

        :note: This ignores forbidden paths.

        :param revnum: Revision to search for branches.
        :return: iterator that returns tuples with (path, revision number,
            still exists). The revision number is the revision in which the
            branch last existed.
        """
        assert from_revnum >= to_revnum
        if to_revnum is None:
            to_revnum = self.get_latest_revnum()

        created_branches = {}

        ret = []

        if project is not None:
            prefixes = filter(self.transport.has,
                              layout.get_project_prefixes(project))
        else:
            prefixes = [""]

        pb = ui.ui_factory.nested_progress_bar()
        try:
            for (paths, i, revprops) in self._log.iter_changes(prefixes,
                    from_revnum, to_revnum):
                if ((isinstance(revprops, dict) or revprops.is_loaded) and 
                    is_bzr_revision_revprops(revprops)):
                    continue
                pb.update("finding branches", i, to_revnum)
                for p in sorted(paths.keys()):
                    (action, copyfrom_path, copyfrom_rev, kind) = paths[p]
                    if kind not in (subvertpy.NODE_DIR, subvertpy.NODE_UNKNOWN):
                        continue
                    if layout.is_branch_or_tag(p, project):
                        if action in ('R', 'D') and p in created_branches:
                            ret.append((p, created_branches[p], False))
                            del created_branches[p]

                        if action in ('A', 'R', 'M'):
                            created_branches[p] = i
                    elif layout.is_branch_or_tag_parent(p, project):
                        if action in ('R', 'D'):
                            k = created_branches.keys()
                            for c in k:
                                if (c.startswith(p+"/") and
                                    c in created_branches):
                                    ret.append((c, created_branches[c], False))
                                    del created_branches[c]
                        if action in ('A', 'R') and copyfrom_path is not None:
                            parents = [p]
                            while parents:
                                p = parents.pop()
                                try:
                                    for c in self.transport.get_dir(p, i)[0].keys():
                                        n = p+"/"+c
                                        if layout.is_branch_or_tag(n, project):
                                            created_branches[n] = i
                                        elif layout.is_branch_or_tag_parent(n, project):
                                            parents.append(n)
                                except subvertpy.SubversionException, (_, num):
                                    if num in (subvertpy.ERR_FS_NOT_DIRECTORY,
                                               subvertpy.ERR_RA_DAV_FORBIDDEN):
                                        continue
                                    raise
        finally:
            pb.finished()

        for p, i in created_branches.iteritems():
            ret.append((p, i, True))

        return ret

    def is_shared(self):
        """Return True if this repository is flagged as a shared repository."""
        return True

    def get_physical_lock_status(self):
        """See Repository.get_physical_lock_status()."""
        return False

    def get_commit_builder(self, branch, parents, config, timestamp=None,
                           timezone=None, committer=None, revprops=None,
                           revision_id=None, lossy=False):
        """See Repository.get_commit_builder()."""
        from bzrlib.plugins.svn.commit import SvnCommitBuilder
        if branch is None:
            raise Exception("branch option is required for "
                            "SvnRepository.get_commit_builder")
        append_revisions_only = branch.get_config().get_append_revisions_only()
        if append_revisions_only is None:
            append_revisions_only = True
        branch_last_revid = branch.last_revision()
        bp = branch.get_branch_path()
        if parents == [] or parents == [NULL_REVISION]:
            base_foreign_revid = None
            base_mapping = None
            root_action = ("replace", branch.get_revnum())
        else:
            base_foreign_revid, base_mapping = \
                self.lookup_bzr_revision_id(parents[0], project=branch.project)
            if ((base_foreign_revid[2] != branch.get_revnum() or
                base_foreign_revid[1] != bp)):
                root_action = ("replace", branch.get_revnum())
            else:
                root_action = ("open", )

        if root_action[0] == "replace" and append_revisions_only:
            raise bzr_errors.AppendRevisionsOnlyViolation(branch.base)

        return SvnCommitBuilder(self, bp, parents,
                                config, timestamp, timezone, committer,
                                revprops, revision_id,
                                base_foreign_revid, base_mapping,
                                root_action=root_action,
                                push_metadata=not lossy)

    def find_fileprop_paths(self, layout, from_revnum, to_revnum,
                               project=None, check_removed=False):
        assert from_revnum >= to_revnum
        if not check_removed and to_revnum == 0:
            it = iter([])
            it = chain(it, layout.get_branches(self, from_revnum, project))
            it = chain(it, layout.get_tags(self, from_revnum, project))
            return iter(((branch, from_revnum, True) for (project, branch, nick, has_props) in it if has_props in (True, None)))
        else:
            return iter(self.find_branchpaths(layout, from_revnum, to_revnum, project))

    def abort_write_group(self, suppress_errors=False):
        """See Repository.abort_write_group()."""
        self._write_group = None

    def commit_write_group(self):
        """See Repository.commit_write_group()."""
        self._write_group = None

    def start_write_group(self):
        """See Repository.commit_write_group()."""
        if not self.is_write_locked():
            raise bzr_errors.NotWriteLocked(self)
        if self._write_group is not None:
            raise bzr_errors.BzrError("A write group is already active")
        self._write_group = "active"
