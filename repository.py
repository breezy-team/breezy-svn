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

from __future__ import absolute_import

from itertools import chain
import subvertpy

from breezy import (
    branch,
    errors as bzr_errors,
    graph,
    transactions,
    ui,
    urlutils,
    )
from breezy.decorators import only_raises
from breezy.foreign import (
    ForeignRepository,
    )
from breezy.inventory import (
    Inventory,
    )
from breezy.repository import (
    Repository,
    RepositoryFormat,
    needs_read_lock,
    )
from breezy.revisiontree import InventoryRevisionTree
from breezy.revision import (
    NULL_REVISION,
    ensure_null,
    )
from breezy.trace import (
    note,
    )
from breezy.transport import (
    Transport,
    )

from breezy.plugins.svn import (
    cache,
    changes,
    errors,
    layout,
    logwalker,
    revmeta,
    )
from breezy.plugins.svn.branchprops import (
    PathPropertyProvider,
    )
from breezy.plugins.svn.config import SvnRepositoryStack
from breezy.plugins.svn.fileids import (
    CachingFileIdMapStore,
    FileIdMapStore,
    simple_apply_changes,
    )
from breezy.plugins.svn.graph import (
    SubversionGraph,
    )
from breezy.plugins.svn.layout.standard import (
    WildcardLayout,
    )
from breezy.plugins.svn.layout.guess import (
    repository_guess_layout,
    )
from breezy.plugins.svn.mapping import (
    SVN_REVPROP_BZR_SIGNATURE,
    SVN_REVPROP_BZR_TIMESTAMP,
    BzrSvnMapping,
    foreign_vcs_svn,
    find_mappings_fileprops,
    find_mapping_revprops,
    is_bzr_revision_revprops,
    mapping_registry,
    parse_svn_dateprop,
    unpack_highres_date,
    )
from breezy.plugins.svn.parents import (
    DiskCachingParentsProvider,
    )
from breezy.plugins.svn.revids import (
    DiskCachingRevidMap,
    MemoryCachingRevidMap,
    RevidMap,
    )
from breezy.plugins.svn.tree import (
    SvnRevisionTree,
    )
from breezy.plugins.svn.filegraph import (
    PerFileParentProvider,
    )

LAYOUT_SOURCE_GUESSED = 'guess'
LAYOUT_SOURCE_CONFIG = 'config'
LAYOUT_SOURCE_OVERRIDDEN = 'overridden'
LAYOUT_SOURCE_MAPPING_MANDATED = 'mapping-mandated'


class SubversionRepositoryLock(object):
    """Subversion lock."""

    def __init__(self, repository):
        self.repository_token = None
        self.repository = repository

    def unlock(self):
        self.repository.unlock()


class DummyLockableFiles(object):

    def __init__(self, transport):
        self._transport = transport


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
    supports_versioned_directories = True
    supports_nesting_repositories = False
    supports_unreferenced_revisions = False

    @property
    def _matchingbzrdir(self):
        from breezy.plugins.svn.remote import SvnRemoteFormat
        return SvnRemoteFormat()

    def __init__(self):
        super(SvnRepositoryFormat, self).__init__()

    def get_format_description(self):
        return "Subversion Repository"

    def network_name(self):
        return "subversion"

    def initialize(self, controldir, shared=False, _internal=False):
        from breezy.plugins.svn.remote import SvnRemoteAccess
        if not isinstance(controldir, SvnRemoteAccess):
            raise bzr_errors.UninitializableFormat(self)
        return controldir.open_repository()

    def get_foreign_tests_repository_factory(self):
        from breezy.plugins.svn.tests.test_repository import (
            ForeignTestsRepositoryFactory,
            )
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
        mapping.check_revprops(revmeta.metarev.revprops, self)

        lhs_parent_revmeta = revmeta.get_lhs_parent_revmeta(mapping)
        for parent_revid in revmeta.get_rhs_parents(mapping, lhs_parent_revmeta):
            if not self.repository.has_revision(parent_revid):
                self.ghost_revisions.add(parent_revid)

        # TODO: Check that stored revno matches actual revision number

        # Check all paths are under branch root
        branch_root = mapping.get_branch_root(revmeta.metarev.revprops)
        if branch_root is not None:
            for p in revmeta.metarev.paths:
                if not changes.path_is_child(branch_root, p):
                    self.paths_not_under_branch_root += 1

        original_uuid = mapping.get_repository_uuid(revmeta.metarev.revprops)
        if (original_uuid is not None and
            original_uuid != self.repository.uuid):
            self.different_uuid_cnt += 1

        if lhs_parent_revmeta is not None:
            lhs_parent_mapping = lhs_parent_revmeta.get_original_mapping()
            if lhs_parent_mapping is None:
                lhs_parent_mapping = mapping
            elif lhs_parent_mapping.newer_than(mapping):
                self.newer_mapping_parents += 1
                lhs_parent_mapping = mapping
            expected_lhs_parent_revid = lhs_parent_revmeta.get_revision_id(
                lhs_parent_mapping)
            if revmeta.get_stored_lhs_parent_revid(mapping) not in (
                    None, expected_lhs_parent_revid):
                self.inconsistent_stored_lhs_parent += 1
        self.check_texts(revmeta, mapping, lhs_parent_revmeta)

    def check_texts(self, revmeta, mapping, lhs_parent_revmeta):
        # Check for inconsistencies in text file ids/revisions
        text_revisions = revmeta.get_text_revisions(mapping)
        text_ids = revmeta.get_fileid_overrides(mapping)
        fileid_map = self.repository.get_fileid_map(revmeta, mapping)
        path_changes = revmeta.metarev.paths
        for path in set(text_ids.keys() + text_revisions.keys()):
            full_path = urlutils.join(revmeta.metarev.branch_path, path)
            # TODO Check consistency against parent data
        ghost_parents = False
        parent_revmetas = []
        parent_mappings = []
        parent_fileid_maps = []
        for revid in revmeta.get_parent_ids(mapping, lhs_parent_revmeta):
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
            for parent_fileid_map, parent_mapping in zip(parent_fileid_maps,
                    parent_mappings):
                try:
                    parent_text_revisions.append(
                        parent_fileid_map.reverse_lookup(parent_mapping, fileid))
                except KeyError:
                    pass
            if (text_revision != revmeta.get_revision_id(mapping) and
                    not ghost_parents and
                    not text_revision in parent_text_revisions):
                self.invalid_text_revisions += 1


_optimizers_registered = False
def lazy_register_optimizers():
    """Register optimizers for fetching between svn and bzr repositories.
    """
    global _optimizers_registered
    if _optimizers_registered:
        return
    from breezy.repository import InterRepository
    from breezy.plugins.svn import push, fetch
    _optimizers_registered = True
    InterRepository.register_optimiser(fetch.InterFromSvnRepository)
    InterRepository.register_optimiser(push.InterToSvnRepository)


class SvnRepository(ForeignRepository):
    """
    Provides a simplified interface to a Subversion repository
    by using the RA (remote access) API from subversion
    """

    chk_bytes = None

    def __init__(self, some_dir, transport, branch_path=None):
        lazy_register_optimizers()
        self.vcs = foreign_vcs_svn
        _revision_store = None

        assert isinstance(transport, Transport)

        control_files = DummyLockableFiles(transport)
        Repository.__init__(self, SvnRepositoryFormat(), some_dir, control_files)
        self._transport = transport
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
            if log_cache.max_revnum() > self.get_latest_revnum():
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
        self._parents_provider.disable_cache()
        self.texts = None
        self.revisions = None
        self.inventories = None
        self.signatures = None

        self.branchprop_list = PathPropertyProvider(self._log)

        self._revmeta_provider = revmeta.RevisionMetadataProvider(self,
                self.revinfo_cache is not None)

    def _get_bzrdir(self):
        # This is done lazily since the actual repository root may not be
        # accessible, and we don't really need it.
        if self._some_controldir.svn_root_url == self._some_controldir.svn_url:
            return self._some_controldir
        from breezy.plugins.svn.remote import SvnRemoteAccess
        self._some_controldir = SvnRemoteAccess(self._some_controldir.root_transport.clone_root())
        return self._some_controldir

    def _set_bzrdir(self, value):
        self._some_controldir = value

    bzrdir = property(_get_bzrdir, _set_bzrdir)

    def break_lock(self):
        raise NotImplementedError(self.break_lock)

    def dont_leave_lock_in_place(self):
        raise NotImplementedError(self.dont_leave_lock_in_place)

    def leave_lock_in_place(self):
        raise NotImplementedError(self.leave_lock_in_place)

    @needs_read_lock
    def get_file_graph(self):
        """Return the graph walker for text revisions."""
        return graph.Graph(graph.CachingParentsProvider(
            PerFileParentProvider(self)))

    def get_graph(self, other_repository=None):
        """Return the graph walker for this repository format"""
        parents_provider = self._make_parents_provider()
        if (other_repository is not None and
            not self.has_same_location(other_repository)):
            parents_provider = graph.StackedParentsProvider(
                [parents_provider, other_repository._make_parents_provider()])
        return SubversionGraph(self, parents_provider)

    def add_fallback_repository(self, basis_url):
        raise bzr_errors.UnstackableRepositoryFormat(self._format, self.base)

    def pack(self, hint=None, clean_obsolete_packs=False):
        try:
            local_path = self.transport.local_abspath(".")
        except bzr_errors.NotLocalUrl:
            pass
        else:
            from subvertpy import repos
            repo = repos.Repository(local_path)
            pack_fs = getattr(repo, "pack_fs", None)
            if pack_fs is not None:
                pack_fs()

    def get_transaction(self):
        """See Repository.get_transaction()."""
        if self._write_group is None:
            return transactions.PassThroughTransaction()
        else:
            return self._write_group

    def lock_read(self):
        if self._lock_mode:
            assert self._lock_mode in ('r', 'w')
            self._lock_count += 1
        else:
            self._lock_mode = 'r'
            self._lock_count = 1
            self._parents_provider.enable_cache()
        return self

    @only_raises(bzr_errors.LockNotHeld, bzr_errors.LockBroken)
    def unlock(self):
        if self._lock_count == 0:
            raise bzr_errors.LockNotHeld(self)
        if self._lock_count == 1 and self._lock_mode == 'w':
            if self._write_group is not None:
                self.abort_write_group()
                self._lock_count -= 1
                self._lock_mode = None
                raise bzr_errors.BzrError(
                    'Must end write groups before releasing write locks.')
        self._lock_count -= 1
        if self._lock_count == 0:
            self._lock_mode = None
            self._clear_cached_state()
            self._parents_provider.disable_cache()

    def reconcile(self, other=None, thorough=False):
        """Reconcile this repository."""
        from breezy.plugins.svn.reconcile import RepoReconciler
        reconciler = RepoReconciler(self, thorough=thorough)
        reconciler.reconcile()
        return reconciler

    def _cache_add_new_revision(self, revnum, revid, parents):
        assert self.is_locked()
        assert type(parents) == tuple or parents is None
        self._cached_revnum = max(revnum, self._cached_revnum)
        if parents == () and revid != NULL_REVISION:
            parents = (NULL_REVISION,)
        parents_cache = self._parents_provider._cache
        if parents is None:
            if revid in parents_cache and parents_cache[revid] is None:
                del parents_cache[revid]
        else:
            parents_cache[revid] = parents

    def _clear_cached_state(self):
        self._cached_tags = {}
        self._cached_revnum = None
        self._layout = None
        self._layout_source = None
        if self._cache_obj is not None:
            self._cache_obj.commit()

    def lock_write(self):
        """See Branch.lock_write()."""
        if self._lock_mode:
            assert self._lock_mode == 'w'
            self._lock_count += 1
        else:
            self._lock_mode = 'w'
            self._lock_count = 1
            self._parents_provider.enable_cache()
        return SubversionRepositoryLock(self)

    def is_write_locked(self):
        return (self._lock_mode == 'w')

    def is_locked(self):
        return (self._lock_mode is not None)

    @errors.convert_svn_error
    def get_latest_revnum(self):
        """Return the youngest revnum in the Subversion repository.

        Will be cached when there is a read lock open.
        """
        if self._cached_revnum is not None:
            return self._cached_revnum
        revnum = self.transport.get_latest_revnum()
        if self.is_locked():
            self._cached_revnum = revnum
        return revnum

    @needs_read_lock
    def gather_stats(self, revid=None, committers=None):
        """See Repository.gather_stats()."""
        result = {}
        def check_timestamp(timestamp):
            if not 'latestrev' in result:
                result['latestrev'] = timestamp
            result['firstrev'] = timestamp

        count = 0
        if committers is not None and revid is not None:
            all_committers = set()
            graph = self.get_graph()
            revids = [r for (r,ps) in graph.iter_ancestry([revid]) if r !=
                    NULL_REVISION and ps is not None]
            for rev in self.get_revisions(revids):
                if rev.committer != '':
                    all_committers.add(rev.committer)
                check_timestamp((rev.timestamp, rev.timezone))
            result['committers'] = len(all_committers)
            count = len(list(self.all_revision_ids()))
        else:
            mapping = self.get_mapping()
            for revmeta in self._revmeta_provider.iter_all_revisions(
                    self.get_layout(), mapping.is_branch_or_tag,
                    self.get_latest_revnum()):
                if revmeta.is_hidden(mapping):
                    continue
                try:
                    timestamp = unpack_highres_date(
                        revmeta.metarev.revprops[SVN_REVPROP_BZR_TIMESTAMP])
                except KeyError:
                    timestamp = parse_svn_dateprop(
                        revmeta.metarev.revprops[
                            subvertpy.properties.PROP_REVISION_DATE])
                check_timestamp(timestamp)
                count += 1
        # Approximate number of revisions
        result['revisions'] = count
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
            not target_config.get('allow_metadata_in_file_properties')):
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
        for revision in revisions:
            yield self.get_delta_for_revision(revision)

    def get_revision_delta(self, revid, specific_fileids=None):
        return self.get_delta_for_revision(self.get_revision(revid),
            specific_fileids=specific_fileids)

    def get_delta_for_revision(self, revision, specific_fileids=None):
        """See Repository.get_delta_for_revision()."""
        parentrevmeta = revision.svn_meta.get_lhs_parent_revmeta(
            revision.mapping)
        from breezy.plugins.svn.fetch import TreeDeltaBuildEditor
        if parentrevmeta is None:
            parentfileidmap = {}
            parent_branch_path = revision.svn_meta.metarev.branch_path
            parentrevnum = revision.svn_meta.metarev.revnum
            start_empty = True
        else:
            parentfileidmap = self.get_fileid_map(parentrevmeta,
                revision.mapping)
            parent_branch_path = parentrevmeta.metarev.branch_path
            parentrevnum = parentrevmeta.metarev.revnum
            start_empty = False
        editor = TreeDeltaBuildEditor(revision.svn_meta, revision.mapping,
            self.get_fileid_map(revision.svn_meta, revision.mapping),
            parentfileidmap, specific_fileids=specific_fileids)
        conn = self.transport.get_connection(parent_branch_path)
        try:
            reporter = conn.do_diff(revision.svn_meta.metarev.revnum, "",
                    urlutils.join(self.transport.get_svn_repos_root(),
                        revision.svn_meta.metarev.branch_path).rstrip("/"), editor,
                    True, True, False)
            try:
                reporter.set_path("", parentrevnum, start_empty)
                reporter.finish()
            except:
                reporter.abort()
                raise
        finally:
            self.transport.add_connection(conn)
        editor.sort()
        return editor.delta

    def set_layout(self, layout):
        """Set the layout that should be used by default by this instance."""
        self.get_mapping().check_layout(self, layout)
        self._layout = layout
        self._layout_source = LAYOUT_SOURCE_OVERRIDDEN

    def store_layout(self, layout):
        """Permanantly store the layout for this repository."""
        self.set_layout(layout)
        self.get_config_stack().set('layout', str(layout))
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
            config_stack = self.get_config_stack()
            layoutname = config_stack.get('layout')
            if layoutname is not None:
                self._layout_source = LAYOUT_SOURCE_CONFIG
                self._layout = layout.layout_registry.get(layoutname)()
        if self._layout is None:
            config_stack = self.get_config_stack()
            branches = config_stack.get('branches')
            tags = config_stack.get('tags')
            if branches is not None:
                self._layout_source = LAYOUT_SOURCE_CONFIG
                self._layout = WildcardLayout(branches, tags or [])
        if self._layout is None:
            if self._guessed_appropriate_layout is None:
                self._find_guessed_layout(self.get_config_stack())
            self._layout_source = LAYOUT_SOURCE_GUESSED
            self._layout = self._guessed_appropriate_layout
        return self._layout, self._layout_source

    def _find_guessed_layout(self, config_stack):
        # Retrieve guessed-layout from config and see if it accepts
        # self._hinted_branch_path
        layoutname = config_stack.get('guessed-layout')
        # Cope with 'None' being saved as layout name
        if layoutname == 'None':
            layoutname = None
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
        if (self._guessed_layout != config_guessed_layout and
            revnum > 200 and
            self._guessed_layout is not None):
            config_stack.set('guessed-layout', self._guessed_layout)

    def get_guessed_layout(self):
        """Retrieve the layout bzr-svn deems most appropriate for this repo.
        """
        if self._guessed_layout is None:
            # Assume whatever is registered is appropriate
            self._guessed_layout = layout.repository_registry.get(self.uuid)
        if self._guessed_layout is None:
            self._guessed_layout = self.get_mapping().get_guessed_layout(self)
        if self._guessed_layout is None:
            self._find_guessed_layout(self.get_config_stack())
        return self._guessed_layout

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.base)

    def _check(self, revision_ids=None, callback_refs=None, check_repo=True):
        ret = SubversionRepositoryCheckResult(self)
        # FIXME: If this is a local repository, call out to subvertpy.repos.verify_fs.
        # bug=843944
        pb = ui.ui_factory.nested_progress_bar()
        try:
            if revision_ids is not None:
                for i, (revid, revmeta, mapping) in enumerate(
                        self._iter_revmetas(revision_ids)):
                    if revmeta is None:
                        continue
                    if revid == NULL_REVISION:
                        continue
                    pb.update("checking revisions", i, len(revision_ids))
                    ret.check_revmeta(revmeta)
            else:
                last_revnum = self.get_latest_revnum()
                for revmeta in self._revmeta_provider.iter_all_revisions(
                        self.get_layout(), self.get_mapping().is_branch_or_tag,
                        last_revnum):
                    pb.update("checking revisions",
                        last_revnum-revmeta.metarev.revnum, last_revnum)
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
        return self.fileid_map.get_map(revmeta.metarev.get_foreign_revid(), mapping)

    def all_revision_ids(self, layout=None, mapping=None):
        """Find all revision ids in this repository, using the specified or
        default mapping.

        :note: This will use the standard layout to find the revisions,
               any revisions using non-standard branch locations (even
               if part of the ancestry of valid revisions) won't be
               returned.
        """
        ret = set()
        if mapping is None:
            mapping = self.get_mapping()
        if layout is None:
            layout = self.get_layout()
        for revmeta in self._revmeta_provider.iter_all_revisions(layout,
                mapping.is_branch_or_tag, self.get_latest_revnum()):
            if revmeta.is_hidden(mapping):
                continue
            ret.add(revmeta.get_revision_id(mapping))
        return ret

    def set_make_working_trees(self, new_value):
        """See Repository.set_make_working_trees()."""
        raise bzr_errors.UnsupportedOperation(self.set_make_working_trees, self)

    def make_working_trees(self):
        """See Repository.make_working_trees().

        Always returns False, as working trees are never created inside
        Subversion repositories.
        """
        return False

    def get_known_graph_ancestry(self, keys):
        """Get a KnownGraph instance with the ancestry of keys."""
        # most basic implementation is a loop around get_parent_map
        pending = set(keys)
        parent_map = {}
        while pending:
            this_parent_map = {}
            for revid in pending:
                if revid == NULL_REVISION:
                    continue
                parents = self._get_parents(revid)
                if parents is not None:
                    if tuple(parents) == (NULL_REVISION,):
                        parents = ()
                    this_parent_map[revid] = parents
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

    def iter_files_bytes(self, desired_files):
        per_revision = {}
        for (file_id, revision_id, identifier) in desired_files:
            per_revision.setdefault(revision_id, []).append(
                (file_id, identifier))
        for revid, files in per_revision.iteritems():
            try:
                tree = self.revision_tree(revid)
            except bzr_errors.NoSuchRevision:
                raise bzr_errors.RevisionNotPresent(revid, self)
            for identifier, curfile in tree.iter_files_bytes(files):
                yield identifier, curfile

    def revision_trees(self, revids):
        """See Repository.revision_trees()."""
        # TODO: Use diffs
        for revid in revids:
            yield self.revision_tree(revid)

    def revision_tree(self, revision_id):
        """See Repository.revision_tree()."""
        if ensure_null(revision_id) == NULL_REVISION:
            inventory = Inventory(root_id=None)
            inventory.revision_id = revision_id
            return InventoryRevisionTree(self, inventory, revision_id)

        return SvnRevisionTree(self, revision_id)

    def _get_parents(self, revid):
        try:
            revmeta, mapping = self._get_revmeta(ensure_null(revid))
        except bzr_errors.NoSuchRevision:
            return None
        else:
            parentrevmeta = revmeta.get_lhs_parent_revmeta(mapping)
            return revmeta.get_parent_ids(mapping, parentrevmeta)

    def get_parent_map(self, revids):
        """See Repository.get_parent_map()."""
        parent_map = {}
        for revision_id in revids:
            if type(revision_id) != str:
                raise ValueError("expected sequence of revision ids")
            if revision_id == NULL_REVISION:
                parent_map[revision_id] = ()
                continue
            parents = self._get_parents(revision_id)
            if parents is None:
                continue
            if len(parents) == 0:
                parents = [NULL_REVISION]
            parent_map[revision_id] = tuple(parents)
        return parent_map

    def _iter_revmetas(self, revision_ids):
        for revid in revision_ids:
            if revid == NULL_REVISION:
                yield (NULL_REVISION, None, None)
            else:
                try:
                    (revmeta, mapping) = self._get_revmeta(revid)
                except bzr_errors.NoSuchRevision:
                    yield (revid, None, None)
                else:
                    yield (revid, revmeta, mapping)

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
        parentrevmeta = revmeta.get_lhs_parent_revmeta(mapping)
        return revmeta.get_revision(mapping, parentrevmeta)

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
            if is_bzr_revision_revprops(revmeta.metarev.revprops):
                return True
        return False

    def get_config(self):
        return self.bzrdir.get_config()

    def get_config_stack(self):
        return SvnRepositoryStack(self)

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
        revmeta.metarev.refresh_revprops()
        return revmeta.get_signature() is not None

    def get_signature_text(self, revision_id):
        """Return the signature text for a particular revision.

        :param revision_id: Id of the revision for which to return the
                            signature.
        :raises NoSuchRevision: Always
        """
        revmeta, mapping = self._get_revmeta(revision_id)
        # Make sure revprops are fresh, not cached:
        revmeta.metarev.refresh_revprops()
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
    def find_branches(self, using=False, layout=None, revnum=None, mapping=None):
        """Find branches underneath this repository.

        This will include branches inside other branches.
        """
        from breezy.plugins.svn.branch import SvnBranch # avoid circular imports
        # All branches use this repository, so the using argument can be
        # ignored.
        if layout is None:
            layout = self.get_layout()
        if revnum is None:
            revnum = self.get_latest_revnum()

        branches = []
        if mapping is None:
            mapping = self.get_mapping()
        pb = ui.ui_factory.nested_progress_bar()
        try:
            for project, bp, nick, has_props, revnum in layout.get_branches(self,
                    revnum):
                branches.append(SvnBranch(self, self.bzrdir, bp, mapping,
                    _skip_check=True, revnum=revnum))
        finally:
            pb.finished()
        return branches

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

        if layout in self._cached_tags and revnum == self.get_latest_revnum():
            # Use the cache, if it's there
            return self._cached_tags[layout]

        ret = {}
        for (project, branch_path, name, has_props, revnum) in layout.get_tags(self,
                revnum, project=project):
            assert type(name) is unicode
            base_revmeta = self._revmeta_provider.lookup_revision(branch_path, revnum)
            ret[name] = base_revmeta.get_tag_revmeta(mapping)

        if revnum == self.get_latest_revnum():
            # Cache
            self._cached_tags[layout] = ret

        return ret

    @needs_read_lock
    def find_tags_between(self, project, layout, mapping, from_revnum,
                          to_revnum, tags=None):
        return find_tags_between(self._revmeta_provider, project, layout,
            mapping, from_revnum, to_revnum, tags)

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
        if self._lock_mode != 'w':
            raise bzr_errors.NotWriteLocked(self)
        from breezy.plugins.svn.commit import SvnCommitBuilder
        if branch is None:
            raise Exception("branch option is required for "
                            "SvnRepository.get_commit_builder")
        append_revisions_only = branch.get_config_stack().get('append_revisions_only')
        if append_revisions_only is None:
            append_revisions_only = True
        base_revmeta, base_mapping = branch.last_revmeta(skip_hidden=False)
        if len(parents) == 0 or tuple(parents) == (NULL_REVISION,):
            base_foreign_revid = None
            if base_revmeta is None:
                root_action = ("create", )
                base_mapping = None
                branch_path = branch._branch_path
            elif branch.last_revision() == NULL_REVISION:
                root_action = ("open", base_revmeta.metarev.revnum)
                branch_path = base_revmeta.metarev.branch_path
            else:
                root_action = ("replace", base_revmeta.metarev.revnum)
                branch_path = base_revmeta.metarev.branch_path
        else:
            branch_path = base_revmeta.metarev.branch_path

            if parents[0] != branch.last_revision():
                root_action = ("replace", base_revmeta.metarev.revnum)
                base_foreign_revid, base_mapping = \
                     self.lookup_bzr_revision_id(parents[0], project=branch.project)
            else:
                root_action = ("open", base_revmeta.metarev.revnum)
                base_foreign_revid = base_revmeta.metarev.get_foreign_revid()

        if root_action[0] == "replace" and append_revisions_only:
            raise bzr_errors.AppendRevisionsOnlyViolation(branch.base)

        ret = SvnCommitBuilder(self, branch_path, parents,
                                config, timestamp, timezone, committer,
                                revprops, revision_id,
                                base_foreign_revid, base_mapping,
                                root_action=root_action,
                                push_metadata=not lossy,
                                branch=branch)
        self.start_write_group()
        return ret

    def find_fileprop_paths(self, layout, from_revnum, to_revnum,
                               project=None, check_removed=False):
        assert from_revnum >= to_revnum
        if not check_removed and to_revnum == 0:
            it = iter([])
            it = chain(it, layout.get_branches(self, from_revnum, project))
            it = chain(it, layout.get_tags(self, from_revnum, project))
            return iter(((branch, revnum, True) for
                (project, branch, nick, has_props, revnum) in it if has_props
                in (True, None)))
        else:
            return iter(find_branches_between(self._log, self.transport,
                layout, from_revnum, to_revnum, project))


def find_branches_between(logwalker, transport, layout, from_revnum, to_revnum,
        project=None):
    """Find all branch paths that were changed in the specified revision range.

    :note: This ignores forbidden paths.

    :param revnum: Revision to search for branches.
    :return: iterator that returns tuples with (path, revision number,
        still exists). The revision number is the revision in which the
        branch last existed.
    """
    assert from_revnum >= to_revnum
    if project is not None:
        prefixes = filter(
            lambda p: transport.check_path(p, from_revnum),
            layout.get_project_prefixes(project))
    else:
        prefixes = [""]

    created_branches = {}
    ret = []

    pb = ui.ui_factory.nested_progress_bar()
    try:
        for (paths, i, revprops) in logwalker.iter_changes(prefixes,
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
                                for c in transport.get_dir(p, i)[0].keys():
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


def find_tags_between(revmeta_provider, project, layout, mapping, from_revnum,
                      to_revnum, tags=None):
    if tags is None:
        tags = {}
    assert from_revnum <= to_revnum
    pb = ui.ui_factory.nested_progress_bar()
    try:
        entries = []
        for kind, item in revmeta_provider.iter_all_changes(layout,
                mapping.is_branch_or_tag, to_revnum, from_revnum,
                project=project):
            if kind == "revision":
                pb.update("discovering tags", to_revnum-item.metarev.revnum,
                    to_revnum-from_revnum)
                if layout.is_tag(item.metarev.branch_path):
                    entries.append((
                        kind,
                        (item.metarev.branch_path,
                        (item.metarev.revnum, item.get_tag_revmeta(mapping)))))
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
        name = name.decode("utf-8")
        assert type(name) is unicode
        ret[name] = revmeta
    return ret
