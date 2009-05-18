# Copyright (C) 2006-2009 Jelmer Vernooij <jelmer@samba.org>

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
import os
import subvertpy

from bzrlib import (
    branch,
    errors as bzr_errors,
    graph,
    osutils,
    ui,
    urlutils,
    )
from bzrlib.foreign import ForeignRepository
from bzrlib.inventory import Inventory
from bzrlib.lockable_files import (
    LockableFiles,
    TransportLock,
    )
from bzrlib.repository import (
    Repository,
    RepositoryFormat,
    needs_read_lock,
    )
from bzrlib.revisiontree import RevisionTree
from bzrlib.revision import (
    NULL_REVISION,
    ensure_null,
    )
from bzrlib.trace import (
    info,
    mutter,
    note,
    )
from bzrlib.transport import (
    Transport,
    get_transport,
    )

from bzrlib.plugins.svn import (
    cache,
    changes,
    errors,
    layout,
    logwalker,
    revmeta,
    )
from bzrlib.plugins.svn.branchprops import PathPropertyProvider
from bzrlib.plugins.svn.config import SvnRepositoryConfig
from bzrlib.plugins.svn.fileids import (
    CachingFileIdMapStore,
    FileIdMapStore,
    simple_apply_changes,
    )
from bzrlib.plugins.svn.layout.standard import WildcardLayout
from bzrlib.plugins.svn.layout.guess import repository_guess_layout
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
from bzrlib.plugins.svn.parents import DiskCachingParentsProvider
from bzrlib.plugins.svn.revids import (
    DiskCachingRevidMap,
    MemoryCachingRevidMap,
    RevidMap,
    RevisionInfoCache,
    )
from bzrlib.plugins.svn.tree import SvnRevisionTree
from bzrlib.plugins.svn.versionedfiles import SvnTexts

LAYOUT_SOURCE_GUESSED = 'guess'
LAYOUT_SOURCE_CONFIG = 'config'
LAYOUT_SOURCE_REGISTRY = 'registry'
LAYOUT_SOURCE_OVERRIDDEN = 'overridden'
LAYOUT_SOURCE_MAPPING_MANDATED = 'mapping-mandated'

class SvnRepositoryFormat(RepositoryFormat):
    """Repository format for Subversion repositories (accessed using svn_ra).
    """
    rich_root_data = True
    supports_tree_reference = False

    def __get_matchingbzrdir(self):
        from remote import SvnRemoteFormat
        return SvnRemoteFormat()

    _matchingbzrdir = property(__get_matchingbzrdir)

    def __init__(self):
        super(SvnRepositoryFormat, self).__init__()

    def get_format_description(self):
        return "Subversion Repository"

    def network_name(self):
        return "subversion"

    def initialize(self, url, shared=False, _internal=False):
        raise bzr_errors.UninitializableFormat(self)

    def check_conversion_target(self, target_repo_format):
        return target_repo_format.rich_root_data



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
        self.text_not_changed_cnt = 0
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
            note('%6d revisions originating in bzr', self.checked_roundtripped_cnt)
        if self.hidden_rev_cnt > 0:
            note('%6d hidden bzr-created revisions', self.hidden_rev_cnt)
        if self.inconsistent_stored_lhs_parent > 0:
            note('%6d inconsistent stored left-hand side parent revision ids', 
                self.inconsistent_stored_lhs_parent)
        if self.invalid_fileprop_cnt > 0:
            note('%6d invalid bzr-related file properties', 
                 self.invalid_fileprop_cnt)
        if self.text_not_changed_cnt > 0:
            note('%6d files were not changed but had their revision/fileid changed',
                 self.text_not_changed_cnt)
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
            fileprop_mappings = find_mappings_fileprops(revmeta.get_changed_fileprops())
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
            find_mapping_revprops(revmeta.get_revprops()) not in 
                (None, fileprop_mappings[0])):
            self.inconsistent_fileprop_revprop_cnt += 1
        self.checked_roundtripped_cnt += 1
        if revmeta.is_hidden(mapping):
            self.hidden_rev_cnt += 1

        mapping.check_fileprops(revmeta.get_changed_fileprops(), self)
        mapping.check_revprops(revmeta.get_revprops(), self)

        for parent_revid in revmeta.get_rhs_parents(mapping):
            if not self.repository.has_revision(parent_revid):
                self.ghost_revisions.add(parent_revid)

        # TODO: Check that stored revno matches actual revision number

        # Check all paths are under branch root
        branch_root = mapping.get_branch_root(revmeta.get_revprops())
        if branch_root is not None:
            for p in revmeta.get_paths():
                if not changes.path_is_child(branch_root, p):
                    self.paths_not_under_branch_root += 1

        original_uuid = mapping.get_repository_uuid(revmeta.get_revprops())
        if original_uuid is not None and original_uuid != self.repository.uuid:
            self.different_uuid_cnt += 1

        lhs_parent_revmeta = revmeta.get_lhs_parent_revmeta(mapping)
        if lhs_parent_revmeta is not None:
            lhs_parent_mapping = lhs_parent_revmeta.get_original_mapping()
            if lhs_parent_mapping is None:
                lhs_parent_mapping = mapping
            elif lhs_parent_mapping.newer_than(mapping):
                self.newer_mapping_parents += 1
                lhs_parent_mapping = mapping
            expected_lhs_parent_revid = lhs_parent_revmeta.get_revision_id(lhs_parent_mapping)
            if revmeta.get_stored_lhs_parent_revid(mapping) not in (None, expected_lhs_parent_revid):
                self.inconsistent_stored_lhs_parent += 1
        self.check_texts(revmeta, mapping)

    def check_texts(self, revmeta, mapping):
        # Check for inconsistencies in text file ids/revisions
        text_revisions = revmeta.get_text_revisions(mapping)
        text_parents = revmeta.get_text_parents(mapping)
        text_ids = revmeta.get_fileid_overrides(mapping)
        fileid_map = self.repository.get_fileid_map(revmeta, mapping)
        path_changes = revmeta.get_paths()
        for path in set(text_ids.keys() + text_revisions.keys() + text_parents.keys()):
            if (path in text_revisions and
                text_revisions[path] in text_parents.get(path, [])):
                self.text_revision_in_parents_cnt += 1
            full_path = urlutils.join(revmeta.branch_path, path)
            if not full_path in path_changes:
                mutter('in %s text %r/%r (%r) id changed but not changed', 
                       revmeta.get_revision_id(mapping),
                       text_ids.get(path), text_revisions.get(path),
                       text_parents.get(path))
                self.text_not_changed_cnt += 1
                continue
            # TODO Check consistency against parent data 
        for path, tps in text_parents.iteritems():
            if len(tps) > len(revmeta.get_parent_ids(mapping)):
                self.invalid_text_parents_len += 1
        ghost_parents = False
        parent_revmetas = []
        parent_mappings = []
        parent_fileid_maps = []
        for revid in revmeta.get_parent_ids(mapping):
            try:
                parent_revmeta, parent_mapping = self.repository._get_revmeta(revid)
            except bzr_errors.NoSuchRevision:
                ghost_parents = True
            else:
                parent_revmetas.append(parent_revmeta)
                parent_mappings.append(parent_mapping)
                parent_fileid_map = self.repository.get_fileid_map(parent_revmeta, parent_mapping)
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
                    not ghost_parents and not text_revision in parent_text_revisions):
                self.invalid_text_revisions += 1


class SvnRepository(ForeignRepository):
    """
    Provides a simplified interface to a Subversion repository 
    by using the RA (remote access) API from subversion
    """
    def __init__(self, bzrdir, transport, branch_path=None):
        from bzrlib.plugins.svn import lazy_register_optimizers
        lazy_register_optimizers()
        self.vcs = foreign_vcs_svn
        _revision_store = None

        assert isinstance(transport, Transport)

        control_files = LockableFiles(transport, '', TransportLock)
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
        self._config = None
        self._serializer = None
        self.get_config().add_location(self.base)
        self._log = logwalker.LogWalker(transport=transport)
        self.fileid_map = FileIdMapStore(simple_apply_changes, self)
        self.revmap = RevidMap(self)
        self._default_mapping = None
        self._hinted_branch_path = branch_path
        self._real_parents_provider = self
        self._cached_tags = {}

        use_cache = self.get_config().get_use_cache()

        if use_cache is None:
            # TODO: Don't enable log cache in some situations, e.g. 
            # for large repositories ?
            if self.base.startswith("file://"):
                # Default to no log caching for local connections
                use_cache = set(["fileids", "revinfo", "revids"])
            else:
                use_cache = set(["fileids", "revids", "revinfo", "log"])

        if use_cache:
            cache_obj = cache.cache_cls(self.uuid)

        if "log" in use_cache:
            self._log = logwalker.CachingLogWalker(self._log,
                cache_obj.open_logwalker())

        if "fileids" in use_cache:
            self.fileid_map = CachingFileIdMapStore(
                cache_obj.open_fileid_map(), self.fileid_map)

        if "revids" in use_cache:
            self.revmap = DiskCachingRevidMap(self.revmap, cache_obj.open_revid_map())
            self._real_parents_provider = DiskCachingParentsProvider(
                self._real_parents_provider, cache_obj.open_parents())
        else:
            self.revmap = MemoryCachingRevidMap(self.revmap)

        if "revinfo" in use_cache:
            self.revinfo_cache = cache_obj.open_revision_cache()
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
                use_cache,
                self.transport.has_capability("commit-revprops") in (True, None))

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
            self._lock_mode = None
            self._clear_cached_state()

    def _clear_cached_state(self, revnum=None):
        self._cached_tags = {}
        self._cached_revnum = revnum
        self._layout = None
        self._layout_source = None
        self._parents_provider = graph.CachingParentsProvider(self._real_parents_provider)

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

        for count, (revid, d) in enumerate(zip(revision_ids, self.get_deltas_for_revisions(self.get_revisions(revision_ids)))):
            if _files_pb is not None:
                _files_pb.update("fetch revisions for texts", count, len(revision_ids))
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
            return parse_svn_dateprop(self._log.revprop_list(revnum)[subvertpy.properties.PROP_REVISION_DATE])
        if committers is not None and revid is not None:
            all_committers = set()
            for rev in self.get_revisions(filter(lambda r: r is not None and r != NULL_REVISION, self.get_ancestry(revid))):
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

    def get_mapping_class(self):
        config_mapping_name = self.get_config().get_default_mapping()
        if config_mapping_name is not None:
            return mapping_registry.get(config_mapping_name)
        return mapping_registry.get_default()

    def _properties_to_set(self, mapping):
        """Determine what sort of custom properties to set when 
        committing a new round-tripped revision.
        
        :return: tuple with two booleans: whether to use revision properties 
            and whether to use file properties.
        """
        supports_custom_revprops = self.transport.has_capability("commit-revprops")
        if supports_custom_revprops and mapping.can_use_revprops:
            return (True, mapping.must_use_fileprops)
        else:
            return (False, mapping.can_use_fileprops)

    def get_mapping(self):
        """Get the default mapping that is used for this repository."""
        if self._default_mapping is None:
            mappingcls = self.get_mapping_class()
            self._default_mapping = mappingcls.from_repository(self, self._hinted_branch_path)
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
        parentrevmeta = revision.svn_meta.get_lhs_parent_revmeta(revision.mapping)
        from bzrlib.plugins.svn.fetch import TreeDeltaBuildEditor
        if parentrevmeta is None:
            parentfileidmap = {}
            parent_branch_path = revision.svn_meta.branch_path
            parentrevnum = revision.svn_meta.revnum
            start_empty = True
        else:
            parentfileidmap = self.get_fileid_map(parentrevmeta, revision.mapping)
            parent_branch_path = parentrevmeta.branch_path
            parentrevnum = parentrevmeta.revnum
            start_empty = False
        editor = TreeDeltaBuildEditor(revision.svn_meta, revision.mapping, 
                                      self.get_fileid_map(revision.svn_meta, revision.mapping), 
                                      parentfileidmap)
        conn = self.transport.get_connection(parent_branch_path)
        try:
            reporter = conn.do_diff(revision.svn_meta.revnum, "", urlutils.join(self.transport.get_svn_repos_root(), revision.svn_meta.branch_path).rstrip("/"), editor, True, True, False)
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
        # TODO: Retrieve guessed-layout from config and see if it accepts self._hinted_branch_path
        layoutname = self.get_config().get_guessed_layout()
        if layoutname is not None:
            config_guessed_layout = layout.layout_registry.get(layoutname)()
            if self._hinted_branch_path is None or config_guessed_layout.is_branch(self._hinted_branch_path):
                self._guessed_layout = config_guessed_layout
                self._guessed_appropriate_layout = config_guessed_layout
                return
        else:
            config_guessed_layout = None
        revnum = self.get_latest_revnum()
        (self._guessed_layout, self._guessed_appropriate_layout) = repository_guess_layout(self, 
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

    def _warn_if_deprecated(self):
        # This class isn't deprecated
        pass

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.base)

    def _check(self, revision_ids):
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
                for revmeta in self._revmeta_provider.iter_all_revisions(self.get_layout(), self.get_mapping().is_branch_or_tag, last_revnum):
                    pb.update("checking revisions", last_revnum-revmeta.revnum, last_revnum)
                    ret.check_revmeta(revmeta)
        finally:
            pb.finished()
        return ret

    def get_inventory(self, revision_id):
        """See Repository.get_inventory()."""
        assert revision_id != None
        return self.revision_tree(revision_id).inventory

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
        for revmeta in self._revmeta_provider.iter_all_revisions(layout, mapping.is_branch_or_tag, self.get_latest_revnum()):
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

    def _iter_reverse_revmeta_mapping_ancestry(self, branch_path, revnum, mapping, lhs_history=None, pb=None):
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
            update_todo(todo, self._iter_reverse_revmeta_mapping_history(branch_path, revnum, to_revnum=0, mapping=mapping, pb=bp))
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
                    (_, rhs_parent_bp, rhs_parent_revnum), rhs_parent_mapping = self.lookup_revision_id(rhs_parent_revid)
                except bzr_errors.NoSuchRevision:
                    pass
                else:
                    update_todo(todo, self._iter_reverse_revmeta_mapping_history(rhs_parent_bp, rhs_parent_revnum, to_revnum=0, mapping=mapping, pb=pb))

    def _iter_reverse_revmeta_mapping_history(self, branch_path, revnum, 
        to_revnum, mapping, pb=None, limit=0): 
        assert mapping is not None
        expected_revid = None
        iter = self._revmeta_provider.iter_reverse_branch_changes(branch_path, 
            revnum, to_revnum=to_revnum, pb=pb, limit=limit)
        for revmeta in iter:
            (mapping, lhs_mapping) = revmeta.get_appropriate_mappings(mapping)
            assert not lhs_mapping.newer_than(mapping), "LHS mapping %r newer than %r" % (lhs_mapping, mapping)
            revid = revmeta.get_revision_id(mapping)
            if (expected_revid is not None and
                not revid in (None, expected_revid)):
                # Need to restart, branch root has changed
                (_, branch_path, revnum), mapping = self.lookup_revision_id(revid)
                iter = self._revmeta_provider.iter_reverse_branch_changes(branch_path, revnum, to_revnum=to_revnum, pb=pb, limit=limit)
            if not mapping.is_branch_or_tag(revmeta.branch_path):
                return
            yield revmeta, mapping
            expected_revid = revmeta._get_stored_lhs_parent_revid(mapping)
            mapping = lhs_mapping
        if expected_revid is not None and expected_revid != NULL_REVISION:
            # Need to restart, branch root has changed
            (_, branch_path, revnum), mapping = self.lookup_revision_id(expected_revid)
            for (revmeta, mapping) in self._iter_reverse_revmeta_mapping_history(branch_path, revnum, to_revnum=to_revnum, mapping=mapping, pb=pb, limit=limit):
                yield (revmeta, mapping)

    def iter_reverse_revision_history(self, revision_id, pb=None, limit=0):
        """Iterate backwards through revision ids in the lefthand history

        :param revision_id: The revision id to start with.  All its lefthand
            ancestors will be traversed.
        """
        if revision_id in (None, NULL_REVISION):
            return
        (uuid, branch_path, revnum), mapping = self.lookup_revision_id(revision_id)
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

    def has_revisions(self, revision_ids):
        ret = set()
        for revision_id in revision_ids:
            if self.has_revision(revision_id):
                ret.add(revision_id)
        return ret

    def has_revision(self, revision_id, project=None):
        """See Repository.has_revision()."""
        if revision_id is None:
            return True

        try:
            foreign_revid, _ = self.lookup_revision_id(revision_id, project=project)
        except bzr_errors.NoSuchRevision:
            return False

        return self.has_foreign_revision(foreign_revid)

    def has_foreign_revision(self, (uuid, path, revnum)):
        try:
            return (subvertpy.NODE_DIR == self.transport.check_path(path, revnum))
        except subvertpy.SubversionException, (_, num):
            if num == subvertpy.ERR_FS_NO_SUCH_REVISION:
                return False
            raise

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
            return RevisionTree(self, inventory, revision_id)

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
        (uuid, branch, revnum), mapping = self.lookup_revision_id(revision_id)
        revmeta = self._revmeta_provider.lookup_revision(branch, revnum)
        return revmeta, mapping

    def get_revision(self, revision_id):
        """See Repository.get_revision."""
        if not revision_id or not isinstance(revision_id, str):
            raise bzr_errors.InvalidRevisionId(revision_id=revision_id, branch=self)

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
            return self.lookup_foreign_revision_id(foreign_revid, newest_allowed)
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
        return self.lookup_foreign_revision_id((self.uuid, path, revnum), mapping)

    def lookup_revision_id(self, revid, layout=None, ancestry=None, 
                           project=None):
        """Parse an existing Subversion-based revision id.

        :param revid: The revision id.
        :param layout: Optional repository layout to use when searching for 
                       revisions
        :raises: NoSuchRevision
        :return: Tuple with foreign revision id and mapping.
        """
        # FIXME: Use ancestry
        # If there is no entry in the map, walk over all branches:
        if layout is None:
            layout = self.get_layout()

        # Try a simple parse
        try:
            (uuid, branch_path, revnum), mapping = mapping_registry.parse_revision_id(revid)
            assert isinstance(branch_path, str)
            assert isinstance(mapping, BzrSvnMapping)
            if uuid == self.uuid:
                return (self.uuid, branch_path, revnum), mapping
            # If the UUID doesn't match, this may still be a valid revision
            # id; a revision from another SVN repository may be pushed into 
            # this one.
        except bzr_errors.InvalidRevisionId:
            pass

        return self.revmap.get_branch_revnum(revid, layout, project)

    def seen_bzr_revprops(self):
        """Check whether bzr-specific custom revision properties are present on this 
        repository.

        """
        if self.transport.has_capability("commit-revprops") == False:
            return False
        for revmeta in self._revmeta_provider.iter_all_revisions(self.get_layout(), None, self.get_latest_revnum()):
            if is_bzr_revision_revprops(revmeta.get_revprops()):
                return True
        return False

    def get_config(self):
        if self._config is None:
            self._config = SvnRepositoryConfig(self.base, self.uuid)
        return self._config

    def has_signature_for_revision_id(self, revision_id):
        """Check whether a signature exists for a particular revision id.

        :param revision_id: Revision id for which the signatures should be looked up.
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
        (uuid, path, revnum), mapping = self.lookup_revision_id(revision_id)
        try:
            self.transport.change_rev_prop(revnum, SVN_REVPROP_BZR_SIGNATURE, signature)
        except subvertpy.SubversionException, (_, subvertpy.ERR_REPOS_DISABLED_FEATURE):
            raise errors.RevpropChangeFailed(SVN_REVPROP_BZR_SIGNATURE)

    @needs_read_lock
    def find_branches(self, layout=None, revnum=None):
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
            for project, bp, nick in layout.get_branches(self, revnum, pb=pb):
                branches.append(SvnBranch(self, bp, _skip_check=True))
        finally:
            pb.finished()
        return branches

    @needs_read_lock
    def find_tags_between(self, project, layout, mapping, from_revnum, to_revnum, tags=None):
        if tags is None:
            tags = {}
        assert from_revnum <= to_revnum
        pb = ui.ui_factory.nested_progress_bar()
        try:
            entries = []
            for kind, item in self._revmeta_provider.iter_all_changes(layout, mapping.is_branch_or_tag, to_revnum, from_revnum, project=project):
                if kind == "revision":
                    pb.update("discovering tags", to_revnum-item.revnum, to_revnum-from_revnum)
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
                        if changes.path_is_child(t, path) and revnum > lastrevnum:
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
                    layout=layout, mapping=mapping, from_revnum=0, to_revnum=revnum)
        return self._cached_tags[layout]

    def find_branchpaths(self, layout,
                         from_revnum=0, to_revnum=None, 
                         project=None):
        """Find all branch paths that were changed in the specified revision 
        range.

        :param revnum: Revision to search for branches.
        :return: iterator that returns tuples with (path, revision number, 
            still exists). The revision number is the revision in which the 
            branch last existed.
        """
        if to_revnum is None:
            to_revnum = self.get_latest_revnum()

        created_branches = {}

        ret = []

        if project is not None:
            prefixes = layout.get_project_prefixes(project)
        else:
            prefixes = [""]

        pb = ui.ui_factory.nested_progress_bar()
        try:
            for (paths, i, revprops) in self._log.iter_changes(prefixes, from_revnum, to_revnum):
                if (isinstance(revprops, dict) or revprops.is_loaded) and is_bzr_revision_revprops(revprops):
                    continue
                pb.update("finding branches", i, to_revnum)
                for p in sorted(paths.keys()):
                    if layout.is_branch_or_tag(p, project):
                        if paths[p][0] in ('R', 'D') and p in created_branches:
                            ret.append((p, created_branches[p], False))
                            del created_branches[p]

                        if paths[p][0] in ('A', 'R', 'M'): 
                            created_branches[p] = i
                    elif layout.is_branch_or_tag_parent(p, project):
                        if paths[p][0] in ('R', 'D'):
                            k = created_branches.keys()
                            for c in k:
                                if c.startswith(p+"/") and c in created_branches:
                                    ret.append((c, created_branches[c], False))
                                    del created_branches[c] 
                        if paths[p][0] in ('A', 'R') and paths[p][1] is not None:
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
                                    if num == subvertpy.ERR_FS_NOT_DIRECTORY:
                                        pass
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
                           revision_id=None):
        """See Repository.get_commit_builder()."""
        from bzrlib.plugins.svn.commit import SvnCommitBuilder
        return SvnCommitBuilder(self, branch.get_branch_path(), parents, 
                                config, timestamp, timezone, committer, 
                                revprops, revision_id, 
                                append_revisions_only=True)

    def find_fileprop_paths(self, layout, from_revnum, to_revnum, 
                               project=None, check_removed=False):
        if not check_removed and from_revnum == 0:
            it = iter([])
            it = chain(it, layout.get_branches(self, to_revnum, project))
            it = chain(it, layout.get_tags(self, to_revnum, project))
            return iter(((branch, to_revnum, True) for (project, branch, nick) in it))
        else:
            return iter(self.find_branchpaths(layout, from_revnum, to_revnum, project))

    def find_children(self, path, revnum, pb=None):
        """Find all children of path in revnum.

        :param path:  Path to check
        :param revnum:  Revision to check
        """
        assert isinstance(path, str), "invalid path"
        path = path.strip("/")
        conn = self.transport.connections.get(self.transport.get_svn_repos_root())
        results = []
        unchecked_dirs = set([path])
        num_checked = 0
        try:
            while len(unchecked_dirs) > 0:
                if pb is not None:
                    pb.update("listing branch contents", num_checked, 
                        num_checked+len(unchecked_dirs))
                nextp = unchecked_dirs.pop()
                num_checked += 1
                try:
                    dirents = conn.get_dir(nextp, revnum, subvertpy.ra.DIRENT_KIND)[0]
                except subvertpy.SubversionException, (_, num):
                    if num == subvertpy.ERR_FS_NOT_DIRECTORY:
                        continue
                    raise
                for k, v in dirents.iteritems():
                    childp = urlutils.join(nextp, k)
                    if v['kind'] == subvertpy.NODE_DIR:
                        unchecked_dirs.add(childp)
                    results.append(childp)
        finally:
            self.transport.connections.add(conn)
        return results

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
        self._write_group = None 
