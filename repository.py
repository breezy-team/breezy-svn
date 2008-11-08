# Copyright (C) 2006-2008 Jelmer Vernooij <jelmer@samba.org>

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
"""Subversion repository access."""

from bzrlib import osutils, ui, urlutils, xml6
from bzrlib.branch import BranchCheckResult
from bzrlib.errors import (InvalidRevisionId, NoSuchRevision, NotBranchError, 
                           UninitializableFormat, NotWriteLocked)
from bzrlib.graph import CachingParentsProvider
from bzrlib.inventory import Inventory
from bzrlib.lockable_files import LockableFiles, TransportLock
from bzrlib.repository import Repository, RepositoryFormat, needs_read_lock
from bzrlib.revisiontree import RevisionTree
from bzrlib.revision import Revision, NULL_REVISION, ensure_null
from bzrlib.transport import Transport, get_transport
from bzrlib.trace import info

from copy import copy
from itertools import chain
import os

from bzrlib.plugins.svn import cache, changes, errors, layout, logwalker, revmeta
import subvertpy
from bzrlib.plugins.svn.branchprops import PathPropertyProvider
from bzrlib.plugins.svn.config import SvnRepositoryConfig
from subvertpy import SubversionException, properties
from bzrlib.plugins.svn.layout.standard import WildcardLayout
from bzrlib.plugins.svn.layout.guess import repository_guess_layout
from bzrlib.plugins.svn.mapping import (SVN_REVPROP_BZR_SIGNATURE,
                     SVN_REVPROP_BZR_TAGS,
                     parse_tags_property,
                     BzrSvnMapping,
                     mapping_registry,
                     is_bzr_revision_revprops, is_bzr_revision_fileprops,
                     parse_svn_dateprop)
from bzrlib.plugins.svn.parents import DiskCachingParentsProvider
from bzrlib.plugins.svn.revids import CachingRevidMap, RevidMap
from bzrlib.plugins.svn.tree import SvnRevisionTree
from bzrlib.plugins.svn.versionedfiles import (SvnTexts, VirtualRevisionTexts, 
                                               VirtualInventoryTexts, VirtualSignatureTexts)

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

    def initialize(self, url, shared=False, _internal=False):
        raise UninitializableFormat(self)

    def check_conversion_target(self, target_repo_format):
        return target_repo_format.rich_root_data


CACHE_DB_VERSION = 4

cachedbs = {}

class SvnRepository(Repository):
    """
    Provides a simplified interface to a Subversion repository 
    by using the RA (remote access) API from subversion
    """
    def __init__(self, bzrdir, transport, branch_path=None):
        from bzrlib.plugins.svn import lazy_register_optimizers
        lazy_register_optimizers()
        from bzrlib.plugins.svn.fileids import (CachingFileIdMap, 
                                                simple_apply_changes, FileIdMap)
        _revision_store = None

        assert isinstance(transport, Transport)

        control_files = LockableFiles(transport, '', TransportLock)
        Repository.__init__(self, SvnRepositoryFormat(), bzrdir, control_files)

        self._cached_revnum = None
        self._lock_mode = None
        self._lock_count = 0
        self._layout = None
        self._guessed_layout = None
        self.transport = transport
        self.uuid = transport.get_uuid()
        assert self.uuid is not None
        self.base = transport.base
        assert self.base is not None
        self._config = None
        self._serializer = xml6.serializer_v6
        self.get_config().add_location(self.base)
        self._log = logwalker.LogWalker(transport=transport)
        self.fileid_map = FileIdMap(simple_apply_changes, self)
        self.revmap = RevidMap(self)
        self._default_mapping = None
        self._hinted_branch_path = branch_path
        self._real_parents_provider = self
        self._cached_tags = {}

        use_cache = self.get_config().get_use_cache()

        if use_cache:
            cache_dir = self.create_cache_dir()
            cache_file = os.path.join(cache_dir, 'cache-v%d' % CACHE_DB_VERSION)
            if not cachedbs.has_key(cache_file):
                cachedbs[cache_file] = cache.connect_cachefile(cache_file)
            self.cachedb = cachedbs[cache_file]
            self._log = logwalker.CachingLogWalker(self._log, cache_db=self.cachedb)
            cachedir_transport = get_transport(cache_dir)
            self.fileid_map = CachingFileIdMap(cachedir_transport, self.fileid_map)
            self.revmap = CachingRevidMap(self.revmap, self.cachedb)
            self._real_parents_provider = DiskCachingParentsProvider(self._real_parents_provider, cachedir_transport)

        self._parents_provider = CachingParentsProvider(self._real_parents_provider)
        self.texts = SvnTexts(self)
        self.revisions = VirtualRevisionTexts(self)
        self.inventories = VirtualInventoryTexts(self)
        self.signatures = VirtualSignatureTexts(self)

        self.branchprop_list = PathPropertyProvider(self._log)

        self._revmeta_provider = revmeta.RevisionMetadataProvider(self, 
                use_cache,
                self.transport.has_capability("commit-revprops") in (True, None))

    def get_revmap(self):
        return self.revmap
   
    def get_transaction(self):
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
        self._parents_provider = CachingParentsProvider(self._real_parents_provider)

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

    def get_latest_revnum(self):
        if self._lock_mode in ('r','w') and self._cached_revnum is not None:
            return self._cached_revnum
        self._cached_revnum = self.transport.get_latest_revnum()
        return self._cached_revnum

    def item_keys_introduced_by(self, revision_ids, _files_pb=None):
        fileids = {}

        for count, (revid, d) in enumerate(zip(revision_ids, self.get_deltas_for_revisions(self.get_revisions(revision_ids)))):
            if _files_pb is not None:
                _files_pb.update("fetch revisions for texts", count, len(revision_ids))
            for c in d.added + d.modified:
                fileids.setdefault(c[1], set()).add(revid)
            for c in d.renamed:
                fileids.setdefault(c[2], set()).add(revid)

        for fileid, altered_versions in fileids.items():
            yield ("file", fileid, altered_versions)
        
        # We're done with the files_pb.  Note that it finished by the caller,
        # just as it was created by the caller.
        del _files_pb

        yield ("inventory", None, revision_ids)

        # signatures
        revisions_with_signatures = set()
        for rev_id in revision_ids:
            try:
                self.get_signature_text(rev_id)
            except NoSuchRevision:
                # not signed.
                pass
            else:
                revisions_with_signatures.add(rev_id)
        yield ("signatures", None, revisions_with_signatures)
        yield ("revisions", None, revision_ids)

    @needs_read_lock
    def gather_stats(self, revid=None, committers=None):
        result = {}
        def revdate(revnum):
            return parse_svn_dateprop(self._log.revprop_list(revnum)[properties.PROP_REVISION_DATE])
        if committers is not None and revid is not None:
            all_committers = set()
            for rev in self.get_revisions(filter(lambda r: r is not None and r != NULL_REVISION, self.get_ancestry(revid))):
                if rev.committer != '':
                    all_committers.add(rev.committer)
            result['committers'] = len(all_committers)
        result['firstrev'] = revdate(0)
        result['latestrev'] = revdate(self.get_latest_revnum())
        result['uuid'] = self.uuid
        # Approximate number of revisions
        result['revisions'] = self.get_latest_revnum()+1
        return result

    def get_mapping_class(self):
        config_mapping_name = self.get_config().get_default_mapping()
        if config_mapping_name is not None:
            return mapping_registry.get(config_mapping_name)
        return mapping_registry.get_default()

    def get_mapping(self):
        if self._default_mapping is None:
            mappingcls = self.get_mapping_class()
            self._default_mapping = mappingcls.from_repository(self, self._hinted_branch_path)
        return self._default_mapping

    def _make_parents_provider(self):
        return self._parents_provider

    def get_deltas_for_revisions(self, revisions):
        for revision in revisions:
            yield self.get_revision_delta(revision)

    def get_revision_delta(self, revision):
        parentrevmeta = revision.svn_meta.get_lhs_parent_revmeta(revision.svn_mapping)
        from bzrlib.plugins.svn.fetch import TreeDeltaBuildEditor
        if parentrevmeta is None:
            parentfileidmap = {}
            parent_branch_path = revision.svn_meta.branch_path
            parentrevnum = revision.svn_meta.revnum
            start_empty = True
        else:
            parentfileidmap = self.get_fileid_map(parentrevmeta, revision.svn_mapping)
            parent_branch_path = parentrevmeta.branch_path
            parentrevnum = parentrevmeta.revnum
            start_empty = False
        editor = TreeDeltaBuildEditor(revision.svn_meta, revision.svn_mapping, 
                                      self.get_fileid_map(revision.svn_meta, revision.svn_mapping), 
                                      parentfileidmap)
        conn = self.transport.get_connection(parent_branch_path)
        try:
            reporter = conn.do_diff(revision.svn_meta.revnum, "", urlutils.join(self.transport.get_svn_repos_root(), revision.svn_meta.branch_path), editor, True, True, False)
            reporter.set_path("", parentrevnum, start_empty)
            reporter.finish()
        finally:
            self.transport.add_connection(conn)
        return editor.delta

    def set_layout(self, layout):
        self.get_mapping().check_layout(self, layout)
        self._layout = layout

    def get_layout(self):
        if self._layout is None:
            self._layout = self.get_mapping().get_mandated_layout(self)
        if self._layout is None:
            layoutname = self.get_config().get_layout()
            if layoutname is not None:
                self._layout = mapping_registry.get(layoutname)()
        if self._layout is None:
            branches = self.get_config().get_branches()
            tags = self.get_config().get_tags()
            if branches is not None:
                self._layout = WildcardLayout(branches, tags or [])
        if self._layout is None:
            self._layout = layout.repository_registry.get(self.uuid)
        if self._layout is None:
            (self._guessed_layout, self._layout) = repository_guess_layout(self, 
                    self.get_latest_revnum(), self._hinted_branch_path)
        return self._layout

    def get_guessed_layout(self):
        if self._guessed_layout is None:
            self._guessed_layout = self.get_mapping().get_guessed_layout(self)
        if self._guessed_layout is None:
            (self._guessed_layout, self._layout) = repository_guess_layout(self, 
                    self.get_latest_revnum(), self._hinted_branch_path)
        return self._guessed_layout

    def _warn_if_deprecated(self):
        # This class isn't deprecated
        pass

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.base)

    def create_cache_dir(self):
        cache_dir = cache.create_cache_dir()
        dir = os.path.join(cache_dir, self.uuid)
        if not os.path.exists(dir):
            info("Initialising Subversion metadata cache in %s" % dir)
            os.mkdir(dir)
        return dir

    def _check(self, revision_ids):
        return BranchCheckResult(self)

    def get_inventory(self, revision_id):
        assert revision_id != None
        return self.revision_tree(revision_id).inventory

    def get_fileid_map(self, revmeta, mapping):
        return self.fileid_map.get_map(revmeta.uuid, revmeta.revnum, revmeta.branch_path, mapping)

    def transform_fileid_map(self, revmeta, mapping):
        return self.fileid_map.apply_changes(revmeta, mapping)[0]

    def all_revision_ids(self, layout=None, mapping=None):
        if mapping is None:
            mapping = self.get_mapping()
        if layout is None:
            layout = self.get_layout()
        for revmeta in self._revmeta_provider.iter_all_changes(layout, mapping, self.get_latest_revnum()):
            if revmeta.is_hidden(mapping):
                continue
            yield revmeta.get_revision_id(mapping)

    def set_make_working_trees(self, new_value):
        """See Repository.set_make_working_trees()."""
        pass # FIXME: ignored, nowhere to store it... 

    def make_working_trees(self):
        """See Repository.make_working_trees().

        Always returns False, as working trees are never created inside 
        Subversion repositories.
        """
        return False

    def iter_reverse_revision_history(self, revision_id, pb=None, limit=0):
        """Iterate backwards through revision ids in the lefthand history

        :param revision_id: The revision id to start with.  All its lefthand
            ancestors will be traversed.
        """
        if revision_id in (None, NULL_REVISION):
            return
        (branch_path, revnum, mapping) = self.lookup_revision_id(revision_id)
        for revmeta in self._revmeta_provider.iter_reverse_branch_changes(branch_path, revnum, to_revnum=0, 
                                                        mapping=mapping, pb=pb, 
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

    def has_revision(self, revision_id, project=None):
        """See Repository.has_revision()."""
        if revision_id is None:
            return True

        try:
            (path, revnum, _) = self.lookup_revision_id(revision_id, project=project)
        except NoSuchRevision:
            return False

        try:
            return (subvertpy.NODE_DIR == self.transport.check_path(path, revnum))
        except SubversionException, (_, num):
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
        parent_map = {}
        for revision_id in revids:
            if revision_id == NULL_REVISION:
                parent_map[revision_id] = ()
                continue

            try:
                revmeta, mapping = self._get_revmeta(ensure_null(revision_id))
            except NoSuchRevision:
                continue
            else:
                parent_map[revision_id] = revmeta.get_parent_ids(mapping)

        return parent_map

    def _get_revmeta(self, revision_id):
        (branch, revnum, mapping) = self.lookup_revision_id(revision_id)
        revmeta = self._revmeta_provider.get_revision(branch, revnum)
        return revmeta, mapping

    def get_revision(self, revision_id):
        """See Repository.get_revision."""
        if not revision_id or not isinstance(revision_id, str):
            raise InvalidRevisionId(revision_id=revision_id, branch=self)

        revmeta, mapping = self._get_revmeta(revision_id)
        
        return revmeta.get_revision(mapping)

    def get_revisions(self, revision_ids):
        """See Repository.get_revisions()."""
        # TODO: More efficient implementation?
        return map(self.get_revision, revision_ids)

    def add_revision(self, rev_id, rev, inv=None, config=None):
        raise NotImplementedError(self.add_revision)

    def generate_revision_id(self, revnum, path, mapping):
        """Generate an unambiguous revision id. 
        
        :param revnum: Subversion revision number.
        :param path: Branch path.
        :param mapping: Mapping to use.

        :return: New revision id.
        """
        assert isinstance(path, str)
        assert isinstance(revnum, int)
        assert isinstance(mapping, BzrSvnMapping)

        return self._revmeta_provider.get_revision(path, revnum).get_revision_id(mapping)

    def lookup_revision_id(self, revid, layout=None, ancestry=None, 
                           project=None):
        """Parse an existing Subversion-based revision id.

        :param revid: The revision id.
        :param layout: Optional repository layout to use when searching for 
                       revisions
        :raises: NoSuchRevision
        :return: Tuple with branch path, revision number and mapping.
        """
        # FIXME: Use ancestry
        # If there is no entry in the map, walk over all branches:
        if layout is None:
            layout = self.get_layout()
        return self.get_revmap().get_branch_revnum(revid, layout, project)

    def get_inventory_xml(self, revision_id):
        """See Repository.get_inventory_xml()."""
        return self.serialise_inventory(self.get_inventory(revision_id))

    def get_inventory_sha1(self, revision_id):
        """Get the sha1 for the XML representation of an inventory.

        :param revision_id: Revision id of the inventory for which to return 
         the SHA1.
        :return: XML string
        """

        return osutils.sha_string(self.get_inventory_xml(revision_id))

    def get_revision_xml(self, revision_id):
        """Return the XML representation of a revision.

        :param revision_id: Revision for which to return the XML.
        :return: XML string
        """
        return self._serializer.write_revision_to_string(self.get_revision(revision_id))

    def seen_bzr_revprops(self):
        """Check whether bzr-specific custom revision properties are present on this 
        repository.

        """
        if self.transport.has_capability("commit-revprops") == False:
            return False
        for revmeta in self._revmeta_provider.iter_all_changes(self.get_layout(), None, self.get_latest_revnum()):
            if revmeta.is_bzr_revision_revprops():
                return True
        return False

    def get_config(self):
        if self._config is None:
            self._config = SvnRepositoryConfig(self.uuid)
        return self._config

    def has_signature_for_revision_id(self, revision_id):
        """Check whether a signature exists for a particular revision id.

        :param revision_id: Revision id for which the signatures should be looked up.
        :return: False, as no signatures are stored for revisions in Subversion 
            at the moment.
        """
        try:
            revmeta, mapping = self._get_revmeta(revision_id)
        except NoSuchRevision:
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
            raise NoSuchRevision(self, revision_id)
        return signature

    def add_signature_text(self, revision_id, signature):
        (path, revnum, mapping) = self.lookup_revision_id(revision_id)
        try:
            self.transport.change_rev_prop(revnum, SVN_REVPROP_BZR_SIGNATURE, signature)
        except SubversionException, (_, subvertpy.ERR_REPOS_DISABLED_FEATURE):
            raise errors.RevpropChangeFailed(SVN_REVPROP_BZR_SIGNATURE)

    @needs_read_lock
    def find_deleted_branches_between(self, layout, from_revnum, to_revnum, project=None):
        deleted = set()
        if project is not None:
            prefixes = layout.get_project_prefixes(project)
        else:
            prefixes = None
        pb = ui.ui_factory.nested_progress_bar()
        try:
            for (paths, revnum, revprops) in self._log.iter_changes(prefixes, from_revnum, to_revnum):
                pb.update("finding branches", revnum, to_revnum)
                for p in paths:
                    if ((layout.is_branch_parent(p, project) or layout.is_branch(p, project)) and 
                            paths[p][0] in ('R', 'D')):
                        deleted.add(p)
        finally:
            pb.finished()
        return deleted

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
        if project is None:
            prefixes = [""]
        else:
            prefixes = layout.get_project_prefixes(project)

        try:
            entries = list(self._log.iter_changes(prefixes, to_revnum, from_revnum, pb=pb))
            for (paths, revnum, revprops) in reversed(entries):
                if revprops is None:
                    continue
                if (self.transport.has_capability("log-revprops") and 
                    SVN_REVPROP_BZR_TAGS in revprops):
                    for name, revid in parse_tags_property(revprops[SVN_REVPROP_BZR_TAGS]):
                        if revid is None:
                            del tags[name]
                        else:
                            tags[name] = revid
                    continue
                tag_changes = {}
                for p in sorted(paths):
                    (action, cf, cr) = paths[p]
                    if layout.is_tag_parent(p, project) and cf is not None:
                        parents = [p]
                        while parents:
                            p = parents.pop()
                            try:
                                for c in self.transport.get_dir(p, revnum)[0].keys():
                                    n = p+"/"+c
                                    if layout.is_tag(n, project):
                                        tag_changes[n] = self._revmeta_provider.get_revision(n, revnum, revprops=revprops).generate_revision_id(mapping)
                                    elif layout.is_tag_parent(n, project):
                                        parents.append(n)
                            except SubversionException, (_, subvertpy.ERR_FS_NOT_DIRECTORY):
                                pass
                    else:
                        try:
                            (pt, bp, rp) = layout.split_project_path(p, project)
                        except errors.NotSvnBranchPath:
                            continue
                        if pt != "tag":
                            continue
                        if action == "D" and rp == "":
                            tag_changes[p] = None
                        elif rp == "" and cf is not None:
                            # This tag was (recreated) here, so unless anything else under this 
                            # tag changed
                            tp = p
                            tr = revnum
                            newpaths = copy(paths)
                            del newpaths[p]
                            if not changes.changes_path(newpaths, p, False) and layout.is_branch(cf):
                                tp = cf
                                tr = self._log.find_latest_change(cf, cr)
                            try:
                                tag_changes[p] = self.generate_revision_id(tr, tp, mapping)
                            except SubversionException, (_, errors.ERR_FS_NOT_DIRECTORY):
                                pass
                        else:
                            try:
                                tag_changes[bp] = self._revmeta_provider.get_revision(bp, revnum, revprops=revprops).get_revision_id(mapping)
                            except SubversionException, (_, errors.ERR_FS_NOT_DIRECTORY):
                                pass
                for path, revid in tag_changes.items():
                    name = layout.get_tag_name(path, project)
                    if revid is None:
                        del tags[name]
                    else:
                        tags[name] = revid
        finally:
            pb.finished()

        return tags

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

        if not (layout, mapping) in self._cached_tags:
            self._cached_tags[layout,mapping] = self.find_tags_between(project=project,
                    layout=layout, mapping=mapping, from_revnum=0, to_revnum=revnum)
        return self._cached_tags[layout,mapping]

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
                pb.update("finding branches", i, to_revnum)
                if self.transport.has_capability("log-revprops") and is_bzr_revision_revprops(revprops) is not None:
                    continue
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
                                except SubversionException, (_, subvertpy.ERR_FS_NOT_DIRECTORY):
                                    pass
        finally:
            pb.finished()

        for p, i in created_branches.items():
            ret.append((p, i, True))

        return ret

    def is_shared(self):
        """Return True if this repository is flagged as a shared repository."""
        return True

    def get_physical_lock_status(self):
        return False

    def get_commit_builder(self, branch, parents, config, timestamp=None, 
                           timezone=None, committer=None, revprops=None, 
                           revision_id=None):
        from bzrlib.plugins.svn.commit import SvnCommitBuilder
        return SvnCommitBuilder(self, branch.get_branch_path(), parents, config, timestamp, 
                timezone, committer, revprops, revision_id, 
                append_revisions_only=True)

    def find_fileprop_paths(self, layout, from_revnum, to_revnum, 
                               project=None, check_removed=False):
        if not check_removed and from_revnum == 0:
            it = iter([])
            it = chain(it, layout.get_branches(self, to_revnum, project))
            it = chain(it, layout.get_tags(self, to_revnum, project))
            for (project, branch, nick) in it:
                yield (branch, to_revnum, True)
        else:
            for (branch, revno, exists) in self.find_branchpaths(
                    layout, from_revnum, to_revnum, project):
                yield (branch, revno, exists)

    def abort_write_group(self):
        self._write_group = None

    def commit_write_group(self):
        self._write_group = None

    def start_write_group(self):
        if not self.is_write_locked():
            raise NotWriteLocked(self)
        self._write_group = None 
