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
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
"""Checkouts and working trees (working copies)."""

try:
    from collections import defaultdict
except ImportError:
    from bzrlib.plugins.svn.pycompat import defaultdict

import os
import subvertpy
import subvertpy.wc
from subvertpy import properties
from subvertpy.wc import *
import urllib

import bzrlib, bzrlib.add
from bzrlib import (
    osutils,
    urlutils,
    )
from bzrlib.branch import (
    BranchReferenceFormat,
    )
from bzrlib.bzrdir import (
    BzrDirFormat,
    BzrDir,
    Converter,
    )
from bzrlib.errors import (
    NotBranchError,
    NoSuchFile,
    NoSuchRevision,
    NoRepositoryPresent,
    NoWorkingTree,
    UnsupportedFormatError,
    UninitializableFormat,
    )
from bzrlib.inventory import (
    Inventory,
    InventoryFile,
    InventoryLink,
    )
from bzrlib.lockable_files import LockableFiles
from bzrlib.lockdir import LockDir
from bzrlib.revision import NULL_REVISION
from bzrlib.trace import mutter
from bzrlib.transport import get_transport
from bzrlib.workingtree import (
    WorkingTree,
    WorkingTreeFormat,
    )

from bzrlib.plugins.svn import (
    errors as bzrsvn_errors,
    svk,
    )
from bzrlib.plugins.svn.branch import (
    SvnBranch,
    )
from bzrlib.plugins.svn.commit import (
    _revision_id_to_svk_feature,
    )
from bzrlib.plugins.svn.errors import (
    convert_svn_error,
    )
from bzrlib.plugins.svn.fileids import (
    idmap_lookup,
    )
from bzrlib.plugins.svn.format import (
    get_rich_root_format,
    )
from bzrlib.plugins.svn.mapping import (
    escape_svn_path,
    )
from bzrlib.plugins.svn.remote import (
    SvnRemoteAccess,
    )
from bzrlib.plugins.svn.repository import (
    SvnRepository,
    )
from bzrlib.plugins.svn.transport import (
    SvnRaTransport,
    svn_config,
    )
from bzrlib.plugins.svn.tree import (
    SvnBasisTree,
    SubversionTree,
    )

def update_wc(adm, basedir, conn, revnum):
    # FIXME: honor SVN_CONFIG_SECTION_HELPERS:SVN_CONFIG_OPTION_DIFF3_CMD
    # FIXME: honor SVN_CONFIG_SECTION_MISCELLANY:SVN_CONFIG_OPTION_USE_COMMIT_TIMES
    # FIXME: honor SVN_CONFIG_SECTION_MISCELLANY:SVN_CONFIG_OPTION_PRESERVED_CF_EXTS
    editor = adm.get_update_editor(basedir, False, True)
    assert editor is not None
    reporter = conn.do_update(revnum, "", True, editor)
    adm.crawl_revisions(basedir, reporter, restore_files=False, 
                        recurse=True, use_commit_times=False)
    # FIXME: handle externals


def apply_prop_changes(orig_props, prop_changes):
    """Apply a set of property changes to a properties dictionary.
    
    :param orig_props: Dictionary with original properties (will be modified)
    :param prop_changes: Dictionary of new property values (None for deletion)
    :return: New dictionary
    """
    for k,v in prop_changes:
        if v is None:
            del orig_props[k]
        else:
            orig_props[k] = v
    return orig_props


def generate_ignore_list(ignore_map):
    """Create a list of ignores, ordered by directory.
    
    :param ignore_map: Dictionary with paths as keys, patterns as values.
    :return: list of ignores
    """
    ignores = []
    keys = ignore_map.keys()
    for k in sorted(keys):
        elements = ["."]
        if k.strip("/") != "":
            elements.append(k.strip("/"))
        elements.append(ignore_map[k].strip("/"))
        ignores.append("/".join(elements))
    return ignores


class SvnWorkingTree(SubversionTree,WorkingTree):
    """WorkingTree implementation that uses a svn working copy for storage."""

    def __init__(self, bzrdir, local_path, branch):
        assert isinstance(local_path, unicode)
        self.basedir = local_path
        version = check_wc(self.basedir.encode("utf-8"))
        self._format = SvnWorkingTreeFormat(version)
        self.bzrdir = bzrdir
        self._branch = branch
        self.base_revnum = 0
        max_rev = revision_status(self.basedir.encode("utf-8"), None, True)[1]
        self._update_base_revnum(max_rev)
        self._detect_case_handling()
        self._transport = bzrdir.get_workingtree_transport(None)
        self.controldir = os.path.join(bzrdir.svn_controldir, 'bzr')
        try:
            os.makedirs(self.controldir)
            os.makedirs(os.path.join(self.controldir, 'lock'))
        except OSError:
            pass
        control_transport = bzrdir.transport.clone(urlutils.join(
                                                   get_adm_dir(), 'bzr'))
        self._control_files = LockableFiles(control_transport, 'lock', LockDir)
        self.views = self._make_views()

    def __repr__(self):
        return "<%s of %s>" % (self.__class__.__name__, self.basedir.encode('utf-8'))

    def get_ignore_list(self):
        """Obtain the list of ignore patterns for this working tree.

        :note: Will interpret the svn:ignore properties, rather than read 
               .bzrignore
        """
        ignores = set([get_adm_dir()])
        ignores.update(svn_config.get_default_ignores())

        def dir_add(wc, prefix, patprefix):
            assert isinstance(prefix, unicode)
            assert isinstance(patprefix, str)
            ignorestr = wc.prop_get(properties.PROP_IGNORE, 
                self.abspath(prefix).rstrip("/").encode("utf-8"))
            if ignorestr is not None:
                for pat in ignorestr.splitlines():
                    ignores.add(urlutils.joinpath(patprefix, pat))

            entries = wc.entries_read(False)
            for entry in entries:
                if entry == "":
                    continue

                # Ignore ignores on things that aren't directories
                if entries[entry].kind != subvertpy.NODE_DIR:
                    continue

                subprefix = osutils.pathjoin(prefix, entry.decode("utf-8"))

                subwc = self._get_wc(subprefix, base=wc)
                try:
                    dir_add(subwc, subprefix, urlutils.joinpath(patprefix, entry))
                finally:
                    subwc.close()

        wc = self._get_wc()
        try:
            dir_add(wc, u"", ".")
        finally:
            wc.close()

        return ignores

    def is_control_filename(self, path):
        """Check whether path is a control file (used by bzr or svn)."""
        return is_adm_dir(path)

    def apply_inventory_delta(self, changes):
        raise NotImplementedError(self.apply_inventory_delta)

    def _update(self, revnum=None):
        if revnum is None:
            # FIXME: should be able to use -1 here
            revnum = self.branch.get_revnum()
        adm = self._get_wc(write_lock=True, depth=-1)
        try:
            conn = self.branch.repository.transport.get_connection(self.branch.get_branch_path())
            try:
                update_wc(adm, self.basedir.encode("utf-8"), conn, revnum)
            finally:
                self.branch.repository.transport.add_connection(conn)
        finally:
            adm.close()
        return revnum

    def update(self, change_reporter=None, possible_transports=None, 
               revnum=None):
        """Update the workingtree to a new Bazaar revision number.
        
        """
        orig_revnum = self.base_revnum
        self._update_base_revnum(self._update(revnum))
        return self.base_revnum - orig_revnum

    def remove(self, files, verbose=False, to_file=None, keep_files=True, 
               force=False):
        """Remove files from the working tree."""
        # FIXME: Use to_file argument
        # FIXME: Use verbose argument
        assert isinstance(files, list)
        wc = self._get_wc(write_lock=True)
        try:
            for file in files:
                wc.delete(self.abspath(file).encode("utf-8"))
        finally:
            wc.close()

        for file in files:
            self._change_fileid_mapping(None, file)
        self.read_working_inventory()

    @convert_svn_error
    def _get_wc(self, relpath=u"", write_lock=False, depth=0, base=None):
        """Open a working copy handle."""
        assert isinstance(relpath, unicode)
        return WorkingCopy(base, 
            self.abspath(relpath).rstrip("/").encode("utf-8"), 
            write_lock, depth)

    def _get_rel_wc(self, relpath, write_lock=False):
        assert isinstance(relpath, unicode)
        dir = os.path.dirname(relpath)
        file = os.path.basename(relpath)
        return (self._get_wc(dir, write_lock), file)

    def move(self, from_paths, to_dir=None, after=False, **kwargs):
        """Move files to a new location."""
        # FIXME: Use after argument
        if after:
            raise NotImplementedError("move after not supported")
        for entry in from_paths:
            from_abspath = osutils.safe_utf8(self.abspath(entry))
            to_wc = self._get_wc(osutils.safe_unicode(to_dir), write_lock=True)
            try:
                to_wc.copy(from_abspath, osutils.safe_utf8(os.path.basename(entry)))
            finally:
                to_wc.close()
            from_wc = self._get_wc(write_lock=True)
            try:
                from_wc.delete(from_abspath)
            finally:
                from_wc.close()
            new_name = osutils.pathjoin(osutils.safe_utf8(to_dir), 
                                        osutils.safe_utf8(os.path.basename(entry)))
            self._change_fileid_mapping(self.inventory.path2id(entry), new_name)
            self._change_fileid_mapping(None, entry)

        self.read_working_inventory()

    def rename_one(self, from_rel, to_rel, after=False):
        from_rel = osutils.safe_unicode(from_rel)
        to_rel = osutils.safe_unicode(to_rel)
        # FIXME: Use after
        if after:
            raise NotImplementedError("rename_one after not supported")
        from_wc = None
        (to_wc, to_file) = self._get_rel_wc(to_rel, write_lock=True)
        try:
            if os.path.dirname(from_rel) == os.path.dirname(to_rel):
                # Prevent lock contention
                from_wc = to_wc
            else:
                (from_wc, _) = self._get_rel_wc(from_rel, write_lock=True)
            try:
                from_id = self.inventory.path2id(from_rel)
                to_wc.copy(self.abspath(from_rel), to_file)
                from_wc.delete(self.abspath(from_rel))
            finally:
                from_wc.close()
        finally:
            if from_wc != to_wc:
                to_wc.close()
        self._change_fileid_mapping(None, from_rel)
        self._change_fileid_mapping(from_id, to_rel)
        self.read_working_inventory()

    def path_to_file_id(self, revnum, current_revnum, path):
        """Generate a bzr file id from a Subversion file name. 
        
        :param revnum: Revision number.
        :param path: Absolute path within the Subversion repository.
        :return: Tuple with file id and revision id.
        """
        assert isinstance(revnum, int) and revnum >= 0
        assert isinstance(path, str)

        rp = self.branch.unprefix(path)
        entry = idmap_lookup(self.basis_tree().id_map, self.basis_tree().workingtree.branch.mapping, rp.decode("utf-8"))
        assert entry[0] is not None
        assert isinstance(entry[0], str), "fileid %r for %r is not a string" % (entry[0], path)
        return entry[:2]

    def read_working_inventory(self):
        """'Read' the working inventory.

        """
        inv = Inventory()

        def add_file_to_inv(relpath, id, revid, parent_id):
            """Add a file to the inventory.
            
            :param relpath: Path relative to working tree root
            :param id: File id of current directory
            :param revid: Revision id
            :param parent_id: File id of parent directory
            """
            assert isinstance(relpath, unicode)
            abspath = self.abspath(relpath)
            if os.path.islink(abspath):
                file = InventoryLink(id, os.path.basename(relpath), parent_id)
                file.revision = revid
                file.symlink_target = os.readlink(abspath.encode(osutils._fs_enc)).decode(osutils._fs_enc)
                file.text_sha1 = None
                file.text_size = None
                file.executable = False
                inv.add(file)
            else:
                file = InventoryFile(id, os.path.basename(relpath), parent_id)
                file.revision = revid
                try:
                    data = osutils.fingerprint_file(open(abspath.encode(osutils._fs_enc)))
                    file.text_sha1 = data['sha1']
                    file.text_size = data['size']
                    file.executable = self.is_executable(id, relpath)
                    inv.add(file)
                except IOError:
                    # Ignore non-existing files
                    pass

        def find_copies(url, relpath=u""):
            """Find copies of the specified path

            :param url: URL of which to find copies
            :param relpath: Optional subpath to search in
            :return: Yields all copies
            """
            ret = []
            wc = self._get_wc(relpath)
            try:
                entries = wc.entries_read(False)
                for entry in entries.values():
                    subrelpath = osutils.pathjoin(relpath, entry.name.decode("utf-8"))
                    if entry.name == "" or entry.kind != 'directory':
                        if ((entry.copyfrom_url == url or entry.url == url) and 
                            not (entry.schedule in (SCHEDULE_DELETE,
                                                    SCHEDULE_REPLACE))):
                            ret.append(osutils.pathjoin(
                                    self.branch.get_branch_path().strip("/").decode("utf-8"), 
                                    subrelpath))
                    else:
                        find_copies(url, subrelpath)
            finally:
                wc.close()
            return ret

        def find_ids(entry, rootwc):
            relpath = urllib.unquote(entry.url[len(entry.repos):].strip("/"))
            assert entry.schedule in (SCHEDULE_NORMAL, 
                                      SCHEDULE_DELETE,
                                      SCHEDULE_ADD,
                                      SCHEDULE_REPLACE)
            if entry.schedule == SCHEDULE_NORMAL:
                assert entry.revision >= 0
                # Keep old id
                return self.path_to_file_id(entry.cmt_rev, entry.revision, 
                        relpath)
            elif entry.schedule == SCHEDULE_DELETE:
                return (None, None)
            elif (entry.schedule == SCHEDULE_ADD or 
                  entry.schedule == SCHEDULE_REPLACE):
                # See if the file this file was copied from disappeared
                # and has no other copies -> in that case, take id of other file
                if (entry.copyfrom_url and 
                    list(find_copies(entry.copyfrom_url)) == [relpath]):
                    return self.path_to_file_id(entry.copyfrom_rev, 
                        entry.revision, entry.copyfrom_url[len(entry.repos):])
                ids = self._get_new_file_ids(rootwc)
                if ids.has_key(relpath):
                    return (ids[relpath], None)
                # FIXME: Generate more random file ids
                return ("NEW-" + escape_svn_path(entry.url[len(entry.repos):].strip("/")), None)

        def add_dir_to_inv(relpath, wc, parent_id):
            assert isinstance(relpath, unicode)
            entries = wc.entries_read(False)
            entry = entries[""]
            assert parent_id is None or isinstance(parent_id, str), \
                    "%r is not a string" % parent_id
            (id, revid) = find_ids(entry, rootwc)
            if id is None:
                mutter('no id for %r', entry.url)
                return
            assert revid is None or isinstance(revid, str), "%r is not a string" % revid
            assert isinstance(id, str), "%r is not a string" % id

            # First handle directory itself
            inv.add_path(relpath, 'directory', id, parent_id).revision = revid
            if relpath == u"":
                inv.revision_id = revid

            for name in entries:
                if name == "":
                    continue

                subrelpath = os.path.join(relpath, name.decode("utf-8"))

                entry = entries[name]
                assert entry
                
                if entry.kind == subvertpy.NODE_DIR:
                    subwc = self._get_wc(subrelpath, base=wc)
                    try:
                        assert isinstance(subrelpath, unicode)
                        add_dir_to_inv(subrelpath, subwc, id)
                    finally:
                        subwc.close()
                else:
                    (subid, subrevid) = find_ids(entry, rootwc)
                    if subid:
                        assert isinstance(subrelpath, unicode)
                        add_file_to_inv(subrelpath, subid, subrevid, id)
                    else:
                        mutter('no id for %r', entry.url)

        rootwc = self._get_wc() 
        try:
            add_dir_to_inv(u"", rootwc, None)
        finally:
            rootwc.close()

        self._set_inventory(inv, dirty=False)
        return inv

    def _set_base(self, revid, revnum, tree=None):
        assert isinstance(revid, str)
        self.base_revid = revid
        assert isinstance(revnum, int)
        self.base_revnum = revnum
        self.base_tree = tree

    def set_last_revision(self, revid):
        mutter('setting last revision to %r', revid)
        if revid is None or revid == NULL_REVISION:
            self._set_base(revid, 0)
            return

        rev = self.branch.lookup_revision_id(revid)
        self._set_base(revid, rev)

    def set_parent_trees(self, parents_list, allow_leftmost_as_ghost=False):
        """See MutableTree.set_parent_trees."""
        self.set_parent_ids([rev for (rev, tree) in parents_list])

    def set_parent_ids(self, parent_ids):
        """See MutableTree.set_parent_ids."""
        super(SvnWorkingTree, self).set_parent_ids(parent_ids)
        if parent_ids == [] or parent_ids[0] == NULL_REVISION:
            merges = []
        else:
            merges = parent_ids[1:]
        adm = self._get_wc(write_lock=True)
        try:
            svk_merges = svk.parse_svk_features(self._get_svk_merges(self._get_base_branch_props()))

            # Set svk:merge
            for merge in merges:
                try:
                    svk_merges.add(_revision_id_to_svk_feature(merge,
                        self.branch.repository.lookup_revision_id))
                except NoSuchRevision:
                    pass

            adm.prop_set(svk.SVN_PROP_SVK_MERGE, 
                         svk.serialize_svk_features(svk_merges), self.basedir.encode("utf-8"))
        finally:
            adm.close()

    def smart_add(self, file_list, recurse=True, action=None, save=True):
        """See MutableTree.smart_add()."""
        assert isinstance(recurse, bool)
        if action is None:
            action = bzrlib.add.AddAction()
        # TODO: use action
        if not file_list:
            # no paths supplied: add the entire tree.
            file_list = [u'.']
        ignored = defaultdict(list)
        added = []

        for file_path in file_list:
            todo = []
            file_path = os.path.abspath(osutils.safe_unicode(file_path))
            f = self.relpath(file_path)
            wc = self._get_wc(os.path.dirname(f.encode(osutils._fs_enc)).decode(osutils._fs_enc), write_lock=True)
            try:
                if not self.inventory.has_filename(f):
                    if save:
                        mutter('adding %r', file_path)
                        wc.add(file_path.encode("utf-8"))
                    added.append(file_path)
                if recurse and osutils.file_kind(file_path.encode(osutils._fs_enc)) == 'directory':
                    # Filter out ignored files and update ignored
                    for c in os.listdir(file_path.encode(osutils._fs_enc)):
                        if self.is_control_filename(c):
                            continue
                        c = c.decode(osutils._fs_enc)
                        c_path = osutils.pathjoin(file_path, c)
                        ignore_glob = self.is_ignored(c)
                        if ignore_glob is not None:
                            ignored[ignore_glob].append(c_path)
                        todo.append(c_path)
            finally:
                wc.close()
            if todo != []:
                cadded, cignored = self.smart_add(todo, recurse, action, save)
                added.extend(cadded)
                ignored.update(cignored)
        return added, ignored

    def add(self, files, ids=None, kinds=None):
        """Add files to the working tree."""
        # TODO: Use kinds
        if isinstance(files, str):
            files = [files]
            if isinstance(ids, str):
                ids = [ids]
        if ids is not None:
            ids = iter(ids)
        assert isinstance(files, list)
        for f in files:
            wc = self._get_wc(os.path.dirname(osutils.safe_unicode(f)), write_lock=True)
            try:
                try:
                    wc.add(osutils.safe_utf8(self.abspath(f)))
                    if ids is not None:
                        self._change_fileid_mapping(ids.next(), f, wc)
                except subvertpy.SubversionException, (_, num):
                    if num == subvertpy.ERR_ENTRY_EXISTS:
                        continue
                    elif num == subvertpy.ERR_WC_PATH_NOT_FOUND:
                        raise NoSuchFile(path=f)
                    raise
            finally:
                wc.close()
        self.read_working_inventory()

    def basis_tree(self):
        """Return the basis tree for a working tree."""
        if self.base_revid is None or self.base_revid == NULL_REVISION:
            return self.branch.repository.revision_tree(self.base_revid)

        if self.base_tree is None:
            self.base_tree = SvnBasisTree(self)

        return self.base_tree

    def _update_base_revnum(self, fetched):
        self._set_base(self.branch.generate_revision_id(fetched), fetched)
        self.read_working_inventory()

    def pull(self, source, overwrite=False, stop_revision=None, 
             delta_reporter=None, possible_transports=None):
        """Pull in changes from another branch into this working tree."""
        # FIXME: Use delta_reporter
        # FIXME: Use source
        # FIXME: Use overwrite
        result = self.branch.pull(source, overwrite=overwrite, stop_revision=stop_revision)
        fetched = self._update(self.branch.get_revnum())
        self._update_base_revnum(fetched)
        return result

    def get_file_properties(self, file_id, path=None):
        if path is None:
            path = self._inventory.id2path(file_id)
        wc = self._get_wc()
        try:
            (prop_changes, orig_props) = wc.get_prop_diffs(self.basedir.encode("utf-8"))
        finally:
            wc.close()
        return apply_prop_changes(orig_props, prop_changes)

    def get_file_sha1(self, file_id, path=None, stat_value=None):
        """Determine the SHA1 for a file."""
        if path is None:
            path = self._inventory.id2path(file_id)
        return osutils.fingerprint_file(open(self.abspath(path).encode(osutils._fs_enc)))['sha1']

    def _change_fileid_mapping(self, id, path, wc=None):
        if wc is None:
            subwc = self._get_wc(write_lock=True)
        else:
            subwc = wc
        try:
            new_entries = self._get_new_file_ids(subwc)
            if id is None:
                if new_entries.has_key(path):
                    del new_entries[path]
            else:
                assert isinstance(id, str)
                new_entries[path] = id
            fileprops = self._get_branch_props()
            self.branch.mapping.export_fileid_map_fileprops(new_entries, fileprops)
            self._set_branch_props(subwc, fileprops)
        finally:
            if wc is None:
                subwc.close()

    def _get_changed_branch_props(self):
        wc = self._get_wc()
        try:
            ret = {}
            (prop_changes, orig_props) = wc.get_prop_diffs(self.basedir.encode("utf-8"))
            for k,v in prop_changes:
                ret[k] = (orig_props.get(k), v)
            return ret
        finally:
            wc.close()

    def _get_branch_props(self):
        wc = self._get_wc()
        try:
            (prop_changes, orig_props) = wc.get_prop_diffs(self.basedir.encode("utf-8"))
            return apply_prop_changes(orig_props, prop_changes)
        finally:
            wc.close()

    def _set_branch_props(self, wc, fileprops):
        for k,v in fileprops.iteritems():
            wc.prop_set(k, v, self.basedir.encode("utf-8"))

    def _get_base_branch_props(self):
        wc = self._get_wc()
        try:
            (prop_changes, orig_props) = wc.get_prop_diffs(self.basedir.encode("utf-8"))
            return orig_props
        finally:
            wc.close()

    def _get_new_file_ids(self, wc):
        return self.branch.mapping.import_fileid_map_fileprops(
            self._get_changed_branch_props())

    def _get_svk_merges(self, base_branch_props):
        return base_branch_props.get(svk.SVN_PROP_SVK_MERGE, "")

    def apply_inventory_delta(self, delta):
        assert delta == []

    def _last_revision(self):
        return self.base_revid

    def path_content_summary(self, path, _lstat=os.lstat,
        _mapper=osutils.file_kind_from_stat_mode):
        """See Tree.path_content_summary."""
        abspath = self.abspath(path)
        try:
            stat_result = _lstat(abspath.encode(osutils._fs_enc))
        except OSError, e:
            if getattr(e, 'errno', None) == errno.ENOENT:
                # no file.
                return ('missing', None, None, None)
            # propagate other errors
            raise
        kind = _mapper(stat_result.st_mode)
        if kind == 'file':
            size = stat_result.st_size
            # try for a stat cache lookup
            executable = self._is_executable_from_path_and_stat(path, stat_result)
            return (kind, size, executable, self._sha_from_stat(
                path, stat_result))
        elif kind == 'directory':
            return kind, None, None, None
        elif kind == 'symlink':
            return ('symlink', None, None, os.readlink(abspath.encode(osutils._fs_enc)).decode(osutils._fs_enc))
        else:
            return (kind, None, None, None)

    def _get_base_revmeta(self):
        return self.branch.repository._revmeta_provider.lookup_revision(self.branch.get_branch_path(self.base_revnum), self.base_revnum)

    def _reset_data(self):
        pass

    def break_lock(self):
        """Break a lock if one is present from another instance.

        Uses the ui factory to ask for confirmation if the lock may be from
        an active process.

        This will probe the repository for its lock as well.
        """
        if getattr(subvertpy.wc, "cleanup", None) is not None:
            subvertpy.wc.cleanup(self.basedir.encode("utf-8"))
        self._control_files.break_lock()

    def unlock(self):
        # non-implementation specific cleanup
        self._cleanup()

        # reverse order of locking.
        try:
            return self._control_files.unlock()
        finally:
            self.branch.unlock()

    if not osutils.supports_executable():
        def is_executable(self, file_id, path=None):
            inv = self.basis_tree()._inventory
            if file_id in inv:
                return inv[file_id].executable
            # Default to not executable
            return False

    def update_basis_by_delta(self, new_revid, delta):
        """Update the parents of this tree after a commit.

        This gives the tree one parent, with revision id new_revid. The
        inventory delta is applied to the current basis tree to generate the
        inventory for the parent new_revid, and all other parent trees are
        discarded.

        All the changes in the delta should be changes synchronising the basis
        tree with some or all of the working tree, with a change to a directory
        requiring that its contents have been recursively included. That is,
        this is not a general purpose tree modification routine, but a helper
        for commit which is not required to handle situations that do not arise
        outside of commit.

        :param new_revid: The new revision id for the trees parent.
        :param delta: An inventory delta (see apply_inventory_delta) describing
            the changes from the current left most parent revision to new_revid.
        """
        rev = self.branch.lookup_revision_id(new_revid)
        self._set_base(new_revid, rev)

        # TODO: Implement more efficient version
        newrev = self.branch.repository.get_revision(new_revid)
        newrevtree = self.branch.repository.revision_tree(new_revid)
        svn_revprops = self.branch.repository._log.revprop_list(rev)

        simple_revprops_cache = {
                new_revid: (rev, svn_revprops[properties.PROP_REVISION_DATE],
                    svn_revprops.get(properties.PROP_REVISION_AUTHOR, ""))}

        def lookup_revid(revid):
            try:
                return simple_revprops_cache[revid]
            except KeyError:
                revmeta, mapping = self.branch.repository._get_revmeta(revid)
                revprops = revmeta.get_revprops()
                ret = (revmeta.revnum, 
                       revprops[properties.PROP_REVISION_DATE],
                       revprops.get(properties.PROP_REVISION_AUTHOR, ""))
                simple_revprops_cache[revid] = ret
                return ret

        def process_committed(adm, relpath, fileprops, revid):
            mutter("process %r -> %r", relpath, revid)
            abspath = self.abspath(relpath).rstrip("/")
            revnum = int(fileprops['svn:entry:committed-rev'])
            date = fileprops['svn:entry:committed-date']
            author = fileprops.get('svn:entry:last-author', '')
            adm.process_committed(abspath.encode("utf-8"), False, revnum, date, author)
            mutter("doneprocess %r -> %r", relpath, revid)

        def update_settings(adm, path, id):
            assert isinstance(path, unicode)
            if newrevtree.inventory[id].kind != 'directory':
                return

            entries = adm.entries_read(False)
            for name, entry in entries.iteritems():
                name = name.decode("utf-8")
                if name == "":
                    fileprops = newrevtree.get_file_properties(id, path)
                    process_committed(adm, path, fileprops, 
                                  newrevtree.inventory[id].revision)
                    continue

                child_path = os.path.join(path, name)
                assert isinstance(child_path, unicode)
                child_id = newrevtree.inventory.path2id(child_path)
                child_abspath = self.abspath(child_path).rstrip("/")
                assert isinstance(child_abspath, unicode)

                if not child_id in newrevtree.inventory:
                    pass
                elif newrevtree.inventory[child_id].kind == 'directory':
                    subwc = self._get_wc(child_path, write_lock=True, base=adm)
                    try:
                        update_settings(subwc, child_path, child_id)
                    finally:
                        subwc.close()
                else:
                    pristine_abspath = get_pristine_copy_path(
                        child_abspath.encode("utf-8")).decode("utf-8")
                    if os.path.exists(pristine_abspath.encode(osutils._fs_enc)):
                        os.remove(pristine_abspath.encode(osutils._fs_enc))
                    source_f = open(child_abspath.encode(osutils._fs_enc), "r")
                    target_f = open(pristine_abspath.encode(osutils._fs_enc), "w")
                    target_f.write(source_f.read())
                    target_f.close()
                    source_f.close()
                    fileprops = newrevtree.get_file_properties(child_id, 
                            child_path)
                    process_committed(adm, child_path, fileprops,
                                  newrevtree.inventory[child_id].revision)

        # Set proper version for all files in the wc
        adm = self._get_wc(write_lock=True)
        try:
            update_settings(adm, u"", newrevtree.inventory.root.file_id)
        finally:
            adm.close()

        self.set_parent_ids([new_revid])


class SvnWorkingTreeFormat(WorkingTreeFormat):
    """Subversion working copy format."""

    def __init__(self, version):
        self.version = version

    def __get_matchingbzrdir(self):
        return SvnWorkingTreeDirFormat()

    _matchingbzrdir = property(__get_matchingbzrdir)

    def get_format_description(self):
        return "Subversion Working Copy Version %d" % self.version

    def get_format_string(self):
        raise NotImplementedError

    def initialize(self, a_bzrdir, revision_id=None):
        raise NotImplementedError(self.initialize)

    def open(self, a_bzrdir):
        raise NotImplementedError(self.initialize)


class SvnCheckout(BzrDir):
    """BzrDir implementation for Subversion checkouts (directories 
    containing a .svn subdirectory."""

    def __init__(self, transport, format):
        super(SvnCheckout, self).__init__(transport, format)
        self.local_path = transport.local_abspath(".")

        # Open related remote repository + branch
        try:
            wc = WorkingCopy(None, self.local_path.encode("utf-8"))
        except subvertpy.SubversionException, (msg, ERR_WC_UNSUPPORTED_FORMAT):
            raise UnsupportedFormatError(msg, kind='workingtree')
        try:
            self.svn_url = wc.entry(self.local_path.encode("utf-8"), True).url
        finally:
            wc.close()

        self._remote_transport = None
        self._remote_bzrdir = None
        self.svn_controldir = os.path.join(self.local_path, get_adm_dir())
        self.root_transport = self.transport = transport

    def backup_bzrdir(self):
        self.root_transport.copy_tree(".svn", ".svn.backup")
        return (self.root_transport.abspath(".svn"),
                self.root_transport.abspath(".svn.backup"))

    def is_control_filename(self, filename):
        return filename == '.svn' or filename.startswith('.svn/')

    def get_remote_bzrdir(self):
        if self._remote_bzrdir is None:
            self._remote_bzrdir = SvnRemoteAccess(self.get_remote_transport())
        return self._remote_bzrdir

    def get_remote_transport(self):
        if self._remote_transport is None:
            self._remote_transport = SvnRaTransport(self.svn_url)
        return self._remote_transport
        
    def clone(self, path, revision_id=None, force_new_repo=False):
        raise NotImplementedError(self.clone)

    def open_workingtree(self, _unsupported=False, recommend_upgrade=False):
        try:
            return SvnWorkingTree(self, self.local_path, self.open_branch())
        except bzrsvn_errors.NotSvnBranchPath, e:
            raise NoWorkingTree(self.local_path)

    def sprout(self, url, revision_id=None, force_new_repo=False, 
               recurse='down', possible_transports=None, accelerator_tree=None,
               hardlink=False):
        # FIXME: honor force_new_repo
        # FIXME: Use recurse
        result = get_rich_root_format().initialize(url)
        repo = self._find_repository()
        repo.clone(result, revision_id)
        branch = self.open_branch()
        branch.sprout(result, revision_id)
        result.create_workingtree(hardlink=hardlink)
        return result

    def create_repository(self, shared=False):
        raise UninitializableFormat(self._format)

    def open_repository(self):
        raise NoRepositoryPresent(self)

    def find_repository(self, _ignore_branch_path=False):
        raise NoRepositoryPresent(self)

    def _find_repository(self):
        return SvnRepository(self, self.get_remote_transport().clone_root(), 
                             self.get_remote_bzrdir().branch_path)

    def needs_format_conversion(self, format=None):
        if format is None:
            format = BzrDirFormat.get_default_format()
        return not isinstance(self._format, format.__class__)

    def get_workingtree_transport(self, format):
        assert format is None
        return get_transport(self.svn_controldir)

    def create_workingtree(self, revision_id=None, hardlink=None):
        """See BzrDir.create_workingtree().

        Not implemented for Subversion because having a .svn directory
        implies having a working copy.
        """
        raise NotImplementedError(self.create_workingtree)

    def create_branch(self):
        """See BzrDir.create_branch()."""
        raise NotImplementedError(self.create_branch)

    def open_branch(self, unsupported=True):
        """See BzrDir.open_branch()."""
        repos = self._find_repository()

        try:
            branch = SvnBranch(repos, self.get_remote_bzrdir().branch_path)
        except subvertpy.SubversionException, (_, num):
            if num == subvertpy.ERR_WC_NOT_DIRECTORY:
                raise NotBranchError(path=self.base)
            raise

        branch.bzrdir = self.get_remote_bzrdir()
 
        return branch


class SvnCheckoutConverter(Converter):
    """Converts from a Subversion directory to a bzr dir."""
    def __init__(self, target_format):
        """Create a SvnCheckoutConverter.
        :param target_format: The format the resulting repository should be.
        """
        super(SvnCheckoutConverter, self).__init__()
        self.target_format = target_format

    def convert(self, to_convert, pb):
        """See Converter.convert()."""
        remote_branch = to_convert.open_branch()
        bzrdir = self.target_format.initialize(to_convert.root_transport.base)
        branch = BranchReferenceFormat().initialize(bzrdir, remote_branch)
        wt = bzrdir.create_workingtree()
        # FIXME: Convert working tree
        to_convert.root_transport.delete_tree(".svn")
        return bzrdir
