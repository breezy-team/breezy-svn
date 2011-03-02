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
from subvertpy import (
    ERR_WC_UNSUPPORTED_FORMAT,
    properties,
    )
from subvertpy.wc import (
    SCHEDULE_ADD,
    SCHEDULE_DELETE,
    SCHEDULE_NORMAL,
    SCHEDULE_REPLACE,
    WorkingCopy,
    check_wc,
    get_adm_dir,
    is_adm_dir,
    revision_status,
    )
import urllib

import bzrlib.add
from bzrlib import (
    hashcache,
    osutils,
    urlutils,
    version_info as bzrlib_version,
    )
from bzrlib.branch import (
    BranchReferenceFormat,
    )
from bzrlib.errors import (
    BzrError,
    NotBranchError,
    NoSuchFile,
    NoSuchId,
    NoSuchRevision,
    NoRepositoryPresent,
    NoWorkingTree,
    TransportNotPossible,
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
from bzrlib.revision import (
    NULL_REVISION,
    )
from bzrlib.trace import mutter
from bzrlib.transport import get_transport
from bzrlib.workingtree import (
    WorkingTree,
    WorkingTreeFormat,
    )

from bzrlib.plugins.svn import (
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
    NotSvnBranchPath,
    )
from bzrlib.plugins.svn.format import (
    SvnWorkingTreeDirFormat,
    )
from bzrlib.plugins.svn.mapping import (
    escape_svn_path,
    get_svn_file_contents,
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
    BasisTreeIncomplete,
    SvnBasisTree,
    SubversionTree,
    )

from bzrlib.bzrdir import (
    Converter,
    )
from bzrlib.controldir import (
    ControlDirFormat,
    ControlDir,
    format_registry,
    )


class RepositoryRootUnknown(BzrError):
    _fmt = "The working tree does not store the root of the Subversion repository."


class LocalRepositoryOpenFailed(BzrError):

    _fmt = ("Unable to open local repository at %(url)s")

    def __init__(self, url):
        self.url = url


class CorruptWorkingTree(BzrError):

    _fmt = ("Unable to open working tree at %(path)s: %(msg)s")

    def __init__(self, path, msg):
        self.path = path
        self.msg = msg


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

    def __init__(self, bzrdir, local_path, entry):
        assert isinstance(local_path, unicode)
        self.entry = entry
        self.basedir = local_path
        version = check_wc(self.basedir.encode("utf-8"))
        self._format = SvnWorkingTreeFormat(version)
        self.bzrdir = bzrdir
        self._branch = None
        self.base_revnum = 0
        max_rev = revision_status(self.basedir.encode("utf-8"), None, True)[1]
        self.mapping = self.branch.mapping
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
        cache_filename = control_transport.local_abspath('stat-cache')
        self._hashcache = hashcache.HashCache(self.basedir, cache_filename,
            self.bzrdir._get_file_mode(),
            self._content_filter_stack_provider())
        hc = self._hashcache
        hc.read()
        self._control_files = LockableFiles(control_transport, 'lock', LockDir)
        self.views = self._make_views()

    def _setup_directory_is_tree_reference(self):
        self._directory_is_tree_reference = self._directory_is_never_tree_reference

    @property
    def basis_idmap(self):
        if self._base_idmap is None:
            self._base_idmap = self.branch.repository.get_fileid_map(
                    self._get_base_revmeta(),
                    self.mapping)
        return self._base_idmap

    def get_branch_path(self, revnum=None):
        if revnum is None:
            try:
                return self.bzrdir.get_branch_path()
            except RepositoryRootUnknown:
                pass
        return self.branch.get_branch_path(revnum)

    @property
    def branch(self):
        if self._branch is None:
            self._branch = self.bzrdir.open_branch()
        return self._branch

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

                try:
                    subwc = self._get_wc(subprefix, base=wc)
                except subvertpy.SubversionException, (_, num):
                    if num == subvertpy.ERR_WC_NOT_DIRECTORY:
                        continue
                    raise
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

    def _update(self, revnum=None):
        if revnum is None:
            # FIXME: should be able to use -1 here
            revnum = self.branch.get_revnum()
        bp = self.get_branch_path()
        adm = self._get_wc(write_lock=True, depth=-1)
        try:
            conn = self.branch.repository.transport.get_connection(bp)
            try:
                update_wc(adm, self.basedir.encode("utf-8"), conn, revnum)
            finally:
                self.branch.repository.transport.add_connection(conn)
        finally:
            adm.close()
        return revnum

    def update(self, change_reporter=None, possible_transports=None,
               revision=None, old_tip=None, show_base=False, revnum=None):
        """Update the workingtree to a new Bazaar revision number.

        """
        orig_revnum = self.base_revnum
        if revision is not None and revnum is not None:
            raise AssertionError("revision and revnum are mutually exclusive")
        if revision is not None:
            revnum = self.branch.lookup_bzr_revision_id(revision)
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
                wc.delete(osutils.safe_utf8(self.abspath(file)),
                          keep_local=keep_files)
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
        :param path: Path of the file
        :return: Tuple with file id and revision id.
        """
        assert isinstance(path, unicode)
        path = osutils.normpath(path)
        if path == u".":
            path = u""
        return self.lookup_id(path)

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
                                    self.get_branch_path().strip("/").decode("utf-8"),
                                    subrelpath))
                    else:
                        find_copies(url, subrelpath)
            finally:
                wc.close()
            return ret

        def find_ids(relpath, entry, rootwc):
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
                    list(find_copies(entry.copyfrom_url)) == [relpath.encode("utf-8")]):
                    return self.path_to_file_id(entry.copyfrom_rev,
                        entry.revision, self.unprefix(urllib.unquote(entry.copyfrom_url[len(entry.repos):])).decode("utf-8"))
                ids = self._get_new_file_ids()
                if ids.has_key(relpath):
                    return (ids[relpath], None)
                # FIXME: Generate more random but consistent file ids
                return ("NEW-" + escape_svn_path(entry.url[len(entry.repos):].strip("/")), None)

        def add_dir_to_inv(relpath, wc, parent_id):
            assert isinstance(relpath, unicode)
            entries = wc.entries_read(False)
            entry = entries[""]
            assert parent_id is None or isinstance(parent_id, str), \
                    "%r is not a string" % parent_id
            (id, revid) = find_ids(relpath, entry, rootwc)
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
                    try:
                        subwc = self._get_wc(subrelpath, base=wc)
                    except subvertpy.SubversionException, (_, num):
                        if num == subvertpy.ERR_WC_NOT_DIRECTORY:
                            continue
                        raise
                    try:
                        assert isinstance(subrelpath, unicode)
                        add_dir_to_inv(subrelpath, subwc, id)
                    finally:
                        subwc.close()
                else:
                    (subid, subrevid) = find_ids(subrelpath, entry, rootwc)
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
        assert revid is None or isinstance(revid, str)
        self.base_revid = revid
        assert isinstance(revnum, int)
        self._base_idmap = None
        self.base_revnum = revnum
        self.base_tree = tree

    def set_last_revision(self, revid):
        mutter('setting last revision to %r', revid)
        rev = self.branch.lookup_bzr_revision_id(revid)
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
            old_svk_merges = svk.parse_svk_features(self._get_svk_merges(
                self._get_base_branch_props()))
            svk_merges = set(old_svk_merges)

            # Set svk:merge
            for merge in merges:
                try:
                    svk_merges.add(_revision_id_to_svk_feature(merge,
                        self.branch.repository.lookup_bzr_revision_id))
                except NoSuchRevision:
                    pass
            if old_svk_merges != svk_merges:
                adm.prop_set(
                    svk.SVN_PROP_SVK_MERGE, svk.serialize_svk_features(svk_merges),
                    self.basedir.encode("utf-8"))
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
                if (recurse and
                    osutils.file_kind(file_path.encode(osutils._fs_enc)) == 'directory'):
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

    def add(self, files, ids=None, kinds=None, _copyfrom=None):
        """Add files to the working tree."""
        # TODO: Use kinds
        if isinstance(files, basestring):
            files = [files]
            if isinstance(ids, str):
                ids = [ids]
        if ids is None:
            ids = [None] * len(files)
        if _copyfrom is None:
            _copyfrom = [(None, -1)] * len(files)
        if kinds is None:
            kinds = [self._kind(file) for file in files]
        assert isinstance(files, list)
        for f, kind, file_id, copyfrom in zip(files, kinds, ids, _copyfrom):
            wc = self._get_wc(os.path.dirname(osutils.safe_unicode(f)),
                              write_lock=True)
            try:
                try:
                    utf8_abspath = osutils.safe_utf8(self.abspath(f))
                    wc.add(utf8_abspath, copyfrom[0], copyfrom[1])
                except subvertpy.SubversionException, (_, num):
                    if num in (subvertpy.ERR_ENTRY_EXISTS,
                               subvertpy.ERR_WC_SCHEDULE_CONFLICT):
                        continue
                    elif num == subvertpy.ERR_WC_PATH_NOT_FOUND:
                        raise NoSuchFile(path=f)
                    raise
            finally:
                wc.close()
            if file_id is not None:
                self._change_fileid_mapping(file_id, f)
        self.read_working_inventory()

    def basis_tree(self):
        """Return the basis tree for a working tree."""
        if self.base_tree is None:
            try:
                self.base_tree = SvnBasisTree(self)
            except BasisTreeIncomplete:
                self.base_tree = self.branch.basis_tree()

        return self.base_tree

    def unprefix(self, relpath):
        """Remove the branch path from a relpath.

        :param relpath: path from the repository root.
        """
        assert relpath.startswith(self.get_branch_path()), \
                "expected %s prefix, got %s" % (self.get_branch_path(), relpath)
        return relpath[len(self.get_branch_path()):].strip("/")

    def _update_base_revnum(self, fetched):
        self._set_base(None, fetched)
        self.read_working_inventory()

    def pull(self, source, overwrite=False, stop_revision=None,
             delta_reporter=None, possible_transports=None, local=False):
        """Pull in changes from another branch into this working tree."""
        # FIXME: Use delta_reporter
        # FIXME: Use source
        # FIXME: Use overwrite
        result = self.branch.pull(source, overwrite=overwrite,
            stop_revision=stop_revision, local=local)
        fetched = self._update(self.branch.get_revnum())
        self._update_base_revnum(fetched)
        return result

    def get_file_properties(self, file_id, path=None):
        if path is None:
            path = self._inventory.id2path(file_id)
        else:
            path = osutils.safe_unicode(path)
        abspath = self.abspath(path)
        if not os.path.isdir(abspath.encode(osutils._fs_enc)):
            wc = self._get_wc(urlutils.split(path)[0])
        else:
            wc = self._get_wc(path)
        try:
            (prop_changes, orig_props) = wc.get_prop_diffs(abspath.encode("utf-8"))
        finally:
            wc.close()
        return apply_prop_changes(orig_props, prop_changes)

    def _change_fileid_mapping(self, id, path, wc=None):
        """Change the file id mapping for a particular path."""
        if wc is None:
            subwc = self._get_wc(write_lock=True)
        else:
            subwc = wc
        try:
            new_entries = self._get_new_file_ids()
            if id is None:
                if new_entries.has_key(path):
                    del new_entries[path]
            else:
                assert isinstance(id, str)
                new_entries[path] = id
            fileprops = self._get_branch_props()
            self.mapping.export_fileid_map_fileprops(new_entries, fileprops)
            self._set_branch_props(subwc, fileprops)
        finally:
            if wc is None:
                subwc.close()

    def lookup_id(self, path, entry=None):
        ids = self._get_new_file_ids()
        if path in ids:
            return (ids[path], None)
        return self.basis_idmap.lookup(self.mapping, path)[:2]

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
        return self.get_file_properties(None, "")

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

    def _get_new_file_ids(self):
        return self.mapping.import_fileid_map_fileprops(
            self._get_changed_branch_props())

    def _get_svk_merges(self, base_branch_props):
        return base_branch_props.get(svk.SVN_PROP_SVK_MERGE, "")

    def _apply_inventory_delta_change(self, base_tree, old_path, new_path,
                                      file_id, ie):
        already_there = (
            old_path == new_path and
            base_tree._inventory[file_id].kind == ie.kind)
        if old_path is not None:
            old_abspath = osutils.safe_utf8(self.abspath(old_path))
            if not already_there:
                self.remove([old_path], keep_files=True)
            copyfrom = (urlutils.join(self.entry.url, old_path), self.base_revnum)
        else:
            try:
                old_path = base_tree.id2path(file_id)
            except NoSuchId:
                copyfrom = (None, -1)
            else:
                copyfrom = (urlutils.join(self.entry.url, old_path),
                            self.base_revnum)
        if new_path is not None:
            if not already_there:
                # if base_tree.id2path(file_id) == new_path and base_tree._inventory[file_id].kind == ie.kind
                # FIXME: Revert
                # else:
                self.add([new_path], [file_id], _copyfrom=[copyfrom])

    def apply_inventory_delta(self, delta):
        """Apply an inventory delta."""
        def cmp_delta_changes(a, b):
            # Only sort on new_path for now
            return cmp(a[1], b[1])
        delta.sort(cmp_delta_changes)
        base_tree = self.basis_tree()
        for (old_path, new_path, file_id, ie) in delta:
            self._apply_inventory_delta_change(base_tree, old_path, new_path,
                file_id, ie)

    def _last_revision(self):
        if self.base_revid is None:
            self.base_revid = self.branch.generate_revision_id(self.base_revnum)
        return self.base_revid

    def path_content_summary(self, path, _lstat=os.lstat,
        _mapper=osutils.file_kind_from_stat_mode):
        """See Tree.path_content_summary."""
        abspath = self.abspath(path)
        try:
            stat_result = _lstat(abspath.encode(osutils._fs_enc))
        except OSError, e:
            import errno
            if getattr(e, 'errno', None) == errno.ENOENT:
                # no file.
                return ('missing', None, None, None)
            # propagate other errors
            raise
        kind = _mapper(stat_result.st_mode)
        if kind == 'file':
            size = stat_result.st_size
            # try for a stat cache lookup
            executable = self._is_executable_from_path_and_stat(path,
                stat_result)
            return (kind, size, executable, self._sha_from_stat(
                path, stat_result))
        elif kind == 'directory':
            return kind, None, None, None
        elif kind == 'symlink':
            return ('symlink', None, None, os.readlink(abspath.encode(osutils._fs_enc)).decode(osutils._fs_enc))
        else:
            return (kind, None, None, None)

    def _get_base_revmeta(self):
        return self.branch.repository._revmeta_provider.lookup_revision(
            self.get_branch_path(self.base_revnum), self.base_revnum)

    def _reset_data(self):
        pass

    def break_lock(self):
        """Break a lock if one is present from another instance.

        Uses the ui factory to ask for confirmation if the lock may be from
        an active process.

        This will probe the repository for its lock as well.
        """
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

    def transmit_svn_dir_deltas(self, file_id, editor):
        path = self.id2path(file_id)
        encoded_path = self.abspath(path).encoded("utf-8")
        root_adm = self._get_wc(write_lock=True)
        try:
            root_adm.transmit_prop_deltas(encoded_path, True, editor)
        finally:
            root_adm.close()

    def transmit_svn_file_deltas(self, file_id, editor):
        path = self.id2path(file_id)
        encoded_path = self.abspath(path).encode("utf-8")
        root_adm = self._get_wc(write_lock=True)
        try:
            adm = root_adm.probe_try(encoded_path, True, 1)
            entry = adm.entry(encoded_path)
            root_adm.transmit_prop_deltas(encoded_path, entry, editor)
            root_adm.transmit_text_deltas(encoded_path, True, editor)
        finally:
            root_adm.close()

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
        rev = self.branch.lookup_bzr_revision_id(new_revid)
        self._set_base(new_revid, rev)

        newrev = self.branch.repository.get_revision(new_revid)
        svn_revprops = self.branch.repository._log.revprop_list(rev)

        class DummyEditor(object):

            def apply_textdelta(self, checksum):
                def window_handler(window):
                    pass
                return window_handler

            def close(self):
                pass

        def update_entry(cq, path, root_adm, md5sum=None):
            mutter('updating entry for %s'% path)
            adm = root_adm.probe_try(self.abspath(path).encode("utf-8"), True, 1)
            cq.queue(self.abspath(path).rstrip("/").encode("utf-8"), adm,
                True, None, False, False, md5sum)

        cq = subvertpy.wc.CommittedQueue()
        root_adm = self._get_wc(self.abspath("."), write_lock=True, depth=-1)
        try:
            for (old_path, new_path, file_id, ie) in delta:
                if old_path is not None:
                    update_entry(cq, old_path, root_adm)
                if new_path is not None:
                    if ie.kind in ("symlink", "file"):
                        f = get_svn_file_contents(self, ie.kind, ie.file_id)
                        md5sum = osutils.md5(self.get_file_text(file_id)).digest()
                    else:
                        md5sum = None
                    update_entry(cq, new_path, root_adm, md5sum)
            root_adm.process_committed_queue(cq,
                rev, svn_revprops[properties.PROP_REVISION_DATE],
                svn_revprops[properties.PROP_REVISION_AUTHOR])
        finally:
           root_adm.close()
        self.set_parent_ids([new_revid])


class SvnWorkingTreeFormat(WorkingTreeFormat):
    """Subversion working copy format."""

    def __init__(self, version=None):
        self.version = version

    def __get_matchingbzrdir(self):
        return SvnWorkingTreeDirFormat()

    _matchingbzrdir = property(__get_matchingbzrdir)

    def get_format_description(self):
        if self.version is not None:
            return "Subversion Working Copy (version %d)" % self.version
        else:
            return "Subversion Working Copy"

    def get_format_string(self):
        raise NotImplementedError

    def initialize(self, a_bzrdir, revision_id=None):
        raise NotImplementedError(self.initialize)

    def open(self, a_bzrdir):
        raise NotImplementedError(self.initialize)


class SvnCheckout(ControlDir):
    """ControlDir implementation for Subversion checkouts (directories
    containing a .svn subdirectory."""

    @property
    def control_transport(self):
        return None

    @property
    def control_url(self):
        return urlutils.join(self.user_transport, get_adm_dir())

    @property
    def user_transport(self):
        return self._transport

    def __init__(self, transport, format):
        self._transport = transport
        self._format = format
        self._mode_check_done = False
        self.local_path = transport.local_abspath(".")

        # Open related remote repository + branch
        try:
            wc = WorkingCopy(None, self.local_path.encode("utf-8"))
        except subvertpy.SubversionException, (msg, num):
            if num == ERR_WC_UNSUPPORTED_FORMAT:
                raise UnsupportedFormatError(msg, kind='workingtree')
            else:
                raise
        try:
            try:
                self.entry = wc.entry(self.local_path.encode("utf-8"), True)
            except subvertpy.SubversionException, (msg, num):
                if num in (subvertpy.ERR_ENTRY_NOT_FOUND,
                           subvertpy.ERR_NODE_UNKNOWN_KIND):
                    raise CorruptWorkingTree(self.local_path.encode("utf-8"),
                        msg)
                else:
                    raise
        finally:
            wc.close()

        self._remote_branch_transport = None
        self._remote_repo_transport = None
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
        if self._remote_branch_transport is None:
            self._remote_branch_transport = SvnRaTransport(self.entry.url, from_transport=self._remote_repo_transport)
        return self._remote_branch_transport

    def clone(self, path, revision_id=None, force_new_repo=False):
        raise NotImplementedError(self.clone)

    def open_workingtree(self, _unsupported=False, recommend_upgrade=False):
        try:
            return SvnWorkingTree(self, self.local_path, self.entry)
        except NotSvnBranchPath, e:
            raise NoWorkingTree(self.local_path)

    def sprout(self, url, revision_id=None, force_new_repo=False,
               recurse='down', possible_transports=None, accelerator_tree=None,
               hardlink=False):
        # FIXME: honor force_new_repo
        # FIXME: Use recurse
        result = format_registry.make_bzrdir('default').initialize(url)
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
        if self.entry.repos is None:
            return self.get_remote_bzrdir().find_repository()
        if self._remote_repo_transport is None:
            try:
                self._remote_repo_transport = SvnRaTransport(self.entry.repos,
                    from_transport=self._remote_branch_transport)
            except subvertpy.SubversionException, (msg, num):
                if num == subvertpy.ERR_RA_LOCAL_REPOS_OPEN_FAILED:
                    raise LocalRepositoryOpenFailed(self.entry.repos)
                else:
                    raise
        return SvnRepository(self, self._remote_repo_transport,
                self.get_branch_path())

    def get_branch_path(self):
        if self.entry.repos is None:
            raise RepositoryRootUnknown()
        assert self.entry.url.startswith(self.entry.repos)
        return self.entry.url[len(self.entry.repos):].strip("/")

    def needs_format_conversion(self, format=None):
        if format is None:
            format = ControlDirFormat.get_default_format()
        return not isinstance(self._format, format.__class__)

    def get_workingtree_transport(self, format):
        assert format is None
        return get_transport(self.svn_controldir)

    def create_workingtree(self, revision_id=None, hardlink=None):
        """See ControlDir.create_workingtree().

        Not implemented for Subversion because having a .svn directory
        implies having a working copy.
        """
        raise NotImplementedError(self.create_workingtree)

    def create_branch(self):
        """See ControlDir.create_branch()."""
        raise NotImplementedError(self.create_branch)

    if bzrlib_version >= (2, 2):
        def open_branch(self, name=None, unsupported=False, 
            ignore_fallbacks=None):
            return self._open_branch(name=name,
                ignore_fallbacks=ignore_fallbacks, unsupported=unsupported)
    else:
        def open_branch(self, ignore_fallbacks=None, unsupported=False):
            return self._open_branch(name=None,
                ignore_fallbacks=ignore_fallbacks, unsupported=unsupported)

    def _open_branch(self, name=None, unsupported=True, ignore_fallbacks=False):
        """See ControlDir.open_branch()."""
        repos = self._find_repository()

        try:
            branch = SvnBranch(repos, self.get_branch_path())
        except RepositoryRootUnknown:
            return self.get_remote_bzrdir().open_branch()
        except subvertpy.SubversionException, (_, num):
            if num == subvertpy.ERR_WC_NOT_DIRECTORY:
                raise NotBranchError(path=self.base)
            raise

        branch.bzrdir = self.get_remote_bzrdir()

        return branch

    def _find_creation_modes(self):
        """Determine the appropriate modes for files and directories.

        They're always set to be consistent with the base directory,
        assuming that this transport allows setting modes.
        """
        # TODO: Do we need or want an option (maybe a config setting) to turn
        # this off or override it for particular locations? -- mbp 20080512
        if self._mode_check_done:
            return
        self._mode_check_done = True
        try:
            st = self.transport.stat('.')
        except TransportNotPossible:
            self._dir_mode = None
            self._file_mode = None
        else:
            # Check the directory mode, but also make sure the created
            # directories and files are read-write for this user. This is
            # mostly a workaround for filesystems which lie about being able to
            # write to a directory (cygwin & win32)
            if (st.st_mode & 07777 == 00000):
                # FTP allows stat but does not return dir/file modes
                self._dir_mode = None
                self._file_mode = None
            else:
                self._dir_mode = (st.st_mode & 07777) | 00700
                # Remove the sticky and execute bits for files
                self._file_mode = self._dir_mode & ~07111

    def _get_file_mode(self):
        """Return Unix mode for newly created files, or None.
        """
        if not self._mode_check_done:
            self._find_creation_modes()
        return self._file_mode

    def _get_dir_mode(self):
        """Return Unix mode for newly created directories, or None.
        """
        if not self._mode_check_done:
            self._find_creation_modes()
        return self._dir_mode

    def get_config(self):
        from bzrlib.plugins.svn.config import SubversionControlDirConfig
        return SubversionControlDirConfig()


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
