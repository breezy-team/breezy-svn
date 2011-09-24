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

from collections import (
    defaultdict,
    deque,
    )

import os
import stat
import subvertpy

from subvertpy import (
    ERR_WC_UNSUPPORTED_FORMAT,
    properties,
    )
from subvertpy.wc import (
    SCHEDULE_ADD,
    SCHEDULE_DELETE,
    SCHEDULE_NORMAL,
    SCHEDULE_REPLACE,
    CommittedQueue,
    WorkingCopy,
    cleanup,
    get_adm_dir,
    is_adm_dir,
    revision_status,
    )

import bzrlib.add
from bzrlib import (
    conflicts as _mod_conflicts,
    hashcache,
    osutils,
    transport as _mod_transport,
    urlutils,
    )
from bzrlib.errors import (
    BadFilenameEncoding,
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
    InventoryDirectory,
    InventoryFile,
    InventoryLink,
    )
from bzrlib.lockable_files import (
    LockableFiles,
    TransportLock,
    )
from bzrlib.lockdir import LockDir
from bzrlib.revision import (
    CURRENT_REVISION,
    NULL_REVISION,
    )
from bzrlib.trace import (
    mutter,
    note,
    )
from bzrlib.transport import location_to_url
from bzrlib.workingtree import (
    WorkingTree,
    WorkingTreeFormat,
    )

from bzrlib.plugins.svn import (
    SvnWorkingTreeProber,
    svk,
    )
from bzrlib.plugins.svn.commit import (
    _revision_id_to_svk_feature,
    )
from bzrlib.plugins.svn.errors import (
    convert_svn_error,
    NotSvnBranchPath,
    NoSvnRepositoryPresent,
    )
from bzrlib.plugins.svn.mapping import (
    escape_svn_path,
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

# Compatibility with bzr < 2.5
get_transport_from_path = getattr(_mod_transport, "get_transport_from_path",
        _mod_transport.get_transport)

# Compatibility with subvertpy < 0.8.6
ERR_BAD_FILENAME = getattr(subvertpy, "ERR_BAD_FILENAME", 125001)


try:
    from bzrlib.controldir import Converter
except ImportError:
    from bzrlib.bzrdir import Converter
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


def update_wc(adm, basedir, conn, url, revnum):
    # FIXME: honor SVN_CONFIG_SECTION_HELPERS:SVN_CONFIG_OPTION_DIFF3_CMD
    # FIXME: honor SVN_CONFIG_SECTION_MISCELLANY:SVN_CONFIG_OPTION_USE_COMMIT_TIMES
    # FIXME: honor SVN_CONFIG_SECTION_MISCELLANY:SVN_CONFIG_OPTION_PRESERVED_CF_EXTS
    editor = adm.get_update_editor(basedir, False, True)
    assert editor is not None
    reporter = conn.do_switch(revnum, "", True, url, editor)
    try:
        adm.crawl_revisions(basedir, reporter, restore_files=False,
                            recurse=True, use_commit_times=False)
    except subvertpy.SubversionException, (msg, num):
        if num == subvertpy.ERR_RA_ILLEGAL_URL:
            raise BzrError(msg)
        raise
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


class Walker(object):

    def __init__(self, workingtree, start=u"", recursive=True):
        self.workingtree = workingtree
        self.todo = list([start])
        self.pending = deque()
        self.recursive = recursive

    def __iter__(self):
        return iter(self.next, None)

    def next(self):
        while not self.pending:
            try:
                p = self.todo.pop()
            except IndexError:
                return None
            try:
                wc = self.workingtree._get_wc(p)
            except subvertpy.SubversionException, (_, num):
                if num == subvertpy.ERR_WC_NOT_DIRECTORY:
                    continue
                raise
            try:
                for name, entry in wc.entries_read(True).iteritems():
                    subp = osutils.pathjoin(p, name.decode("utf-8")).rstrip("/")
                    if entry.kind == subvertpy.NODE_DIR and name != "":
                        if self.recursive:
                            self.todo.append(subp)
                    else:
                        self.pending.append((subp, entry))
            finally:
                wc.close()
        return self.pending.popleft()


class SvnWorkingTree(SubversionTree, WorkingTree):
    """WorkingTree implementation that uses a svn working copy for storage."""

    def __init__(self, bzrdir, format, local_path, entry):
        assert isinstance(local_path, unicode)
        self.entry = entry
        self.basedir = local_path
        self._format = format
        self.bzrdir = bzrdir
        self._branch = None
        self.mapping = self.branch.mapping
        self._reset_data()
        self._cached_base_tree = None
        self._transport = bzrdir.get_workingtree_transport(None)
        self._detect_case_handling()
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

    def kind(self, file_id, path=None):
        if path is not None:
            return osutils.file_kind(self.abspath(path))
        else:
            return osutils.file_kind(self.id2abspath(file_id))

    def _detect_case_handling(self):
        try:
            self._transport.stat(".svn/FoRmAt")
        except NoSuchFile:
            self.case_sensitive = True
        else:
            self.case_sensitive = False

    def _set_root_id(self, file_id):
        self._change_fileid_mapping(file_id, u"")

    def get_file_mtime(self, file_id, path=None):
        """See Tree.get_file_mtime."""
        if not path:
            path = self.id2path(file_id)
        return os.lstat(self.abspath(path)).st_mtime

    def _setup_directory_is_tree_reference(self):
        self._directory_is_tree_reference = self._directory_is_never_tree_reference

    def get_file_sha1(self, file_id, path=None, stat_value=None):
        if not path:
            path = self.id2path(file_id)
        return self._hashcache.get_sha1(path, stat_value)

    @property
    def basis_idmap(self):
        if self._cached_base_idmap is not None:
            return self._cached_base_idmap
        idmap = self.branch.repository.get_fileid_map(
                    self._get_base_revmeta(),
                    self.mapping)
        if self.is_locked():
            self._cached_base_idmap = idmap
        return idmap

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

    def conflicts(self):
        # FIXME: Retrieve conflicts
        return _mod_conflicts.ConflictList()

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
                    dir_add(subwc, subprefix, urlutils.joinpath(patprefix,
                        entry))
                finally:
                    subwc.close()

        wc = self._get_wc()
        try:
            dir_add(wc, u"", ".")
        finally:
            wc.close()

        return ignores

    def flush(self):
        pass

    def is_control_filename(self, path):
        """Check whether path is a control file (used by bzr or svn)."""
        return is_adm_dir(path)

    def _update(self, branch_path, revnum):
        if revnum is None:
            # FIXME: should be able to use -1 here
            revnum = self.branch.get_revnum()
        old_branch_path = self.get_branch_path()
        adm = self._get_wc(write_lock=True, depth=-1)
        try:
            conn = self.branch.repository.transport.get_connection(old_branch_path)
            try:
                update_wc(adm, self.basedir.encode("utf-8"), conn, 
                        urlutils.join(self.branch.repository.transport.svn_url, branch_path),
                        revnum)
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
            ((uuid, branch_path, revnum), mapping) = self.branch.lookup_bzr_revision_id(revision)
        else:
            branch_path = self.branch.get_branch_path()
        self._cached_base_revnum = self._update(branch_path, revnum)
        self._cached_base_revid = None
        self._cached_base_idmap = None
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
                try:
                    wc.delete(osutils.safe_utf8(self.abspath(file)),
                              keep_local=keep_files)
                except subvertpy.SubversionException, (msg, num):
                    if num == ERR_BAD_FILENAME:
                        note("%s does not exist." % file)
                    else:
                        raise
        finally:
            wc.close()

        for file in files:
            self._change_fileid_mapping(None, file)

    def unversion(self, file_ids):
        wc = self._get_wc(write_lock=True)
        try:
            for file_id in file_ids:
                path = self.id2path(file_id)
                wc.delete(osutils.safe_utf8(self.abspath(path)),
                          keep_local=True)
        finally:
            wc.close()

    def all_file_ids(self):
        """See Tree.all_file_ids"""
        ret = set()
        w = Walker(self)
        for path, entry in w:
            ret.add(self.lookup_id(path))
        return ret

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

    def _rename_fileid(self, old_path, new_path, wc=None):
        self._change_fileid_mapping(self.path2id(old_path), new_path, wc)
        self._change_fileid_mapping(None, old_path, wc)
        if not os.path.isdir(self.abspath(old_path)):
            return
        for from_subpath, entry in Walker(self, old_path):
            from_subpath = from_subpath.strip("/")
            to_subpath = osutils.pathjoin(new_path,
                from_subpath[len(old_path):].strip("/"))
            self._change_fileid_mapping(self.path2id(from_subpath), to_subpath,
                    wc)
            self._change_fileid_mapping(None, from_subpath, wc)

    def move(self, from_paths, to_dir=None, after=False, **kwargs):
        """Move files to a new location."""
        # FIXME: Use after argument
        if after:
            raise NotImplementedError("move after not supported")
        for from_path in from_paths:
            from_abspath = osutils.safe_utf8(self.abspath(from_path))
            new_path = osutils.pathjoin(
                osutils.safe_utf8(to_dir),
                osutils.safe_utf8(os.path.basename(from_path)))
            self._rename_fileid(from_path, new_path)
            to_wc = self._get_wc(osutils.safe_unicode(to_dir), write_lock=True)
            try:
                to_wc.copy(from_abspath,
                    osutils.safe_utf8(os.path.basename(from_path)))
            finally:
                to_wc.close()
            from_wc = self._get_wc(write_lock=True)
            try:
                from_wc.delete(from_abspath)
            finally:
                from_wc.close()

    def rename_one(self, from_rel, to_rel, after=False):
        from_rel = osutils.safe_unicode(from_rel)
        to_rel = osutils.safe_unicode(to_rel)
        # FIXME: Use after
        if after:
            raise NotImplementedError("rename_one after not supported")
        from_wc = None
        from_id = self.path2id(from_rel)
        self._rename_fileid(from_rel, to_rel)
        (to_wc, to_file) = self._get_rel_wc(to_rel, write_lock=True)
        try:
            if os.path.dirname(from_rel) == os.path.dirname(to_rel):
                # Prevent lock contention
                from_wc = to_wc
            else:
                (from_wc, _) = self._get_rel_wc(from_rel, write_lock=True)
            try:
                to_wc.copy(self.abspath(from_rel), to_file)
                from_wc.delete(self.abspath(from_rel))
            finally:
                from_wc.close()
        finally:
            if from_wc != to_wc:
                to_wc.close()

    def path_to_file_id(self, revnum, current_revnum, path):
        """Generate a bzr file id from a Subversion file name.

        :param revnum: Revision number.
        :param path: Path of the file
        :return: Tuple with file id and revision id.
        """
        assert type(path) == unicode
        path = osutils.normpath(path)
        if path == u".":
            path = u""
        return self.lookup_id(path)

    def _find_ids(self, relpath, entry):
        assert type(relpath) == unicode
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
            ids = self._get_new_file_ids()
            if ids.has_key(relpath):
                return (ids[relpath], None)
            # FIXME: Generate more random but consistent file ids
            return ("NEW-" + escape_svn_path(osutils.safe_utf8(relpath).strip("/")),
                    None)

    def get_root_id(self):
        return self.path2id("")

    def path2id(self, path):
        wc = self._get_wc()
        try:
            try:
                entry = self._get_entry(wc, osutils.safe_utf8(self.abspath(path)))
                (file_id, revision) = self._find_ids(osutils.safe_unicode(path), entry)
            except KeyError:
                return None
            else:
                return file_id
        finally:
            wc.close()

    def _get_entry(self, wc, path):
        assert type(path) == str
        related_wc = wc.probe_try(path)
        if related_wc is None:
            raise KeyError
        return related_wc.entry(path)

    def filter_unversioned_files(self, paths):
        ret = set()
        wc = self._get_wc()
        try:
            for p in paths:
                try:
                    entry = self._get_entry(wc, self.abspath(p).encode("utf-8"))
                except KeyError:
                    ret.add(p)
                else:
                    if entry.schedule == SCHEDULE_DELETE:
                        ret.add(p)
        finally:
            wc.close()
        return ret

    def has_or_had_id(self, file_id):
        if self.has_id(file_id):
            return True
        if self.basis_tree().has_id(file_id):
            return True
        return False

    def has_id(self, file_id):
        try:
            self.id2path(file_id)
        except NoSuchId:
            return False
        else:
            return True

    def id2path(self, file_id):
        ids = self._get_new_file_ids()
        for path, fid in ids.iteritems():
            if file_id == fid:
                return path
        try:
            return self.basis_idmap.reverse_lookup(self.mapping, file_id)
        except KeyError:
            pass
        if file_id.startswith("NEW-"):
            return urlutils.unescape(file_id[4:])
        raise NoSuchId(self, file_id)

    def _ie_from_entry(self, relpath, entry, parent_id):
        assert type(relpath) == unicode
        (file_id, revid) = self._find_ids(relpath, entry)
        abspath = self.abspath(relpath)
        if entry.kind == subvertpy.NODE_DIR:
            ie = InventoryDirectory(file_id, os.path.basename(relpath),
                    parent_id)
            ie.revision = revid
            return ie
        elif os.path.islink(abspath):
            ie = InventoryLink(file_id, os.path.basename(relpath), parent_id)
            ie.revision = revid
            target_path = os.readlink(abspath.encode(osutils._fs_enc))
            ie.symlink_target = target_path.decode(osutils._fs_enc)
            return ie
        else:
            ie = InventoryFile(file_id, os.path.basename(relpath), parent_id)
            ie.revision = revid
            try:
                data = osutils.fingerprint_file(
                    open(abspath.encode(osutils._fs_enc)))
            except IOError:
                # Ignore non-existing files
                return None
            else:
                ie.text_sha1 = data['sha1']
                ie.text_size = data['size']
                ie.executable = self.is_executable(id, relpath)
                return ie

    def iter_entries_by_dir(self, specific_file_ids=None, yield_parents=False):
        if specific_file_ids:
            wc = self._get_wc()
            try:
                for file_id in specific_file_ids:
                    path = self.id2path(file_id)
                    parent = os.path.dirname(path)
                    parent_id = self.lookup_id(parent)
                    entry = self._get_entry(wc,
                            osutils.safe_utf8(self.abspath(path)))
                    ie = self._ie_from_entry(path, entry, parent_id)
                    if ie is not None:
                        yield path, ie
            finally:
                wc.close()
        else:
            fileids = {}
            w = Walker(self)
            for relpath, entry in w:
                if entry.schedule == SCHEDULE_DELETE:
                    continue
                assert isinstance(relpath, unicode)
                if relpath == u"":
                    parent_id = None
                else:
                    parent_id = fileids[os.path.dirname(relpath)]
                ie = self._ie_from_entry(relpath, entry, parent_id)
                if ie is not None:
                    fileids[relpath] = ie.file_id
                    yield relpath, ie

    def extras(self):
        """Return the extras."""
        w = Walker(self)
        versioned_files = set()
        all_files = set()
        for path, dir_entry in w:
            versioned_files.add(path)
            dirabs = self.abspath(path)
            if not osutils.isdir(dirabs):
                # e.g. directory deleted
                continue
            for subf in os.listdir(dirabs):
                if self.bzrdir.is_control_filename(subf):
                    continue
                try:
                    (subf_norm, can_access) = osutils.normalized_filename(subf)
                except UnicodeDecodeError:
                    path_os_enc = path.encode(osutils._fs_enc)
                    relpath = path_os_enc + '/' + subf
                    raise BadFilenameEncoding(relpath, osutils._fs_enc)
                subp = os.path.join(path, subf_norm)
                all_files.add(subp)
        return iter(all_files - versioned_files)

    def merge_modified(self):
        return {}

    def set_last_revision(self, revid):
        mutter('setting last revision to %r', revid)
        (foreign_revid, mapping) = self.branch.lookup_bzr_revision_id(revid)
        self._cached_base_revnum = foreign_revid[2]
        self._cached_base_revid = revid
        self._cached_base_idmap = None

    def set_parent_trees(self, parents_list, allow_leftmost_as_ghost=False):
        """See MutableTree.set_parent_trees."""
        self.set_parent_ids([rev for (rev, tree) in parents_list])

    def set_parent_ids(self, parent_ids, allow_leftmost_as_ghost=False):
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
                    svk.SVN_PROP_SVK_MERGE,
                    svk.serialize_svk_features(svk_merges),
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
                if self.filter_unversioned_files([f]):
                    if save:
                        mutter('adding %r', file_path)
                        encoded_file_path = file_path.encode("utf-8")
                        wc.add(encoded_file_path)
                        self._update_special(wc, encoded_file_path, f)
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

    def _update_special(self, adm, abspath, relpath):
        kind = self.kind(None, relpath)
        if kind == "file":
            value = None
        elif kind == "symlink":
            value = properties.PROP_SPECIAL_VALUE
        else:
            return
        adm.prop_set(properties.PROP_SPECIAL, value, abspath)

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
                self._update_special(wc, utf8_abspath, f)
            finally:
                wc.close()
            if file_id is not None:
                self._change_fileid_mapping(file_id, f)

    def basis_tree(self):
        """Return the basis tree for a working tree."""
        try:
            return SvnBasisTree(self)
        except BasisTreeIncomplete:
            return self.branch.basis_tree()

    def list_files(self, include_root=False, from_dir=None, recursive=True):
        """See `Tree.list_files`."""
        # TODO: This doesn't sort the output
        # TODO: This ignores unversioned files at the moment
        if from_dir is None:
            from_dir = u""
        else:
            from_dir = osutils.safe_unicode(from_dir)
        w = Walker(self, from_dir, recursive=recursive)
        fileids = {}
        for path, entry in w:
            relpath = path.decode("utf-8")
            if entry.schedule in (SCHEDULE_NORMAL, SCHEDULE_ADD,
                    SCHEDULE_REPLACE):
                versioned = 'V'
            else:
                versioned = '?'
            if relpath == u"":
                parent_id = None
            else:
                parent_path = os.path.dirname(relpath)
                try:
                    parent_id = fileids[parent_path]
                except KeyError:
                    parent_id = self.path2id(parent_path)
                    fileids[parent_path] = parent_id
            ie = self._ie_from_entry(relpath, entry, parent_id)
            if ie is not None:
                fileids[relpath] = ie.file_id
            if include_root or relpath != u"":
                yield relpath, versioned, ie.kind, ie.file_id, ie

    def revision_tree(self, revid):
        return self.branch.repository.revision_tree(revid)

    def unprefix(self, relpath):
        """Remove the branch path from a relpath.

        :param relpath: path from the repository root.
        """
        assert relpath.startswith(self.get_branch_path()), \
                "expected %s prefix, got %s" % (self.get_branch_path(), relpath)
        return relpath[len(self.get_branch_path()):].strip("/")

    def pull(self, source, overwrite=False, stop_revision=None,
             delta_reporter=None, possible_transports=None, local=False):
        """Pull in changes from another branch into this working tree."""
        # FIXME: Use delta_reporter
        result = self.branch.pull(source, overwrite=overwrite,
            stop_revision=stop_revision, local=local)
        fetched = self._update(self.branch.get_branch_path(), self.branch.get_revnum())
        self._cached_base_revnum = fetched
        self._cached_base_revid = None
        self._cached_base_idmap = None
        return result

    def get_file_properties(self, file_id, path=None):
        if path is None:
            path = self.id2path(file_id)
        else:
            path = osutils.safe_unicode(path)
        abspath = self.abspath(path)
        if not os.path.isdir(abspath.encode(osutils._fs_enc)):
            wc = self._get_wc(urlutils.split(path)[0])
        else:
            wc = self._get_wc(path)
        try:
            (prop_changes, orig_props) = wc.get_prop_diffs(
                abspath.encode("utf-8"))
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
        try:
            return self.basis_idmap.lookup(self.mapping, path)[:2]
        except KeyError:
            mutter("fileid map: %r", self._get_new_file_ids())
            raise

    def _get_changed_branch_props(self):
        wc = self._get_wc()
        try:
            ret = {}
            (prop_changes, orig_props) = wc.get_prop_diffs(
                self.basedir.encode("utf-8"))
            for k,v in prop_changes:
                ret[k] = (orig_props.get(k), v)
            return ret
        finally:
            wc.close()

    def _get_branch_props(self):
        return self.get_file_properties(None, "")

    def _set_branch_props(self, wc, fileprops):
        for k, v in fileprops.iteritems():
            wc.prop_set(k, v, self.basedir.encode("utf-8"))

    def _get_base_branch_props(self):
        wc = self._get_wc()
        try:
            (prop_changes, orig_props) = wc.get_prop_diffs(
                self.basedir.encode("utf-8"))
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
            base_tree.kind(file_id) == ie.kind)
        if old_path is not None:
            old_abspath = osutils.safe_utf8(self.abspath(old_path))
            if not already_there:
                self.remove([old_path], keep_files=True)
            copyfrom = (urlutils.join(self.entry.url, old_path),
                        self.base_revnum)
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

    @property
    def base_revnum(self):
        if self._cached_base_revnum is not None:
            return self._cached_base_revnum
        max_rev = revision_status(self.basedir.encode("utf-8"), None, True)[1]
        if self.is_locked():
            self._cached_base_revnum = max_rev
        return max_rev

    def _last_revision(self):
        if self._cached_base_revid is not None:
            return self._cached_base_revid
        revid = self.branch.generate_revision_id(self.base_revnum)
        if self.is_locked():
            self._cached_base_revid = revid
        return revid

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
            target = os.readlink(abspath.encode(osutils._fs_enc))
            return ('symlink', None, None, target.decode(osutils._fs_enc))
        else:
            return (kind, None, None, None)

    def _get_base_revmeta(self):
        return self.branch.repository._revmeta_provider.lookup_revision(
            self.get_branch_path(self.base_revnum), self.base_revnum)

    def _reset_data(self):
        self._cached_base_revnum = None
        self._cached_base_idmap = None
        self._cached_base_revid = None

    def break_lock(self):
        """Break a lock if one is present from another instance.

        Uses the ui factory to ask for confirmation if the lock may be from
        an active process.

        This will probe the repository for its lock as well.
        """
        cleanup(self.basedir.encode("utf-8"))
        self._control_files.break_lock()

    def unlock(self):
        if self._control_files._lock_count == 1:
            # non-implementation specific cleanup
            self._cleanup()
            self._reset_data()

        # reverse order of locking.
        try:
            return self._control_files.unlock()
        finally:
            self.branch.unlock()

    def _is_executable_from_path_and_stat_from_stat(self, path, stat_result):
        mode = stat_result.st_mode
        return bool(stat.S_ISREG(mode) and stat.S_IEXEC & mode)

    if not osutils.supports_executable():
        def is_executable(self, file_id, path=None):
            basis_tree = self.basis_tree()
            if file_id in basis_tree:
                return basis_tree.is_executable(file_id)
            # Default to not executable
            return False
    else:
        def is_executable(self, file_id, path=None):
            if not path:
                path = self.id2path(file_id)
            mode = os.lstat(self.abspath(path)).st_mode
            return bool(stat.S_ISREG(mode) and stat.S_IEXEC & mode)

        _is_executable_from_path_and_stat = \
            _is_executable_from_path_and_stat_from_stat

    def transmit_svn_dir_deltas(self, file_id, editor):
        path = self.id2path(file_id)
        encoded_path = self.abspath(path).encoded("utf-8")
        root_adm = self._get_wc(write_lock=True)
        try:
            root_adm.transmit_prop_deltas(encoded_path, True, editor)
        finally:
            root_adm.close()

    def transmit_svn_file_deltas(self, file_id, path, editor):
        if path is None:
            path = self.id2path(file_id)
        encoded_path = self.abspath(path).encode("utf-8")
        root_adm = self._get_wc(write_lock=True)
        try:
            adm = root_adm.probe_try(encoded_path, True, 1)
            try:
                entry = adm.entry(encoded_path)
                root_adm.transmit_prop_deltas(encoded_path, entry, editor)
                self._update_special(adm, encoded_path, path)
                root_adm.transmit_text_deltas(encoded_path, True, editor)
            finally:
                adm.close()
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
        revmeta, mapping = self.branch.last_revmeta(True)
        assert revmeta.get_revision_id(mapping) == new_revid
        rev = revmeta.metarev.revnum
        self._cached_base_revnum = rev
        self._cached_base_revid = new_revid
        self._cached_base_idmap = None

        newrev = self.branch.repository.get_revision(new_revid)
        svn_revprops = revmeta.metarev.revprops

        class DummyEditor(object):

            def apply_textdelta(self, checksum):
                def window_handler(window):
                    pass
                return window_handler

            def close(self):
                pass

        adms_to_close = set()
        def update_entry(cq, path, root_adm, md5sum=None):
            mutter('updating entry for %s', path)
            adm = root_adm.probe_try(
                self.abspath(path).encode("utf-8"), True, 1)
            cq.queue(self.abspath(path).rstrip("/").encode("utf-8"), adm,
                True, None, False, False, md5sum)
            adms_to_close.add(adm)

        cq = CommittedQueue()
        root_adm = self._get_wc(self.abspath("."), write_lock=True, depth=-1)
        adms_to_close.add(root_adm)
        try:
            for repos_path, changes in revmeta.metarev.paths.iteritems():
                if changes[0] == 'D':
                    continue
                assert changes[0] in ('A', 'M', 'R')
                path = repos_path[len(revmeta.metarev.branch_path):].strip("/")
                path = path.decode("utf-8")
                try:
                    kind = self.kind(None, path)
                except NoSuchFile:
                    md5sum = None
                else:
                    if kind == "file":
                        md5sum = osutils.md5(self.get_file_text(None, path)).digest()
                    else:
                        md5sum = None
                update_entry(cq, path, root_adm, md5sum)
            root_adm.process_committed_queue(cq,
                rev, svn_revprops[properties.PROP_REVISION_DATE],
                svn_revprops[properties.PROP_REVISION_AUTHOR])
        finally:
            for adm in adms_to_close:
                adm.close()
        self.set_parent_ids([new_revid])

    def annotate_iter(self, file_id, default_revision=CURRENT_REVISION):
        from bzrlib.plugins.svn.annotate import Annotater
        annotater = Annotater(self.branch.repository, file_id)
        for revid in self.get_parent_ids():
            foreign_revid, mapping = self.branch.repository.lookup_bzr_revision_id(
                revid)
            annotater.check_file_revs(revid, foreign_revid[1], foreign_revid[2],
                self.mapping, None)
        annotater.check_file(self.get_file_lines(file_id, self.id2path(file_id)),
            default_revision)
        return annotater.get_annotated()


class SvnWorkingTreeFormat(WorkingTreeFormat):
    """Subversion working copy format."""

    supports_versioned_directories = True

    def __init__(self, version=None):
        self.version = version

    def __get_matchingbzrdir(self):
        return SvnWorkingTreeDirFormat(self.version)

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

    def get_controldir_for_branch(self):
        from bzrlib.plugins.svn.remote import SvnRemoteFormat
        return SvnRemoteFormat()


class SvnWorkingTreeDirFormat(ControlDirFormat):
    """Working Tree implementation that uses Subversion working copies."""

    _lock_class = TransportLock

    def __init__(self, version=None):
        super(SvnWorkingTreeDirFormat, self).__init__()
        self.version = version

    def open(self, transport, _found=False):
        import subvertpy
        try:
            return SvnCheckout(transport, self)
        except subvertpy.SubversionException, (_, num):
            if num in (subvertpy.ERR_RA_LOCAL_REPOS_OPEN_FAILED,):
                raise NoSvnRepositoryPresent(transport.base)
            raise

    def get_format_string(self):
        raise NotImplementedError(self.get_format_string)

    def get_format_description(self):
        return 'Subversion Local Checkout'

    def is_initializable(self):
        return False

    def is_supported(self):
        return True

    def initialize_on_transport(self, transport):
        raise UninitializableFormat(self)

    def initialize_on_transport_ex(self, transport, use_existing_dir=False,
        create_prefix=False, force_new_repo=False, stacked_on=None,
        stack_on_pwd=None, repo_format_name=None, make_working_trees=None,
        shared_repo=False, vfs_only=False):
        raise UninitializableFormat(self)

    def get_converter(self, format):
        """See ControlDirFormat.get_converter()."""
        return SvnCheckoutConverter(format)

    @property
    def repository_format(self):
        from bzrlib.plugins.svn.repository import SvnRepositoryFormat
        return SvnRepositoryFormat()

    def get_branch_format(self):
        from bzrlib.plugins.svn.branch import SvnBranchFormat
        return SvnBranchFormat()


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
        self._config = None
        if transport.base.startswith("readonly+"):
            real_transport = transport._decorated
        else:
            real_transport = transport
        SvnWorkingTreeProber().probe_transport(real_transport)
        self.local_path = real_transport.local_abspath(".")

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

        self.svn_root_url = self.entry.repos
        self.svn_url = self.entry.url
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
        from bzrlib.plugins.svn.remote import SvnRemoteAccess
        if self._remote_bzrdir is None:
            self._remote_bzrdir = SvnRemoteAccess(self.get_remote_transport())
        return self._remote_bzrdir

    def get_remote_transport(self):
        if self._remote_branch_transport is None:
            self._remote_branch_transport = SvnRaTransport(self.entry.url,
                from_transport=self._remote_repo_transport)
        return self._remote_branch_transport

    def clone(self, location, revision_id=None, force_new_repo=False,
              preserve_stacking=False):
        wt = self.open_workingtree()
        if revision_id is None:
            revision_id = wt.last_revision()
        url = location_to_url(location)
        path = urlutils.local_path_from_url(url)
        return wt.branch.create_checkout(path, lightweight=True,
            revision_id=revision_id).bzrdir

    def open_workingtree(self, _unsupported=False, recommend_upgrade=False):
        try:
            return SvnWorkingTree(self,
                    SvnWorkingTreeFormat(self._format.version),
                    self.local_path, self.entry)
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
        return self.get_remote_bzrdir().find_repository()

    def get_branch_path(self):
        if self.entry.repos is None:
            raise RepositoryRootUnknown()
        assert self.entry.url.startswith(self.entry.repos)
        return self.entry.url[len(self.entry.repos):].strip("/")

    def needs_format_conversion(self, format):
        return not isinstance(self._format, format.__class__)

    def get_workingtree_transport(self, format):
        assert format is None
        return get_transport_from_path(self.svn_controldir)

    def create_workingtree(self, revision_id=None, hardlink=None):
        """See ControlDir.create_workingtree().

        Not implemented for Subversion because having a .svn directory
        implies having a working copy.
        """
        raise NotImplementedError(self.create_workingtree)

    def create_branch(self):
        """See ControlDir.create_branch()."""
        raise NotImplementedError(self.create_branch)

    def open_branch(self, name=None, unsupported=True, ignore_fallbacks=False,
            mapping=None):
        """See ControlDir.open_branch()."""
        from bzrlib.plugins.svn.branch import SvnBranch
        repos = self._find_repository()
        if mapping is None:
            mapping = repos.get_mapping()

        try:
            return SvnBranch(repos, self.get_remote_bzrdir(),
                self.get_branch_path(), mapping)
        except RepositoryRootUnknown:
            return self.get_remote_bzrdir().open_branch()
        except subvertpy.SubversionException, (_, num):
            if num == subvertpy.ERR_WC_NOT_DIRECTORY:
                raise NotBranchError(path=self.base)
            raise

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
        from bzrlib.plugins.svn.config import SvnRepositoryConfig
        if self._config is None:
            self._config = SvnRepositoryConfig(self.entry.url, self.entry.uuid)
        return self._config


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
        from bzrlib.branch import BranchReferenceFormat
        remote_branch = to_convert.open_branch()
        bzrdir = self.target_format.initialize(to_convert.root_transport.base)
        branch = BranchReferenceFormat().initialize(bzrdir, remote_branch)
        wt = bzrdir.create_workingtree()
        # FIXME: Convert working tree
        to_convert.root_transport.delete_tree(".svn")
        return bzrdir
