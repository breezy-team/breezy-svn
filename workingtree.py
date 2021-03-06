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

from __future__ import absolute_import

from bisect import bisect_left
from collections import (
    defaultdict,
    deque,
    )

import errno
import os
import operator
import posixpath
from six import string_types
import stat
import subvertpy

from subvertpy import (
    ERR_BAD_FILENAME,
    ERR_WC_UNSUPPORTED_FORMAT,
    ERR_WC_NODE_KIND_CHANGE,
    properties,
    )
from subvertpy.ra import (
    DEPTH_INFINITY,
    )
from subvertpy.wc import (
    SCHEDULE_ADD,
    SCHEDULE_DELETE,
    SCHEDULE_NORMAL,
    SCHEDULE_REPLACE,
    CommittedQueue,
    Adm,
    cleanup,
    get_adm_dir,
    is_adm_dir,
    match_ignore_list,
    revision_status,
    )

import breezy.add
from breezy import (
    conflicts as _mod_conflicts,
    errors as bzr_errors,
    hashcache,
    location as _mod_location,
    osutils,
    rio as _mod_rio,
    transport as _mod_transport,
    urlutils,
    )
from breezy.branch import BranchWriteLockResult
from breezy.errors import (
    BadFilenameEncoding,
    BzrError,
    MergeModifiedFormatError,
    NotBranchError,
    NoSuchFile,
    NoSuchId,
    NoSuchRevision,
    NoRepositoryPresent,
    NoWorkingTree,
    ReadOnlyError,
    TokenLockingNotSupported,
    TransportNotPossible,
    UnsupportedFormatError,
    UnsupportedOperation,
    UninitializableFormat,
    )
from breezy.lock import (
    LogicalLockResult,
    )
from breezy.lockable_files import (
    TransportLock,
    )
from breezy.revision import (
    CURRENT_REVISION,
    NULL_REVISION,
    )
from six import (
    text_type,
    )
from breezy.trace import (
    mutter,
    note,
    )
from breezy.bzr.workingtree import (
    MERGE_MODIFIED_HEADER_1,
    )
from breezy.workingtree import (
    WorkingTree,
    WorkingTreeFormat,
    )

from . import (
    SvnWorkingTreeProber,
    svk,
    )
from .commit import (
    _revision_id_to_svk_feature,
    )
from .errors import (
    convert_svn_error,
    NotSvnBranchPath,
    NoSvnRepositoryPresent,
    )
from .mapping import (
    escape_svn_path,
    )
from .transport import (
    SvnRaTransport,
    svn_config,
    )
from .tree import (
    BasisTreeIncomplete,
    SvnBasisTree,
    SubversionTree,
    SubversionTreeDirectory,
    SubversionTreeLink,
    SubversionTreeFile,
    )

from breezy.controldir import Converter
from breezy.controldir import (
    ControlDirFormat,
    ControlDir,
    )


class RepositoryRootUnknown(BzrError):
    _fmt = ("The working tree does not store the root of the Subversion "
            "repository.")


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
    print(basedir)
    editor = adm.get_switch_editor(
            "", url, use_commit_times=False,
            depth=DEPTH_INFINITY, notify_func=None, diff3_cmd=None,
            depth_is_sticky=False, allow_unver_obstructions=True)
    reporter = conn.do_switch(revnum, "", True, url, editor)
    try:
        adm.crawl_revisions(basedir, reporter, restore_files=False,
                            recurse=True, use_commit_times=True)
    except subvertpy.SubversionException as e:
        msg, num = e.args
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


class Walker(object):
    """Iterator of a Subversion working copy.

    This follows the Tree.iter_entries_by_dir order:

    * Parents before children
    * Ordered by name
    """

    def __init__(self, workingtree, start=u"", recursive=True):
        """Create a new walker.

        :param workingtree: bzr-svn working tree to walk over
        :param start: Start path, relative to tree root
        :param recursive: Whether to be recursive
        """
        self.workingtree = workingtree
        self.todo = list([start])
        self.pending = deque()
        self.recursive = recursive

    def __iter__(self):
        return iter(self.__next__, None)

    def __next__(self):
        while not self.pending:
            try:
                p = self.todo.pop()
            except IndexError:
                return None
            try:
                wc = self.workingtree._get_wc(p)
            except subvertpy.SubversionException as e:
                msg, num = e.args
                if num == subvertpy.ERR_WC_NOT_DIRECTORY:
                    continue
                raise
            try:
                entries = wc.entries_read(True)
                for name in sorted(entries):
                    entry = entries[name]
                    if isinstance(name, bytes):
                        name = name.decode('utf-8')
                    subp = osutils.pathjoin(p, name).rstrip("/")
                    if entry.kind == subvertpy.NODE_DIR and name != "":
                        if self.recursive:
                            self.todo.append(subp)
                    if name != '' or subp == '':
                        self.pending.append((subp, entry))
            finally:
                wc.close()
        return self.pending.popleft()


class SvnWorkingTree(SubversionTree, WorkingTree):
    """WorkingTree implementation that uses a svn working copy for storage."""

    def __init__(self, controldir, format, local_path, entry):
        self._reset_data()
        if not isinstance(local_path, text_type):
            raise TypeError(local_path)
        self.entry = entry
        self.basedir = local_path
        self._format = format
        self.controldir = controldir
        self._branch = None
        self._cached_base_tree = None
        self._detect_case_handling()
        self.bzr_controldir = os.path.join(controldir.local_path, controldir._adm_dir, 'bzr')
        try:
            os.makedirs(self.bzr_controldir)
            os.makedirs(os.path.join(self.bzr_controldir, 'lock'))
        except OSError:
            pass
        control_transport = controldir.transport.clone('bzr')
        self._transport = control_transport
        cache_filename = control_transport.local_abspath('stat-cache')
        self._hashcache = hashcache.HashCache(self.basedir, cache_filename,
            self.controldir._get_file_mode(),
            self._content_filter_stack_provider())
        self._hashcache.read()
        self._lock_count = 0
        self._lock_mode = None
        self._control_files = None
        self.views = self._make_views()

    def _cleanup(self):
        pass

    @property
    def mapping(self):
        """bzr-svn mapping to use."""
        return self.branch.mapping

    def stored_kind(self, path, file_id=None):
        assert path is not None
        try:
            return self.basis_tree().kind(path, file_id)
        except NoSuchFile:
            return None

    def kind(self, path, file_id=None):
        abspath = self.abspath(path)
        try:
            return osutils.file_kind(abspath)
        except NoSuchFile:
            return None

    def _detect_case_handling(self):
        try:
            self.controldir.transport.stat("FoRmAt")
        except NoSuchFile:
            self.case_sensitive = True
        else:
            self.case_sensitive = False

    def _set_root_id(self, file_id):
        self._change_fileid_mapping(file_id, u"")

    def get_file_mtime(self, path, file_id=None):
        """See Tree.get_file_mtime."""
        try:
            return os.lstat(self.abspath(path)).st_mtime
        except EnvironmentError as e:
            if e.errno == errno.ENOENT:
                raise NoSuchFile(path=path)
            raise

    def _setup_directory_is_tree_reference(self):
        self._directory_is_tree_reference = self._directory_is_never_tree_reference

    def get_file_sha1(self, path, file_id=None, stat_value=None):
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
                return self.controldir.get_branch_path()
            except RepositoryRootUnknown:
                pass
        return self.branch.get_branch_path(revnum)

    @property
    def branch(self):
        if self._branch is None:
            self._branch = self.controldir.open_branch(revnum=self.base_revnum)
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
            if not isinstance(prefix, text_type):
                raise TypeError(prefix)
            if not isinstance(patprefix, bytes):
                raise TypeError(patprefix)
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
                except subvertpy.SubversionException as e:
                    msg, num = e.args
                    if num == subvertpy.ERR_WC_NOT_DIRECTORY:
                        continue
                    raise
                try:
                    dir_add(subwc, subprefix, urlutils.joinpath(patprefix,
                        entry))
                finally:
                    subwc.close()

        with self._get_wc() as wc:
            dir_add(wc, u"", ".")

        return ignores

    def is_ignored(self, path):
        dirname = os.path.dirname(path)
        with self._get_wc(relpath=dirname) as wc:
            ignores = svn_config.get_default_ignores()
            ignorestr = wc.prop_get(properties.PROP_IGNORE,
                self.abspath(dirname))
            if ignorestr is not None:
                ignores.extend(ignorestr.splitlines())
            return match_ignore_list(os.path.basename(path), ignores)

    def flush(self):
        pass

    def is_control_filename(self, path):
        """Check whether path is a control file (used by bzr or svn)."""
        return is_adm_dir(path)

    def _update(self, branch_path, revnum, show_base):
        if revnum is None:
            # FIXME: should be able to use -1 here
            revnum = self.branch.get_revnum()
        old_branch_path = self.get_branch_path()
        with self._get_wc(write_lock=True, depth=-1) as adm:
            conn = self.branch.repository.svn_transport.get_connection(old_branch_path)
            try:
                update_wc(adm, self.basedir.encode("utf-8"), conn,
                        urlutils.join(self.branch.repository.svn_transport.svn_url, branch_path),
                        revnum)
            finally:
                self.branch.repository.svn_transport.add_connection(conn)
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
        self._cached_base_revnum = self._update(branch_path, revnum,
            show_base=show_base)
        self._cached_base_revid = None
        self._cached_base_idmap = None
        return self.base_revnum - orig_revnum

    def remove(self, files, verbose=False, to_file=None, keep_files=True,
               force=False):
        """Remove files from the working tree."""
        if not isinstance(files, list):
            files = [files]
        # FIXME: Use to_file argument
        # FIXME: Use verbose argument
        assert isinstance(files, list)
        with self._get_wc(write_lock=True) as wc:
            for file in files:
                try:
                    wc.delete(osutils.safe_utf8(self.abspath(file)),
                              keep_local=keep_files)
                except subvertpy.SubversionException as e:
                    msg, num = e.args
                    if num == ERR_BAD_FILENAME:
                        note("%s does not exist." % file)
                    else:
                        raise

        for file in files:
            self._change_fileid_mapping(None, file)

    def unversion(self, paths, file_ids=None):
        with self._get_wc(write_lock=True) as wc:
            for path in paths:
                wc.delete(osutils.safe_utf8(self.abspath(path)),
                          keep_local=True)

    def all_versioned_paths(self):
        ret = set()
        w = Walker(self)
        for path, entry in w:
            ret.add(path)
        return ret

    def all_file_ids(self):
        """See Tree.all_file_ids"""
        ret = set()
        w = Walker(self)
        for path, entry in w:
            try:
                ret.add(self.lookup_id(path)[0])
            except KeyError:
                pass
        return ret

    @convert_svn_error
    def _get_wc(self, relpath=u"", write_lock=False, depth=0, base=None):
        """Open a working copy handle."""
        return Adm(base,
            self.abspath(relpath).rstrip("/"),
            write_lock, depth)

    def _get_rel_wc(self, relpath, write_lock=False):
        if not isinstance(relpath, text_type):
            raise TypeError(relpath)
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
            with self._get_wc(osutils.safe_unicode(to_dir), write_lock=True) as to_wc:
                to_wc.copy(from_abspath,
                    osutils.safe_utf8(os.path.basename(from_path)))
            with self._get_wc(write_lock=True) as from_wc:
                from_wc.delete(from_abspath)

    def rename_one(self, from_rel, to_rel, after=False):
        from_rel = osutils.safe_unicode(from_rel)
        to_rel = osutils.safe_unicode(to_rel)
        # FIXME: Use after
        if after:
            raise NotImplementedError("rename_one after not supported")
        from_wc = None
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
        if not isinstance(path, text_type):
            raise TypeError(path)
        path = osutils.normpath(path)
        if path == u".":
            path = u""
        return self.lookup_id(path)

    def _find_ids(self, relpath, entry):
        if not isinstance(relpath, text_type):
            raise TypeError(relpath)
        assert entry.schedule in (SCHEDULE_NORMAL,
                                  SCHEDULE_DELETE,
                                  SCHEDULE_ADD,
                                  SCHEDULE_REPLACE)
        if entry.schedule == SCHEDULE_NORMAL:
            # Keep old id
            try:
                return self.path_to_file_id(
                    entry.cmt_rev, entry.revision, relpath)
            except KeyError:
                if entry.revision == self._cached_base_revnum:
                    raise AssertionError(
                        "file %s:%d not in fileid map for %d" % (
                            relpath, entry.revision, self._cached_base_revnum))
                # For some reason a file that doesn't exist in the current
                # revision has ended up here. Let's just generate a NEW- file
                # id
        elif entry.schedule == SCHEDULE_DELETE:
            return (None, None)
        elif (entry.schedule == SCHEDULE_ADD or
              entry.schedule == SCHEDULE_REPLACE):
            ids = self._get_new_file_ids()
            if relpath in ids:
                return (ids[relpath], None)
        else:
            raise AssertionError("unknown schedule value %r for %s" % (
                entry.schedule, relpath))
        # FIXME: Generate more random but consistent file ids
        return (
            b"NEW-" + escape_svn_path(relpath.strip("/")),
            None)

    def path2id(self, path):
        if isinstance(path, list):
            path = "/".join(path)
        with self._get_wc() as wc:
            try:
                entry = self._get_entry(wc, path)
                (file_id, revision) = self._find_ids(
                        osutils.safe_unicode(path), entry)
            except KeyError:
                return None
            else:
                if not isinstance(file_id, bytes):
                    raise TypeError(file_id)
                return file_id

    def _get_entry(self, wc, path):
        path = osutils.safe_utf8(self.abspath(path))
        related_wc = wc.probe_try(path)
        if related_wc is None:
            raise KeyError
        try:
            return related_wc.entry(path)
        finally:
            related_wc.close()

    def filter_unversioned_files(self, paths):
        ret = set()
        with self._get_wc() as wc:
            for p in paths:
                try:
                    entry = self._get_entry(wc, p)
                except KeyError:
                    ret.add(p)
                else:
                    if entry.schedule == SCHEDULE_DELETE:
                        ret.add(p)
        return ret

    def id2path(self, file_id):
        ids = self._get_new_file_ids()
        for path, fid in ids.items():
            if file_id == fid:
                return path
        try:
            return self.basis_idmap.reverse_lookup(self.mapping, file_id)
        except KeyError:
            pass
        if file_id.startswith("NEW-"):
            return urlutils.unescape(file_id[4:])
        if file_id == self.path2id(''):
            # Special case if self.last_revision() == 'null:'
            return ""
        raise NoSuchId(self, file_id)

    def _ie_from_entry(self, relpath, entry, parent_id):
        assert type(parent_id) is bytes or parent_id is None
        if not isinstance(relpath, text_type):
            raise TypeError(relpath)
        (file_id, revid) = self._find_ids(relpath, entry)
        abspath = self.abspath(relpath)
        basename = os.path.basename(relpath)
        if entry.kind == subvertpy.NODE_DIR:
            ie = SubversionTreeDirectory(file_id, basename, parent_id)
            ie.revision = revid
            return ie
        elif os.path.islink(abspath):
            ie = SubversionTreeLink(file_id, basename, parent_id)
            ie.revision = revid
            target_path = os.readlink(abspath.encode(osutils._fs_enc))
            ie.symlink_target = target_path.decode(osutils._fs_enc)
            return ie
        else:
            ie = SubversionTreeFile(file_id, basename, parent_id)
            ie.revision = revid
            try:
                data = osutils.fingerprint_file(
                    open(abspath.encode(osutils._fs_enc), 'rb'))
            except IOError as e:
                if e.errno == errno.EISDIR:
                    ie = SubversionTreeDirectory(file_id, basename, parent_id)
                    ie.revision = None
                    return ie
                elif e.errno == errno.ENOENT:
                    return None
                raise
            else:
                ie.text_sha1 = data['sha1']
                ie.text_size = data['size']
                ie.executable = self.is_executable(relpath)
                return ie

    def iter_child_entries(self, path, file_id=None):
        """See Tree.iter_child_entries."""
        entry = next(self.iter_entries_by_dir(specific_files=[path]))[1]
        return getattr(entry, 'children', {}).values()

    def iter_entries_by_dir(self, specific_files=None):
        """See WorkingTree.iter_entries_by_dir."""
        if specific_files is not None:
            with self._get_wc() as wc:
                for path in specific_files:
                    parent = os.path.dirname(path)
                    parent_id = self.lookup_id(parent)
                    try:
                        entry = self._get_entry(wc, path)
                    except KeyError:
                        raise NoSuchFile(path, self)
                    if entry.schedule == SCHEDULE_DELETE:
                        continue
                    if not isinstance(path, text_type):
                        path = path.decode('utf-8')
                    ie = self._ie_from_entry(path, entry, parent_id[0])
                    if ie is not None:
                        yield path, ie
        else:
            fileids = {}
            w = Walker(self)
            for relpath, entry in w:
                if entry.schedule == SCHEDULE_DELETE:
                    continue
                if not isinstance(relpath, text_type):
                    raise TypeError(relpath)
                if relpath == u"":
                    parent_id = None
                else:
                    parent_id = fileids[os.path.dirname(relpath)]
                ie = self._ie_from_entry(relpath, entry, parent_id)
                if ie is not None:
                    fileids[relpath] = ie.file_id
                    yield relpath, ie

    def extras(self):
        """See WorkingTree.extras."""
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
                if self.controldir.is_control_filename(subf):
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

    def set_last_revision(self, revid):
        mutter('setting last revision to %r', revid)
        if revid == NULL_REVISION:
            # If there is a hidden revision at the beginning of the branch,
            # use that as the NULL revision.
            foreign_revid = None
            for revmeta, hidden, mapping in self.branch._iter_revision_meta_ancestry():
                if hidden:
                    foreign_revid = revmeta.metarev.get_foreign_revid()
                else:
                    foreign_revid = None
            if foreign_revid is None:
                raise NotImplementedError("unable to set NULL_REVISION if there is "
                    "no hidden initial revision")
            self._cached_base_revnum = foreign_revid[2]
            self._cached_base_idmap = None
        else:
            (foreign_revid, mapping) = self.branch.lookup_bzr_revision_id(revid)
            self._cached_base_revnum = foreign_revid[2]
            self._cached_base_idmap = None
        # FIXME: set any items that are no longer versioned to SCHEDULE_ADD,
        # but remove their versioning information
        self._cached_base_revid = revid

    def set_parent_trees(self, parents_list, allow_leftmost_as_ghost=False):
        """See MutableTree.set_parent_trees."""
        self.set_parent_ids([rev for (rev, tree) in parents_list])

    def get_parent_ids(self):
        """See Tree.get_parent_ids.

        This implementation reads the pending merges list and last_revision
        value and uses that to decide what the parents list should be.
        """
        last_rev = self._last_revision()
        if NULL_REVISION == last_rev:
            parents = []
        else:
            parents = [last_rev]
        if not self._cached_merges:
            try:
                merges_bytes = self._transport.get_bytes('pending-merges')
            except NoSuchFile:
                merges = []
            else:
                merges = []
                for l in osutils.split_lines(merges_bytes):
                    revision_id = l.rstrip('\n')
                    merges.append(revision_id)
            if self.is_locked():
                self._cached_merges = merges
        else:
            merges = self._cached_merges
        parents.extend(merges)
        return parents

    def set_parent_ids(self, parent_ids, allow_leftmost_as_ghost=False):
        """See MutableTree.set_parent_ids."""
        if parent_ids == []:
            merges = []
            self.set_last_revision(NULL_REVISION)
        else:
            merges = parent_ids[1:]
            self.set_last_revision(parent_ids[0])
        if self.is_locked():
            self._cached_merges = merges
        self._transport.put_bytes('pending-merges', '\n'.join(merges),
            mode=self.controldir._get_file_mode())
        with self._get_wc(write_lock=True) as adm:
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

    def smart_add(self, file_list, recurse=True, action=None, save=True):
        """See MutableTree.smart_add()."""
        assert isinstance(recurse, bool)
        if action is None:
            action = breezy.add.AddAction()
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
            with self._get_wc(os.path.dirname(f.encode(osutils._fs_enc)).decode(osutils._fs_enc), write_lock=True) as wc:
                if self.filter_unversioned_files([f]):
                    if save:
                        mutter('adding %r', file_path)
                        wc.add(file_path)
                        self._fix_special(wc, file_path, f)
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
            if todo != []:
                cadded, cignored = self.smart_add(todo, recurse, action, save)
                added.extend(cadded)
                ignored.update(cignored)
        return added, ignored

    def _fix_special(self, adm, abspath, relpath, kind=None):
        if kind is None:
            kind = self.kind(relpath)
        if kind == "file":
            value = None
        elif kind == "symlink":
            value = properties.PROP_SPECIAL_VALUE
        else:
            return
        adm.prop_set(properties.PROP_SPECIAL, value, abspath)

    def _fix_kind(self, adm, abspath, relpath, entry):
        kind = self.kind(relpath)
        if ((entry.kind == subvertpy.NODE_DIR and kind in ('file', 'symlink')) or
            (entry.kind == subvertpy.NODE_FILE and kind == 'directory')):
            try:
                adm.delete(abspath, keep_local=True)
                adm.add(abspath)
            except subvertpy.SubversionException as e:
                msg, num = e.args
                if num == ERR_WC_NODE_KIND_CHANGE:
                    if entry.kind == subvertpy.NODE_DIR:
                        from_kind = "directory"
                    else:
                        from_kind = "file or symlink"
                    raise bzr_errors.UnsupportedKindChange(relpath,
                        from_kind, kind, self._format)
                raise
        self._fix_special(adm, abspath, relpath, kind)

    def add(self, files, ids=None, kinds=None, _copyfrom=None):
        """Add files to the working tree."""
        # TODO: Use kinds
        if isinstance(files, string_types):
            files = [files]
            if isinstance(ids, bytes):
                ids = [ids]
        if ids is None:
            ids = [None] * len(files)
        if _copyfrom is None:
            _copyfrom = [(None, -1)] * len(files)
        if kinds is None:
            kinds = [self.kind(file) for file in files]
        assert isinstance(files, list)
        for f, kind, file_id, copyfrom in zip(files, kinds, ids, _copyfrom):
            f = f.rstrip('/')
            with self._get_wc(os.path.dirname(f), write_lock=True) as wc:
                try:
                    abspath = self.abspath(f)
                    wc.add(abspath, copyfrom[0], copyfrom[1])
                except subvertpy.SubversionException as e:
                    msg, num = e.args
                    if num in (subvertpy.ERR_ENTRY_EXISTS,
                               subvertpy.ERR_WC_SCHEDULE_CONFLICT):
                        continue
                    elif num == subvertpy.ERR_WC_PATH_NOT_FOUND:
                        raise NoSuchFile(path=f)
                    raise
                self._fix_special(wc, abspath, f)
            if file_id is not None:
                self._change_fileid_mapping(file_id, f)

    def basis_tree(self):
        """Return the basis tree for a working tree."""
        try:
            return SvnBasisTree(self)
        except BasisTreeIncomplete:
            return self.branch.basis_tree()

    def list_files(self, include_root=False, from_dir=None, recursive=True):
        """See ``Tree.list_files``."""
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
                    assert type(parent_id) is bytes
                    fileids[parent_path] = parent_id
            ie = self._ie_from_entry(relpath, entry, parent_id)
            if ie is not None:
                fileids[relpath] = ie.file_id
            if include_root or relpath != u"":
                yield (posixpath.relpath(relpath, from_dir) if from_dir else relpath), versioned, ie.kind, ie.file_id, ie

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
             delta_reporter=None, possible_transports=None, local=False,
             show_base=False):
        """Pull in changes from another branch into this working tree."""
        # FIXME: Use delta_reporter
        result = self.branch.pull(source, overwrite=overwrite,
            stop_revision=stop_revision, local=local)
        fetched = self._update(self.branch.get_branch_path(),
            self.branch.get_revnum(), show_base=show_base)
        self._cached_base_revnum = fetched
        self._cached_base_revid = None
        self._cached_base_idmap = None
        self._cached_merges = None
        return result

    def get_file_properties(self, path, file_id=None):
        abspath = self.abspath(path)
        with self._get_wc(write_lock=False) as root_adm, \
             root_adm.probe_try(abspath.encode("utf-8"), False, 2) as adm:
            try:
                (prop_changes, orig_props) = adm.get_prop_diffs(
                    abspath.encode("utf-8"))
            except subvertpy.SubversionException as e:
                msg, num = e.args
                if num in (subvertpy.ERR_WC_NOT_DIRECTORY,
                           subvertpy.ERR_WC_NOT_LOCKED):
                    return {}
                raise
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
                if path in new_entries:
                    del new_entries[path]
            else:
                assert isinstance(id, bytes)
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
            assert ids[path] is bytes
            return (ids[path], None)
        try:
            return self.basis_idmap.lookup(self.mapping, path)[:2]
        except KeyError:
            if path == "":
                return (b"root-%s-%s" % (
                    escape_svn_path(self.get_branch_path()),
                    self.entry.uuid.encode('ascii')), None)
            mutter("fileid map: %r", self._get_new_file_ids())
            raise

    def _get_changed_branch_props(self):
        with self._get_wc() as wc:
            ret = {}
            (prop_changes, orig_props) = wc.get_prop_diffs(
                self.basedir.encode("utf-8"))
            for k, v in prop_changes:
                ret[k] = (orig_props.get(k), v)
            return ret

    def _get_branch_props(self):
        return self.get_file_properties("")

    def _set_branch_props(self, wc, fileprops):
        for k, v in fileprops.items():
            wc.prop_set(k, v, self.basedir.encode("utf-8"))

    def _get_base_branch_props(self):
        with self._get_wc() as wc:
            (prop_changes, orig_props) = wc.get_prop_diffs(
                self.basedir.encode("utf-8"))
            return orig_props

    def _get_new_file_ids(self):
        return self.mapping.import_fileid_map_fileprops(
            self._get_changed_branch_props())

    def _get_svk_merges(self, base_branch_props):
        return base_branch_props.get(svk.SVN_PROP_SVK_MERGE, "")

    def _apply_inventory_delta_change(self, base_tree, old_path, new_path,
                                      file_id, ie):
        already_there = (
            old_path == new_path and
            base_tree.kind(old_path) == ie.kind)
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
        delta.sort(key=operator.itemgetter(1))
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
        try:
            revid = self.branch.generate_revision_id(self.base_revnum)
        except NoSuchRevision:
            # Current revision is no longer on the mainline?
            revid = self.branch.repository.generate_revision_id(self.base_revnum,
                self.get_branch_path(), self.mapping)
        if self.is_locked():
            self._cached_base_revid = revid
        return revid

    def path_content_summary(self, path, _lstat=os.lstat,
        _mapper=osutils.file_kind_from_stat_mode):
        """See Tree.path_content_summary."""
        abspath = self.abspath(path)
        try:
            stat_result = _lstat(abspath.encode(osutils._fs_enc))
        except OSError as e:
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
            self.get_branch_path(), self.base_revnum)

    def _reset_data(self):
        self._cached_base_revnum = None
        self._cached_base_idmap = None
        self._cached_base_revid = None
        self._cached_merges = None

    def break_lock(self):
        """Break a lock if one is present from another instance.

        Uses the ui factory to ask for confirmation if the lock may be from
        an active process.

        This will probe the repository for its lock as well.
        """
        cleanup(self.basedir.encode("utf-8"))

    def is_locked(self):
        return (self._lock_mode is not None)

    def _lock_write(self):
        if self._lock_mode:
            if self._lock_mode == 'r':
                raise ReadOnlyError(self)
            self._lock_count += 1
        else:
            self._lock_mode = 'w'
            self._lock_count = 1
        return BranchWriteLockResult(self.unlock, None)

    def lock_tree_write(self):
        ret = self._lock_write()
        try:
            self.branch.lock_read()
        except:
            ret.unlock()
            raise
        return ret

    def get_physical_lock_status(self):
        return False

    def lock_write(self, token=None):
        """See Branch.lock_write()."""
        # TODO: Obtain lock on the remote server?
        if token is not None:
            raise TokenLockingNotSupported(self)
        ret = self._lock_write()
        try:
            self.branch.lock_write()
        except:
            ret.unlock()
            raise
        return ret

    def lock_read(self):
        """See Branch.lock_read()."""
        if self._lock_mode:
            assert self._lock_mode in ('r', 'w')
            self._lock_count += 1
        else:
            self._lock_mode = 'r'
            self._lock_count = 1
        self.branch.lock_read()
        return LogicalLockResult(self.unlock)

    def unlock(self):
        self._lock_count -= 1
        if self._lock_count == 0:
            self._lock_mode = None
            self._cleanup()
            self._reset_data()
        self.branch.unlock()

    def _is_executable_from_path_and_stat_from_stat(self, path, stat_result):
        mode = stat_result.st_mode
        return bool(stat.S_ISREG(mode) and stat.S_IEXEC & mode)

    def is_executable(self, path, file_id=None):
        if not osutils.supports_executable(self.abspath(path)):
            basis_tree = self.basis_tree()
            if file_id in basis_tree:
                return basis_tree.is_executable(path, file_id)
            # Default to not executable
            return False
        else:
            mode = os.lstat(self.abspath(path)).st_mode
            return bool(stat.S_ISREG(mode) and stat.S_IEXEC & mode)

        _is_executable_from_path_and_stat = \
            _is_executable_from_path_and_stat_from_stat

    def transmit_svn_dir_deltas(self, file_id, editor):
        path = self.id2path(file_id)
        encoded_path = self.abspath(path).encoded("utf-8")
        with self._get_wc(write_lock=True) as root_adm, \
            root_adm.probe_try(encoded_path, True, 1) as adm:
            entry = adm.entry(encoded_path)
            self._fix_kind(root_adm, encoded_path, path, entry)
            root_adm.transmit_prop_deltas(encoded_path, True, editor)

    def transmit_svn_file_deltas(self, path, editor):
        encoded_path = self.abspath(path).encode("utf-8")
        with self._get_wc(write_lock=True) as root_adm, \
            root_adm.probe_try(encoded_path, True, 1) as adm:
            entry = adm.entry(encoded_path)
            self._fix_kind(adm, encoded_path, path, entry)
            root_adm.transmit_prop_deltas(encoded_path, entry, editor)
            root_adm.transmit_text_deltas(encoded_path, True, editor)

    def merge_modified(self):
        """Return a dictionary of files modified by a merge.

        The list is initialized by WorkingTree.set_merge_modified, which is
        typically called after we make some automatic updates to the tree
        because of a merge.

        This returns a map of file_id->sha1, containing only files which are
        still in the working inventory and have that text hash.
        """
        with self.lock_read():
            try:
                hashfile = self._transport.get('merge-hashes')
            except NoSuchFile:
                return {}
            try:
                merge_hashes = {}
                try:
                    if next(hashfile) != MERGE_MODIFIED_HEADER_1 + '\n':
                        raise MergeModifiedFormatError()
                except StopIteration:
                    raise MergeModifiedFormatError()
                for s in _mod_rio.RioReader(hashfile):
                    # RioReader reads in Unicode, so convert file_ids back to utf8
                    file_id = osutils.safe_file_id(s.get("file_id"), warn=False)
                    try:
                        path = self.id2path(file_id)
                    except NoSuchId:
                        continue
                    text_hash = s.get("hash")
                    if text_hash == self.get_file_sha1(path):
                        merge_hashes[file_id] = text_hash
                return merge_hashes
            finally:
                hashfile.close()

    def set_merge_modified(self, modified_hashes):
        def iter_stanzas():
            for file_id, hash in modified_hashes.items():
                yield _mod_rio.Stanza(
                    file_id=file_id.decode('utf8'), hash=hash)
        with self.lock_tree_write():
            my_file = _mod_rio.rio_file(
                iter_stanzas(), MERGE_MODIFIED_HEADER_1)
            self._transport.put_file(
                'merge-hashes', my_file, mode=self.controldir._get_file_mode())

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
        assert revmeta.get_revision_id(mapping) == new_revid, \
                "%r != %r" % (revmeta.get_revision_id(mapping), new_revid)
        rev = revmeta.metarev.revnum
        self._cached_base_revnum = rev
        self._cached_base_revid = new_revid
        self._cached_base_idmap = None
        self._cached_merges = []

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
                self.abspath(path), True, 1)
            adms_to_close.add(adm)
            cq.queue(self.abspath(path).rstrip("/"), adm,
                True, None, False, False, md5sum)

        cq = CommittedQueue()
        root_adm = self._get_wc(self.abspath("."), write_lock=True, depth=-1)
        adms_to_close.add(root_adm)
        try:
            for repos_path, changes in revmeta.metarev.paths.items():
                if changes[0] == 'D':
                    continue
                assert changes[0] in ('A', 'M', 'R')
                path = repos_path[len(revmeta.metarev.branch_path):].strip("/")
                try:
                    kind = self.kind(path)
                except NoSuchFile:
                    md5sum = None
                else:
                    if kind == "file":
                        md5sum = osutils.md5(self.get_file_text(path)).digest()
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

    def annotate_iter(self, path, default_revision=CURRENT_REVISION, file_id=None):
        from .annotate import Annotater
        with self.lock_read():
            annotater = Annotater(self.branch.repository)
            parent_lines = []
            if len(self.get_parent_ids()) > 1:
                graph = self.branch.repository.get_graph()
                head_parents = graph.heads(self.get_parent_ids())
            else:
                head_parents = self.get_parent_ids()
            for revid in head_parents:
                foreign_revid, mapping = self.branch.repository.lookup_bzr_revision_id(
                    revid)
                parent_lines.append(annotater.check_file_revs(
                    revid, foreign_revid[1], foreign_revid[2], self.mapping, path))
            return annotater.check_file(
                self.get_file_lines(path, file_id),
                default_revision, parent_lines)

    def find_related_paths_across_trees(self, paths, trees=[],
            require_versioned=True):
        """Find related paths in tree corresponding to specified filenames in any
        of `lookup_trees`.

        All matches in all trees will be used, and all children of matched
        directories will be used.

        :param paths: The filenames to find related paths for (if None, returns
            None)
        :param trees: The trees to find file_ids within
        :param require_versioned: if true, all specified filenames must occur in
            at least one tree.
        :return: a set of paths for the specified filenames and their children
            in `tree`
        """
        if paths is None:
            return None;
        file_ids = self.paths2ids(
                paths, trees, require_versioned=require_versioned)
        ret = set()
        for file_id in file_ids:
            try:
                ret.add(self.id2path(file_id))
            except errors.NoSuchId:
                pass
        return ret

    def walkdirs(self, prefix=""):
        """Walk the directories of this tree.

        returns a generator which yields items in the form:
                ((curren_directory_path, fileid),
                 [(file1_path, file1_name, file1_kind, (lstat), file1_id,
                   file1_kind), ... ])

        This API returns a generator, which is only valid during the current
        tree transaction - within a single lock_read or lock_write duration.

        If the tree is not locked, it may cause an error to be raised,
        depending on the tree implementation.
        """
        disk_top = self.abspath(prefix)
        if disk_top.endswith('/'):
            disk_top = disk_top[:-1]
        top_strip_len = len(disk_top) + 1
        inventory_iterator = self._walkdirs(prefix)
        disk_iterator = osutils.walkdirs(disk_top, prefix)
        try:
            current_disk = next(disk_iterator)
            disk_finished = False
        except OSError as e:
            if not (e.errno == errno.ENOENT or
                (sys.platform == 'win32' and e.errno == ERROR_PATH_NOT_FOUND)):
                raise
            current_disk = None
            disk_finished = True
        try:
            current_inv = next(inventory_iterator)
            inv_finished = False
        except StopIteration:
            current_inv = None
            inv_finished = True
        while not inv_finished or not disk_finished:
            if current_disk:
                ((cur_disk_dir_relpath, cur_disk_dir_path_from_top),
                    cur_disk_dir_content) = current_disk
            else:
                ((cur_disk_dir_relpath, cur_disk_dir_path_from_top),
                    cur_disk_dir_content) = ((None, None), None)
            if not disk_finished:
                # strip out .bzr dirs
                if (cur_disk_dir_path_from_top[top_strip_len:] == '' and
                    len(cur_disk_dir_content) > 0):
                    # osutils.walkdirs can be made nicer -
                    # yield the path-from-prefix rather than the pathjoined
                    # value.
                    bzrdir_loc = bisect_left(cur_disk_dir_content,
                        ('.svn', '.svn'))
                    if (bzrdir_loc < len(cur_disk_dir_content)
                        and self.controldir.is_control_filename(
                            cur_disk_dir_content[bzrdir_loc][0])):
                        # we dont yield the contents of, or, .bzr itself.
                        del cur_disk_dir_content[bzrdir_loc]
            if inv_finished:
                # everything is unknown
                direction = 1
            elif disk_finished:
                # everything is missing
                direction = -1
            else:
                direction = ((current_inv[0][0] > cur_disk_dir_relpath) -
                             (current_inv[0][0] < cur_disk_dir_relpath))

            if direction > 0:
                # disk is before inventory - unknown
                dirblock = [(relpath, basename, kind, stat, None, None) for
                    relpath, basename, kind, stat, top_path in
                    cur_disk_dir_content]
                yield (cur_disk_dir_relpath, None), dirblock
                try:
                    current_disk = next(disk_iterator)
                except StopIteration:
                    disk_finished = True
            elif direction < 0:
                # inventory is before disk - missing.
                dirblock = [(relpath, basename, 'unknown', None, fileid, kind)
                    for relpath, basename, dkind, stat, fileid, kind in
                    current_inv[1]]
                yield (current_inv[0][0], current_inv[0][1]), dirblock
                try:
                    current_inv = next(inventory_iterator)
                except StopIteration:
                    inv_finished = True
            else:
                # versioned present directory
                # merge the inventory and disk data together
                dirblock = []
                for relpath, subiterator in itertools.groupby(sorted(
                    current_inv[1] + cur_disk_dir_content,
                    key=operator.itemgetter(0)), operator.itemgetter(1)):
                    path_elements = list(subiterator)
                    if len(path_elements) == 2:
                        inv_row, disk_row = path_elements
                        # versioned, present file
                        dirblock.append((inv_row[0],
                            inv_row[1], disk_row[2],
                            disk_row[3], inv_row[4],
                            inv_row[5]))
                    elif len(path_elements[0]) == 5:
                        # unknown disk file
                        dirblock.append((path_elements[0][0],
                            path_elements[0][1], path_elements[0][2],
                            path_elements[0][3], None, None))
                    elif len(path_elements[0]) == 6:
                        # versioned, absent file.
                        dirblock.append((path_elements[0][0],
                            path_elements[0][1], 'unknown', None,
                            path_elements[0][4], path_elements[0][5]))
                    else:
                        raise NotImplementedError('unreachable code')
                yield current_inv[0], dirblock
                try:
                    current_inv = next(inventory_iterator)
                except StopIteration:
                    inv_finished = True
                try:
                    current_disk = next(disk_iterator)
                except StopIteration:
                    disk_finished = True

    def _walkdirs(self, prefix=""):
        """Walk the directories of this tree.

        :param prefix: is used as the directrory to start with.
        :returns: a generator which yields items in the form::

            ((curren_directory_path, fileid),
             [(file1_path, file1_name, file1_kind, None, file1_id,
               file1_kind), ... ])
        """
        pending = [(prefix, '', 'directory', None, self.path2id(''), None)]
        while pending:
            dirblock = []
            currentdir = pending.pop()
            # 0 - relpath, 1- basename, 2- kind, 3- stat, 4-id, 5-kind
            top_id = currentdir[4]
            if currentdir[0]:
                relroot = currentdir[0] + '/'
            else:
                relroot = ""
            # FIXME: stash the node in pending
            for entry in self.iter_child_entries(currentdir[0]):
                if entry.kind == 'directory':
                    for name, child in entry.sorted_children():
                        dirblock.append((relroot + name, name, child.kind, None,
                            child.file_id, child.kind
                            ))
                yield (currentdir[0], entry.file_id), dirblock
                # push the user specified dirs from dirblock
                for dir in reversed(dirblock):
                    if dir[2] == _directory:
                        pending.append(dir)


class SvnWorkingTreeFormat(WorkingTreeFormat):
    """Subversion working copy format."""

    supports_versioned_directories = True

    supports_store_uncommitted = False

    def __init__(self, version=None):
        self.version = version

    def __get_matchingcontroldir(self):
        return SvnWorkingTreeDirFormat(self.version)

    _matchingcontroldir = property(__get_matchingcontroldir)

    def get_format_description(self):
        if self.version is not None:
            return "Subversion Working Copy (version %d)" % self.version
        else:
            return "Subversion Working Copy"

    def get_format_string(self):
        raise NotImplementedError

    def initialize(self, a_controldir, revision_id=None):
        raise NotImplementedError(self.initialize)

    def open(self, a_controldir):
        raise NotImplementedError(self.initialize)

    def get_controldir_for_branch(self):
        from .remote import SvnRemoteFormat
        return SvnRemoteFormat()

    def make_test_tree(self, relpath):
        controldir_format = self.get_controldir_for_branch()
        from breezy.transport import get_transport
        if relpath == '.':
            branch_relpath = '../.branch'
        else:
            branch_relpath = relpath+'.branch'
        t = get_transport(branch_relpath)
        controldir = controldir_format.initialize_on_transport(t)
        branch = controldir.create_branch()
        return branch.create_checkout(relpath, lightweight=True)


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
        except subvertpy.SubversionException as e:
            msg, num = e.args
            if num in (subvertpy.ERR_RA_LOCAL_REPOS_OPEN_FAILED,):
                raise NoSvnRepositoryPresent(transport.base)
            if num in (subvertpy.ERR_WC_NOT_DIRECTORY,):
                raise NotBranchError(path=transport.base)
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

    def initialize_on_transport_ex(
            self, transport, use_existing_dir=False, create_prefix=False,
            force_new_repo=False, stacked_on=None, stack_on_pwd=None,
            repo_format_name=None, make_working_trees=None, shared_repo=False,
            vfs_only=False):
        raise UninitializableFormat(self)

    def get_converter(self, format):
        """See ControlDirFormat.get_converter()."""
        return SvnCheckoutConverter(format)

    @property
    def repository_format(self):
        from .repository import SvnRepositoryFormat
        return SvnRepositoryFormat()

    def get_branch_format(self):
        from .branch import SvnBranchFormat
        return SvnBranchFormat()

    def supports_transport(self, transport):
        from breezy.transport.local import LocalTransport
        return isinstance(transport, LocalTransport)


class SvnCheckout(ControlDir):
    """ControlDir implementation for Subversion checkouts (directories
    containing a .svn subdirectory."""

    @property
    def control_transport(self):
        return self.transport

    @property
    def control_url(self):
        return urlutils.join(self.user_url, get_adm_dir())

    @property
    def user_transport(self):
        return self.root_transport

    def break_lock(self):
        raise NotImplementedError(self.break_lock)

    def __init__(self, transport, format):
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
            wc = Adm(None, self.local_path)
        except subvertpy.SubversionException as e:
            msg, num = e.args
            if num == ERR_WC_UNSUPPORTED_FORMAT:
                raise UnsupportedFormatError(msg, kind='workingtree')
            else:
                raise
        try:
            try:
                self.entry = wc.entry(self.local_path, True)
            except subvertpy.SubversionException as e:
                msg, num = e.args
                if num in (subvertpy.ERR_ENTRY_NOT_FOUND,
                           subvertpy.ERR_NODE_UNKNOWN_KIND):
                    raise CorruptWorkingTree(self.local_path, msg)
                else:
                    raise
        finally:
            wc.close()

        self.svn_root_url = self.entry.repos
        self.svn_url = self.entry.url
        self._remote_branch_transport = None
        self._remote_repo_transport = None
        self._remote_controldir = None
        self._adm_dir = get_adm_dir()
        self.root_transport = transport
        self.transport = self.root_transport.clone(get_adm_dir())

    def backup_bzrdir(self):
        self.root_transport.copy_tree(".svn", ".svn.backup")
        return (self.root_transport.abspath(".svn"),
                self.root_transport.abspath(".svn.backup"))

    def is_control_filename(self, filename):
        return filename == self._adm_dir or filename.startswith(self._adm_dir+'/')

    def get_remote_controldir(self):
        from .remote import SvnRemoteAccess
        if self._remote_controldir is None:
            self._remote_controldir = SvnRemoteAccess(self.get_remote_transport())
        return self._remote_controldir

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
        url = _mod_location.location_to_url(location)
        path = urlutils.local_path_from_url(url)
        return wt.branch.create_checkout(path, lightweight=True,
            revision_id=revision_id).controldir

    def open_workingtree(self, unsupported=False, recommend_upgrade=False):
        wt_format = SvnWorkingTreeFormat(self._format.version)
        ret = SvnWorkingTree(self, wt_format, self.local_path, self.entry)
        try:
            # FIXME: This could potentially be done without actually
            # opening the branch?
            ret.branch # Trigger NotSvnBranchPath error
        except NotSvnBranchPath as e:
            raise NoWorkingTree(self.local_path)
        return ret

    def sprout(self, *args, **kwargs):
        return self.get_remote_controldir().sprout(*args, **kwargs)

    def create_repository(self, shared=False):
        raise UninitializableFormat(self._format)

    def open_repository(self):
        raise NoRepositoryPresent(self)

    def find_repository(self, _ignore_branch_path=False):
        raise NoRepositoryPresent(self)

    def _find_repository(self):
        return self.get_remote_controldir().find_repository()

    def get_branch_path(self):
        if self.entry.repos is None:
            raise RepositoryRootUnknown()
        assert self.entry.url.startswith(self.entry.repos)
        return self.entry.url[len(self.entry.repos):].strip("/")

    def needs_format_conversion(self, format):
        return not isinstance(self._format, format.__class__)

    def create_workingtree(self, revision_id=None, hardlink=None):
        """See ControlDir.create_workingtree().

        Not implemented for Subversion because having a .svn directory
        implies having a working copy.
        """
        raise NotImplementedError(self.create_workingtree)

    def destroy_workingtree(self):
        raise UnsupportedOperation(self.destroy_workingtree, self)

    def create_branch(self):
        """See ControlDir.create_branch()."""
        raise NotImplementedError(self.create_branch)

    def open_branch(self, name=None, unsupported=True, ignore_fallbacks=False,
                    mapping=None, revnum=None, possible_transports=None):
        """See ControlDir.open_branch()."""
        from .branch import SvnBranch
        repos = self._find_repository()
        if mapping is None:
            mapping = repos.get_mapping()

        try:
            return SvnBranch(repos, self.get_remote_controldir(),
                self.get_branch_path(), mapping, revnum=revnum)
        except RepositoryRootUnknown:
            return self.get_remote_controldir().open_branch(revnum=revnum,
                possible_transports=possible_transports)
        except subvertpy.SubversionException as e:
            msg, num = e.args
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
            if (st.st_mode & 0o7777 == 0o0000):
                # FTP allows stat but does not return dir/file modes
                self._dir_mode = None
                self._file_mode = None
            else:
                self._dir_mode = (st.st_mode & 0o7777) | 0o0700
                # Remove the sticky and execute bits for files
                self._file_mode = self._dir_mode & ~0o7111

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
        from .config import SvnRepositoryConfig
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
        from breezy.branch import BranchReferenceFormat
        remote_branch = to_convert.open_branch()
        controldir = self.target_format.initialize(
            to_convert.root_transport.base)
        BranchReferenceFormat().initialize(controldir, remote_branch)
        controldir.create_workingtree()
        # FIXME: Convert working tree
        to_convert.root_transport.delete_tree(".svn")
        return controldir
