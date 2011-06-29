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

"""Fetching revisions from Subversion repositories in batches.

Everywhere in this file 'bzr_base_ie' refers to the entry with the
same file_id in the old revision (left hand side parent revision).

'svn_base_ie' refers to the inventory entry from the svn revision
that the copy happened.

'new_ie' refers to the inventory entry that is newly created, as part
of the inventory delta.
"""


from collections import defaultdict, deque

import subvertpy
from subvertpy import (
    ERR_FS_NOT_DIRECTORY,
    NODE_FILE,
    SubversionException,
    properties,
    )
from subvertpy.delta import (
    apply_txdelta_handler_chunks,
    )

from bzrlib import (
    debug,
    delta,
    lru_cache,
    osutils,
    trace,
    ui,
    urlutils,
    )
from bzrlib.errors import (
    NoSuchId,
    NoSuchRevision,
    )
from bzrlib.inventory import (
    InventoryDirectory,
    InventoryFile,
    InventoryLink,
    )
from bzrlib.revision import (
    NULL_REVISION,
    )
try:
    from bzrlib.revisiontree import InventoryRevisionTree
except ImportError: # bzr < 2.4
    from bzrlib.revisiontree import RevisionTree as InventoryRevisionTree
from bzrlib.repository import (
    InterRepository,
    )
from bzrlib.versionedfile import (
    ChunkedContentFactory,
    FulltextContentFactory,
    )

from bzrlib.plugins.svn.errors import (
    AbsentPath,
    InvalidFileName,
    TextChecksumMismatch,
    convert_svn_error,
    )
from bzrlib.plugins.svn.fileids import (
    get_local_changes,
    )
from bzrlib.plugins.svn.mapping import (
    SVN_PROP_BZR_PREFIX,
    )
from bzrlib.plugins.svn.repository import (
    SvnRepository,
    SvnRepositoryFormat,
    )
from bzrlib.plugins.svn.transport import (
    url_join_unescaped_path,
    )

# Max size of group in which revids are checked when looking for missing
# revisions
MAX_CHECK_PRESENT_INTERVAL = 1000
# Size of the text cache to keep
TEXT_CACHE_SIZE = 1024 * 1024 * 50

def inventory_parent_id_basename_to_file_id(inv, parent_id, basename):
    if parent_id is None and basename == "":
        if inv.root is None:
            return None
        else:
            return inv.root.file_id
    parent_id_basename_index = getattr(inv, "parent_id_basename_to_file_id",
        None)
    if parent_id_basename_index is None:
        return inv[parent_id].children[basename].file_id
    else:
        ret = parent_id_basename_index.iteritems(
            [(parent_id or '', basename.encode("utf-8"))])
        try:
            return ret.next()[1]
        except IndexError:
            raise KeyError((parent_id, basename))


def inventory_ancestors(inv, fileid, exceptions):
    """Find all ancestors of a particular file id in an inventory.

    :param inv: Inventory
    :param fileid: File id
    :param exceptions: Sequence of paths to not report on
    :return: Sequence of paths
    """
    todo = set([fileid])
    while todo:
        fileid = todo.pop()
        for ie in inv[fileid].children.values():
            p = inv.id2path(ie.file_id)
            if p in exceptions:
                continue
            yield p
            if ie.kind == "directory":
                todo.add(ie.file_id)


def md5_strings(chunks):
    """Return the MD5sum of a list of chunks.

    :param chunks: Chunks as strings
    :return: String with MD5 hex digest
    """
    s = osutils.md5()
    map(s.update, chunks)
    return s.hexdigest()


def md5_string(string):
    """Return the MD5sum of a string.

    :param string: String to calculate the MD5sum of.
    :return: MD5sums hex digest
    """
    s = osutils.md5()
    s.update(string)
    return s.hexdigest()


def check_filename(path):
    """Check that a path does not contain invalid characters.

    :param path: Path to check
    :raises InvalidFileName:
    """
    assert isinstance(path, unicode)
    if u"\\" in path:
        raise InvalidFileName(path)


def editor_strip_prefix(editor, path):
    """Wrap editor and strip base path from paths.

    :param editor: Editor to wrap
    :param path: Base path to strip
    """
    if path == "":
        return editor
    return PathStrippingEditor(editor, path)


class PathStrippingDirectoryEditor(object):
    """Directory editor that strips a base from the paths."""

    __slots__ = ('editor', 'path', 'actual')

    def __init__(self, editor, path, actual=None):
        self.editor = editor
        self.path = path
        self.actual = actual

    def open_directory(self, path, base_revnum):
        """See `DirectoryEditor`."""
        if path.strip("/") == self.editor.prefix:
            t = self.editor.actual.open_root(base_revnum)
        elif self.actual is not None:
            t = self.actual.open_directory(self.editor.strip_prefix(path),
                base_revnum)
        else:
            t = None
        return PathStrippingDirectoryEditor(self.editor, path, t)

    def add_directory(self, path, copyfrom_path=None, copyfrom_rev=-1):
        """See `DirectoryEditor`."""
        if path.strip("/") == self.editor.prefix:
            t = self.editor.actual.open_root(copyfrom_rev)
        elif self.actual is not None:
            t = self.actual.add_directory(self.editor.strip_prefix(path),
               self.editor.strip_copy_prefix(copyfrom_path), copyfrom_rev)
        else:
            t = None
        return PathStrippingDirectoryEditor(self.editor, path, t)

    def close(self):
        """See `DirectoryEditor`."""
        if self.actual is not None:
            self.actual.close()

    def change_prop(self, name, value):
        """See `DirectoryEditor`."""
        if self.actual is not None:
            self.actual.change_prop(name, value)

    def delete_entry(self, path, revnum):
        """See `DirectoryEditor`."""
        if self.actual is not None:
            self.actual.delete_entry(self.editor.strip_prefix(path), revnum)
        else:
            raise AssertionError("delete_entry should not be called")

    def add_file(self, path, copyfrom_path=None, copyfrom_rev=-1):
        """See `DirectoryEditor`."""
        if self.actual is not None:
            return self.actual.add_file(self.editor.strip_prefix(path),
                self.editor.strip_copy_prefix(copyfrom_path), copyfrom_rev)
        raise AssertionError("add_file should not be called")

    def open_file(self, path, base_revnum):
        """See `DirectoryEditor`."""
        if self.actual is not None:
            return self.actual.open_file(self.editor.strip_prefix(path),
                   base_revnum)
        raise AssertionError("open_file should not be called")


class PathStrippingEditor(object):
    """Editor that strips a base from the paths."""

    __slots__ = ('actual', 'prefix')

    def __init__(self, actual, path):
        self.actual = actual
        self.prefix = path.strip("/")

    def __getattr__(self, name):
        try:
            return getattr(super(PathStrippingEditor, self), name)
        except AttributeError:
            return getattr(self.actual, name)

    def strip_prefix(self, path):
        path = path.strip("/")
        if not path.startswith(self.prefix):
            raise AssertionError("Invalid path %r doesn't start with %r" % (
                path, self.prefix))
        return path[len(self.prefix):].strip("/")

    def strip_copy_prefix(self, path):
        if path is None:
            return None
        return self.strip_prefix(path)

    def open_root(self, base_revnum=None):
        return PathStrippingDirectoryEditor(self, "")

    def close(self):
        self.actual.close()

    def abort(self):
        self.actual.abort()

    def set_target_revision(self, rev):
        self.actual.set_target_revision(rev)


class DeltaBuildEditor(object):
    """Implementation of the Subversion commit editor interface that
    converts Subversion to Bazaar semantics.
    """

    __slots__ = ('revmeta', '_id_map', 'mapping')

    def __init__(self, revmeta, mapping):
        self.revmeta = revmeta
        self._id_map = None
        self.mapping = mapping

    def set_target_revision(self, revnum):
        if self.revmeta.revnum != revnum:
            raise AssertionError("Expected %d, got %d" % (
                self.revmeta.revnum, revnum))

    def open_root(self, base_revnum=None):
        return self._open_root(base_revnum)

    def close(self):
        pass

    def abort(self):
        pass


class DirectoryBuildEditor(object):

    __slots__ = ('editor', 'path')

    def __init__(self, editor, path):
        self.editor = editor
        self.path = path

    def close(self):
        if self.path == "":
            # If it has changed, it has definitely been reported by now
            if not self.editor.revmeta.knows_fileprops():
                self.editor.revmeta._fileprops = \
                    self.editor.revmeta.get_previous_fileprops()
        self._close()

    def add_directory(self, path, copyfrom_path=None, copyfrom_revnum=-1):
        assert isinstance(path, str)
        path = path.decode("utf-8")
        check_filename(path)
        return self._add_directory(path, copyfrom_path, copyfrom_revnum)

    def open_directory(self, path, base_revnum):
        assert isinstance(path, str)
        path = path.decode("utf-8")
        return self._open_directory(path, base_revnum)

    def absent_directory(self, path):
        raise AbsentPath(path)

    def absent_file(self, path):
        raise AbsentPath(path)

    def change_prop(self, name, value):
        if self.path == "":
            # Replay lazy_dict, since it may be more expensive
            if not self.editor.revmeta.knows_fileprops():
                self.editor.revmeta._fileprops = dict(
                    self.editor.revmeta.get_previous_fileprops())
            if value is None:
                if name in self.editor.revmeta._fileprops:
                    del self.editor.revmeta._fileprops[name]
            else:
                self.editor.revmeta._fileprops[name] = value

        if name in (properties.PROP_ENTRY_COMMITTED_DATE,
                    properties.PROP_ENTRY_COMMITTED_REV,
                    properties.PROP_ENTRY_LAST_AUTHOR,
                    properties.PROP_ENTRY_LOCK_TOKEN,
                    properties.PROP_ENTRY_UUID,
                    properties.PROP_EXECUTABLE):
            pass
        elif (name.startswith(properties.PROP_WC_PREFIX)):
            pass
        elif name.startswith(properties.PROP_PREFIX):
            trace.mutter('unsupported dir property %r', name)

    def add_file(self, path, copyfrom_path=None, copyfrom_revnum=-1):
        assert isinstance(path, str)
        path = path.decode("utf-8")
        check_filename(path)
        return self._add_file(path, copyfrom_path, copyfrom_revnum)

    def open_file(self, path, base_revnum):
        assert isinstance(path, str)
        path = path.decode("utf-8")
        return self._open_file(path, base_revnum)

    def delete_entry(self, path, revnum):
        assert isinstance(path, str)
        path = path.decode("utf-8")
        return self._delete_entry(path, revnum)


class FileBuildEditor(object):

    __slots__ = ('path', 'editor', 'is_executable', 'is_special',
                 'last_file_rev')

    def __init__(self, editor, path):
        self.path = path
        self.editor = editor
        self.is_executable = None
        self.is_special = None

    def apply_textdelta(self, base_checksum=None):
        return self._apply_textdelta(base_checksum)

    def change_prop(self, name, value):
        if name == properties.PROP_EXECUTABLE:
            # You'd expect executable to match
            # properties.PROP_EXECUTABLE_VALUE, but that's not
            # how SVN behaves. It appears to consider the presence
            # of the property sufficient to mark it executable.
            self.is_executable = (value is not None)
        elif name == properties.PROP_SPECIAL:
            self.is_special = (value != None)
        elif name == properties.PROP_ENTRY_COMMITTED_REV:
            self.last_file_rev = int(value)
        elif name == properties.PROP_EXTERNALS:
            trace.mutter('svn:externals property on file!')
        elif name in (properties.PROP_ENTRY_COMMITTED_DATE,
                      properties.PROP_ENTRY_LAST_AUTHOR,
                      properties.PROP_ENTRY_LOCK_TOKEN,
                      properties.PROP_ENTRY_UUID,
                      properties.PROP_MIME_TYPE):
            pass
        elif name.startswith(properties.PROP_WC_PREFIX):
            pass
        elif (name.startswith(properties.PROP_PREFIX) or
              name.startswith(SVN_PROP_BZR_PREFIX)):
            trace.mutter('unsupported file property %r', name)

    def close(self, checksum=None):
        assert isinstance(self.path, unicode)
        return self._close()


class DirectoryRevisionBuildEditor(DirectoryBuildEditor):

    __slots__ = ('new_id', 'bzr_base_path',
                 '_metadata_changed', '_renew_fileids', 'new_ie',
                 'svn_base_ie', 'bzr_base_ie')

    def __repr__(self):
        return "<%s for %s>" % (self.__class__.__name__, self.new_id)

    def __init__(self, editor, bzr_base_path, path, new_id,
            bzr_base_ie, svn_base_ie, parent_file_id, renew_fileids=None):
        super(DirectoryRevisionBuildEditor, self).__init__(editor, path)
        assert isinstance(new_id, str)
        self.new_id = new_id
        self.bzr_base_path = bzr_base_path
        self._metadata_changed = False
        self._renew_fileids = renew_fileids
        self.bzr_base_ie = bzr_base_ie
        self.svn_base_ie = svn_base_ie
        self.new_ie = InventoryDirectory(self.new_id,
                urlutils.basename(self.path), parent_file_id)
        if self.bzr_base_ie:
            self.new_ie.revision = self.bzr_base_ie.revision

    def _delete_entry(self, path, revnum):
        self.editor._delete_entry(self.svn_base_ie.file_id, path, revnum)

    def _close(self):
        if (not self.editor.bzr_base_tree.has_id(self.new_id) or
            (self._metadata_changed and self.path != u"") or
            self.new_ie != self.editor.bzr_base_tree.inventory[self.new_id] or
            self.bzr_base_path != self.path):
            self.new_ie.revision = self.editor._get_directory_revision(self.new_id)
            self.editor.texts.insert_record_stream([
                ChunkedContentFactory(
                (self.new_id, self.new_ie.revision),
                self.editor._get_text_parents(self.new_id), None,
                [])])
            self.editor._inv_delta_append(
                self.bzr_base_path, self.path, self.new_id, self.new_ie)
        if self._renew_fileids:
            # Make sure files get re-added that weren't mentioned explicitly
            self.editor._renew_fileids(self._renew_fileids.file_id)

        if self.path == u"":
            self.editor._finish_commit()

    def _add_directory(self, path, copyfrom_path=None, copyfrom_revnum=-1):
        file_id = self.editor._get_new_file_id(path)

        # This directory was moved here from somewhere else, but the
        # other location hasn't been removed yet.
        if copyfrom_path is not None:
            svn_base_ie = self.editor.get_svn_base_ie_copyfrom(copyfrom_path,
                                                      copyfrom_revnum)
        else:
            svn_base_ie = None

        if self.editor.bzr_base_tree.has_id(file_id):
            bzr_base_ie = self.editor.bzr_base_tree.inventory[file_id]
            bzr_base_path = self.editor.bzr_base_tree.id2path(file_id)
        else:
            bzr_base_path = None
            bzr_base_ie = None

        return DirectoryRevisionBuildEditor(self.editor, bzr_base_path, path,
            file_id, bzr_base_ie, svn_base_ie, self.new_id,
            [])

    def _open_directory(self, path, base_revnum):
        (svn_base_file_id, svn_base_ie) = self.editor.get_svn_base_ie_open(
            self.svn_base_ie.file_id, path, base_revnum)
        if self.bzr_base_ie:
            file_id = self.editor._get_existing_file_id(
                    self.bzr_base_ie.file_id, self.new_id, path)
        else:
            file_id = self.editor._get_new_file_id(path)
        if self.editor.bzr_base_tree.inventory.has_id(file_id):
            bzr_base_ie = self.editor.bzr_base_tree.inventory[file_id]
        else:
            bzr_base_ie = None

        if file_id == svn_base_file_id:
            bzr_base_path = path
            renew_fileids = None
        else:
            bzr_base_path = None
            self._delete_entry(path, base_revnum)
            # If a directory is replaced by a copy of itself, we need
            # to make sure all children get readded with a new file id
            renew_fileids = svn_base_ie
        return DirectoryRevisionBuildEditor(self.editor, bzr_base_path, path,
            file_id, bzr_base_ie, svn_base_ie, self.new_id,
            renew_fileids=renew_fileids)

    def _add_file(self, path, copyfrom_path=None, copyfrom_revnum=-1):
        file_id = self.editor._get_new_file_id(path)
        if self.editor.bzr_base_tree.has_id(file_id):
            bzr_base_path = self.editor.bzr_base_tree.inventory.id2path(file_id)
        else:
            bzr_base_path = None
        if copyfrom_path is not None:
            # Delta's will be against this text
            svn_base_ie = self.editor.get_svn_base_ie_copyfrom(copyfrom_path,
                    copyfrom_revnum)
        else:
            svn_base_ie = None
        return FileRevisionBuildEditor(self.editor, bzr_base_path, path,
                file_id, self.new_id, svn_base_ie)

    def _open_file(self, path, base_revnum):
        (svn_base_file_id, svn_base_ie) = self.editor.get_svn_base_ie_open(
            self.svn_base_ie.file_id, path, base_revnum)
        if self.bzr_base_ie:
            file_id = self.editor._get_existing_file_id(
                self.bzr_base_ie.file_id, self.new_id, path)
        else:
            file_id = self.editor._get_new_file_id(path)
        if file_id == svn_base_file_id:
            bzr_base_path = path
        else:
            # Replace with historical version
            bzr_base_path = None
            self._delete_entry(path, base_revnum)
        return FileRevisionBuildEditor(self.editor, bzr_base_path, path,
                file_id, self.new_id, svn_base_ie)


def chunks_start_with_link(chunks):
    # Shortcut for chunked:
    if not chunks:
        return False
    if len(chunks[0]) >= 5:
        return chunks[0].startswith("link ")
    return "".join(chunks[:6]).startswith('link ')


class FileRevisionBuildEditor(FileBuildEditor):

    __slots__ = ('bzr_base_path', 'file_id', 'is_symlink', 'svn_base_ie',
                 'base_chunks', 'chunks', 'parent_file_id')

    def __init__(self, editor, bzr_base_path, path, file_id, parent_file_id,
                 svn_base_ie):
        super(FileRevisionBuildEditor, self).__init__(editor, path)
        self.bzr_base_path = bzr_base_path
        self.file_id = file_id
        self.svn_base_ie = svn_base_ie
        if self.svn_base_ie is None:
            self.base_chunks = []
            self.is_symlink = False
        else:
            self.is_symlink = (svn_base_ie.kind == 'symlink')
            self.base_chunks = self.editor._get_chunked(self.svn_base_ie)
        self.chunks = None
        self.parent_file_id = parent_file_id

    def _apply_textdelta(self, base_checksum=None):
        if base_checksum is not None:
            actual_checksum = md5_strings(self.base_chunks)
            if base_checksum != actual_checksum:
                raise TextChecksumMismatch(base_checksum, actual_checksum,
                    self.editor.revmeta.branch_path,
                    self.editor.revmeta.revnum)
        self.chunks = []
        return apply_txdelta_handler_chunks(self.base_chunks, self.chunks)

    def _close(self, checksum=None):
        if self.chunks is not None:
            chunks = self.chunks
        else:
            # No delta was send, use the base chunks
            chunks = self.base_chunks
        md5sum = osutils.md5()
        shasum = osutils.sha()
        text_size = 0
        for chunk in chunks:
            md5sum.update(chunk)
            shasum.update(chunk)
            text_size += len(chunk)
        text_sha1 = shasum.hexdigest()
        if checksum is not None:
            actual_checksum = md5sum.hexdigest()
            if checksum != actual_checksum:
                raise TextChecksumMismatch(checksum, actual_checksum,
                        self.editor.revmeta.branch_path,
                        self.editor.revmeta.revnum)

        if self.is_special is not None:
            self.is_symlink = (self.is_special and chunks_start_with_link(chunks))
        elif chunks_start_with_link(chunks):
            # This file just might be a file that is svn:special but didn't
            # contain a symlink but does now
            if not self.is_symlink:
                pass # FIXME: Query whether this file has svn:special set.
        else:
            self.is_symlink = False

        assert self.is_symlink in (True, False)

        if self.is_executable is None:
            if self.svn_base_ie is not None:
                self.is_executable = self.svn_base_ie.executable
            else:
                self.is_executable = False

        parent_keys = self.editor._get_text_parents(self.file_id)

        orig_chunks = chunks
        if self.is_symlink:
            ie = InventoryLink(self.file_id, urlutils.basename(self.path),
                    self.parent_file_id)
            ie.symlink_target = "".join(chunks)[len("link "):].decode("utf-8")
            if "\n" in ie.symlink_target:
                raise AssertionError("bzr doesn't support newlines in symlink "
                                     "targets yet")
            chunks = []
            text_sha1 = osutils.sha_string("")
        else:
            ie = InventoryFile(self.file_id, urlutils.basename(self.path),
                    self.parent_file_id)
            ie.text_sha1 = text_sha1
            ie.text_size = text_size
            assert ie.text_size is not None
            ie.executable = self.is_executable

        text_revision = self.editor._get_file_revision(ie)

        file_key = (self.file_id, text_revision)
        cf = ChunkedContentFactory(file_key, parent_keys, text_sha1, chunks)

        self.editor._text_cache[file_key] = orig_chunks

        self.editor.texts.insert_record_stream([cf])
        ie.revision = text_revision
        self.editor._inv_delta_append(
            self.bzr_base_path, self.path, self.file_id, ie)


class RevisionBuildEditor(DeltaBuildEditor):
    """Implementation of the Subversion commit editor interface that builds a
    Bazaar revision.
    """

    def __repr__(self):
        return "<%s for %r in %r for %r>" % (self.__class__.__name__,
            self.revid, self.target, self.source)

    def __init__(self, source, target, revid, bzr_parent_trees, svn_base_tree,
            revmeta, mapping, text_cache):
        self.target = target
        self.source = source
        self.texts = target.texts
        self.revid = revid
        self._text_revids = None
        self._text_cache = text_cache
        self.bzr_base_tree = bzr_parent_trees[0]
        self.bzr_parent_trees = bzr_parent_trees
        self.svn_base_tree = svn_base_tree
        self._inv_delta = []
        self._new_file_paths = {}
        self._deleted = set()
        self._explicitly_deleted = set()
        self.inventory = None
        super(RevisionBuildEditor, self).__init__(revmeta, mapping)

    def _inv_delta_append(self, old_path, new_path, file_id, ie):
        self._new_file_paths[file_id] = new_path
        self._inv_delta.append((old_path, new_path, file_id, ie))

    def _delete_entry(self, old_parent_id, path, revnum):
        self._explicitly_deleted.add(path)
        def rec_del(ie):
            p = self.bzr_base_tree.inventory.id2path(ie.file_id)
            if p in self._deleted:
                return
            self._deleted.add(p)
            if ie.file_id not in self.id_map.values():
                self._inv_delta_append(p, None, ie.file_id, None)
            if ie.kind != 'directory':
                return
            for c in ie.children.values():
                rec_del(c)
        base_file_id = self._get_bzr_base_file_id(old_parent_id, path)
        rec_del(self.bzr_base_tree.inventory[base_file_id])

    def get_svn_base_ie_open(self, parent_id, path, revnum):
        """Find the inventory entry against which svn is sending the
        changes to generate the current one.

        :param parent_id: File id of the parent
        :param path: Path as it exists in the current revision
        :param revnum: Revision number from which to open
        :return: Tuple with file id and inventory entry
        """
        assert type(path) is unicode
        assert (type(parent_id) is str or (parent_id is None and path == ""))
        if path == u"":
            lhs_parent_revmeta = self.revmeta.get_lhs_parent_revmeta(
                self.mapping)
            if lhs_parent_revmeta is not None:
                try:
                    (action, copyfrom_path, copyfrom_revnum, kind) = self.revmeta.paths.get(
                        self.revmeta.branch_path)
                except TypeError:
                    copyfrom_path = None
                if copyfrom_path is None:
                    svn_base_path = "."
                else:
                    # Map copyfrom_path to the path that's related to the lhs parent branch path.
                    prev_locations = self.source.transport.get_locations(
                        copyfrom_path, copyfrom_revnum, [lhs_parent_revmeta.revnum])
                    copyfrom_path = prev_locations[lhs_parent_revmeta.revnum].strip("/")
                    svn_base_path = urlutils.determine_relative_path(
                        lhs_parent_revmeta.branch_path, copyfrom_path)
                if svn_base_path == ".":
                    svn_base_path = ""
            else:
                svn_base_path = ""
            file_id = self.svn_base_tree.path2id(svn_base_path)
        else:
            file_id = inventory_parent_id_basename_to_file_id(
                self.svn_base_tree.inventory, parent_id,
                urlutils.basename(path))
        return (file_id, self.svn_base_tree.inventory[file_id])

    def get_svn_base_ie_copyfrom(self, path, revnum):
        """Look up the base ie for the svn path, revnum.

        :param path: Path to look up, relative to the current branch path
        :param revnum: Revision number for which to lookup the path
        """
        # Find the ancestor of self.revmeta with revnum revnum
        last_revmeta = None
        for revmeta, mapping in self.source._iter_reverse_revmeta_mapping_history(
            self.revmeta.branch_path, self.revmeta.revnum, revnum, self.mapping):
            last_revmeta = revmeta
        assert last_revmeta is not None and last_revmeta.revnum == revnum
        revid = last_revmeta.get_revision_id(mapping)
        # TODO: Use InterRepository._get_tree() for better performance,
        # as it does (some) caching ?
        inv = self.target.get_inventory(revid)
        file_id = inv.path2id(path)
        return inv[file_id]

    def _get_chunked(self, ie):
        if ie.kind == 'symlink':
            return ("link ", ie.symlink_target.encode("utf-8"))
        key = (ie.file_id, ie.revision)
        file_data = self._text_cache.get(key)
        if file_data is not None:
            return file_data
        return self.target.iter_files_bytes([key + (None,)]).next()[1]

    def _add_merge_texts(self):
        def get_new_file_path(file_id):
            try:
                return self._new_file_paths[file_id]
            except KeyError:
                base_ie = self.bzr_base_tree.inventory[file_id]
                if base_ie.parent_id is None:
                    parent_path = ""
                else:
                    parent_path = get_new_file_path(base_ie.parent_id)
                return urlutils.join(parent_path, base_ie.name)

        # Add dummy merge text revisions
        for file_id in self.bzr_base_tree.all_file_ids():
            if file_id in self._new_file_paths:
                continue
            base_ie = self.bzr_base_tree.inventory[file_id]
            for tree in self.bzr_parent_trees[1:]:
                try:
                    parent_ie = tree.inventory[file_id]
                except NoSuchId:
                    continue
                if parent_ie.revision != base_ie.revision:
                    new_ie = base_ie.copy()
                    new_ie.revision = self.revid
                    record = self.texts.get_record_stream([
                        (base_ie.file_id, base_ie.revision)], 'unordered', True).next()
                    self.texts.insert_record_stream(
                            [FulltextContentFactory(
                                (new_ie.file_id, new_ie.revision),
                                self._get_text_parents(file_id),
                                record.sha1,
                                record.get_bytes_as('fulltext'))])
                    self._inv_delta_append(self.bzr_base_tree.id2path(file_id), get_new_file_path(file_id), file_id, new_ie)
                    break

    def _finish_commit(self):
        if len(self.bzr_parent_trees) > 1:
            self._add_merge_texts()
        rev = self.revmeta.get_revision(self.mapping)
        assert rev.revision_id != NULL_REVISION
        try:
            assert rev.parent_ids[0] != NULL_REVISION
            assert rev.parent_ids[0] == self.bzr_base_tree.get_revision_id(), \
                "revision lhs parent %s does not match base tree revid %s" % \
                 (rev.parent_ids, self.bzr_base_tree.get_revision_id())
            basis_id = rev.parent_ids[0]
            basis_inv = self.bzr_base_tree.inventory
        except IndexError:
            basis_id = NULL_REVISION
            basis_inv = None
        present_parent_ids = self.target.has_revisions(rev.parent_ids)
        rev.inventory_sha1, self.inventory = \
            self.target.add_inventory_by_delta(basis_id, self._inv_delta,
            rev.revision_id,
            tuple([r for r in rev.parent_ids if r in present_parent_ids]),
            basis_inv)
        self.target.add_revision(self.revid, rev)

        # Only fetch signature if it's cheap
        if self.source.transport.has_capability("log-revprops"):
            signature = self.revmeta.get_signature()
            if signature is not None:
                self.target.add_signature_text(self.revid, signature)

    def _open_root(self, base_revnum):
        assert self.revid is not None

        if self.svn_base_tree.inventory.root is None:
            svn_base_ie = None
            svn_base_file_id = None
        else:
            (svn_base_file_id, svn_base_ie) = self.get_svn_base_ie_open(
                None, u"", base_revnum)

        if self.bzr_base_tree.inventory.root is None:
            # First time the root is set
            bzr_base_ie = None
            file_id = self._get_new_file_id(u"")
            renew_fileids = None
            bzr_base_path = None
        else:
            # Just inherit file id from previous
            file_id = self._get_existing_file_id(None, None, u"")
            if self.bzr_base_tree.inventory.has_id(file_id):
                bzr_base_ie = self.bzr_base_tree.inventory[file_id]
                bzr_base_path = self.bzr_base_tree.id2path(file_id)
            else:
                bzr_base_path = None
                bzr_base_ie = None
            if bzr_base_path != u"":
                self._delete_entry(None, u"", base_revnum)
                renew_fileids = None
            elif file_id == svn_base_file_id:
                renew_fileids = None
            else:
                self._delete_entry(None, u"", base_revnum)
                renew_fileids = svn_base_ie
        assert isinstance(file_id, str)

        return DirectoryRevisionBuildEditor(self, bzr_base_path, u"",
                file_id, bzr_base_ie, svn_base_ie, None, renew_fileids)

    def _renew_fileids(self, file_id):
        # A bit expensive (O(d)), but this should be very rare
        delta_new_paths = set(
            [e[1] for e in self._inv_delta if e[1] is not None])
        exceptions = delta_new_paths.union(self._explicitly_deleted)
        for path in inventory_ancestors(self.bzr_base_tree.inventory,
                file_id, exceptions):
            if isinstance(path, str):
                path = path.decode("utf-8")
            self._renew_fileid(path)

    def _renew_fileid(self, path):
        """'renew' the fileid of a path."""
        assert isinstance(path, unicode)
        old_file_id = self.bzr_base_tree.path2id(path)
        old_ie = self.bzr_base_tree.inventory[old_file_id]
        new_ie = old_ie.copy()
        new_ie.file_id = self._get_new_file_id(path)
        new_ie.parent_id = self._get_new_file_id(urlutils.dirname(path))
        new_ie.revision = self.revid
        # FIXME: Use self._text_cache
        record = self.texts.get_record_stream([(old_ie.file_id, old_ie.revision)],
                                                'unordered', True).next()
        self.texts.insert_record_stream(
                [FulltextContentFactory(
                    (new_ie.file_id, new_ie.revision),
                    [], # New file id, so no parents
                    record.sha1,
                    record.get_bytes_as('fulltext'))])
        self._inv_delta_append(self.bzr_base_tree.path2id(new_ie.file_id),
            path, new_ie.file_id, new_ie)

    @property
    def id_map(self):
        if self._id_map is not None:
            return self._id_map

        local_changes = get_local_changes(self.revmeta.paths,
            self.revmeta.branch_path)
        self._id_map = self.source.fileid_map.get_idmap_delta(local_changes,
            self.revmeta, self.mapping)

        return self._id_map

    def _get_map_id(self, new_path):
        return self.id_map.get(new_path)

    def _get_bzr_base_file_id(self, parent_id, path):
        assert isinstance(path, unicode)
        assert (isinstance(parent_id, str) or
                (parent_id is None and path == ""))
        basename = urlutils.basename(path)
        return inventory_parent_id_basename_to_file_id(
            self.bzr_base_tree.inventory, parent_id, basename)

    def _get_existing_file_id(self, old_parent_id, new_parent_id, path):
        assert isinstance(path, unicode)
        assert isinstance(old_parent_id, str) or old_parent_id is None
        assert isinstance(new_parent_id, str) or new_parent_id is None
        ret = self._get_map_id(path)
        if ret is not None:
            return ret
        # If there was no explicit mention of this file id in the map, then
        # this file_id can only stay the same if the parent file id
        # didn't change
        if old_parent_id == new_parent_id:
            return self._get_bzr_base_file_id(old_parent_id, path)
        else:
            return self._get_new_file_id(path)

    def _get_new_file_id(self, new_path):
        assert isinstance(new_path, unicode)
        ret = self._get_map_id(new_path)
        if ret is not None:
            return ret
        return self.mapping.generate_file_id(
            self.revmeta.get_foreign_revid(), new_path)

    def _get_directory_revision(self, file_id):
        return self.revid # FIXME: For now

    def _get_file_revision(self, ie):
        if (ie.file_id in self.bzr_base_tree.inventory or
            len(self.bzr_parent_trees) <= 1):
            # File was touched but not newly introduced since base so it has
            # changed somehow.
            return self.revid

        # See if the parent trees already have this exact
        # inventory entry.
        revision = None
        for tree in self.bzr_parent_trees[1:]:
            try:
                parent_ie = tree.inventory[ie.file_id]
            except NoSuchId:
                continue
            else:
                if revision is not None and revision != parent_ie.revision:
                    # Disagreement
                    return self.revid
                tmp_ie = ie.copy()
                tmp_ie.revision = parent_ie.revision
                if tmp_ie != parent_ie:
                    return self.revid
                revision = tmp_ie.revision
        if revision is None:
            return self.revid
        else:
            return revision

    def _get_text_parents(self, file_id):
        assert isinstance(file_id, str)
        ret = []
        for tree in self.bzr_parent_trees:
            try:
                revision = tree.get_file_revision(file_id)
            except NoSuchId:
                pass
            else:
                if not revision in ret:
                    ret.append((file_id, revision))
        return ret


class FileTreeDeltaBuildEditor(FileBuildEditor):

    __slots__ = ('copyfrom_path', 'base_checksum', 'change_kind')

    def __init__(self, editor, path, copyfrom_path, kind):
        super(FileTreeDeltaBuildEditor, self).__init__(editor, path)
        self.copyfrom_path = copyfrom_path
        self.base_checksum = None
        self.change_kind = kind

    def _close(self, checksum=None):
        text_changed = (self.base_checksum != checksum)
        metadata_changed = (self.is_special is not None or
                            self.is_executable is not None)
        if self.is_special:
            # FIXME: A special file doesn't necessarily mean a symlink
            # we need to fetch it and see if it starts with "link "...
            entry_kind = 'symlink'
        else:
            entry_kind = 'file'
        if self.change_kind == 'add':
            if (self.copyfrom_path is not None and
                self._get_map_id(self.path) is not None):
                self.editor.delta.renamed.append((self.copyfrom_path,
                    self.path, self.editor._get_new_file_id(self.path),
                    entry_kind, text_changed, metadata_changed))
            else:
                self.editor.delta.added.append((self.path,
                    self.editor._get_new_file_id(self.path), entry_kind))
        else:
            self.editor.delta.modified.append((self.path,
                self.editor._get_new_file_id(self.path), entry_kind,
                text_changed, metadata_changed))

    def _apply_textdelta(self, base_checksum=None):
        self.base_checksum = None
        return lambda window: None


class DirectoryTreeDeltaBuildEditor(DirectoryBuildEditor):

    __slots__ = ()

    def _close(self):
        pass

    def _open_directory(self, path, base_revnum):
        return DirectoryTreeDeltaBuildEditor(self.editor, path)

    def _open_file(self, path, base_revnum):
        return FileTreeDeltaBuildEditor(self.editor, path, None, 'open')

    def _add_file(self, path, copyfrom_path=None, copyfrom_revnum=-1):
        return FileTreeDeltaBuildEditor(self.editor, path, copyfrom_path,
            'add')

    def _delete_entry(self, path, revnum):
        # FIXME: old kind
        self.editor.delta.removed.append((path,
            self.editor._get_bzr_base_file_id(path), 'unknown'))

    def _add_directory(self, path, copyfrom_path=None, copyfrom_revnum=-1):
        if (copyfrom_path is not None and
            self.editor._was_renamed(path) is not None):
            self.editor.delta.renamed.append((copyfrom_path, path,
                self.editor._get_new_file_id(path), 'directory', False, False))
        else:
            self.editor.delta.added.append((path,
                self.editor._get_new_file_id(path), 'directory'))
        return DirectoryTreeDeltaBuildEditor(self.editor, path)


class TreeDeltaBuildEditor(DeltaBuildEditor):
    """Implementation of the Subversion commit editor interface that builds a
    Bazaar TreeDelta.
    """

    def __init__(self, revmeta, mapping, newfileidmap, oldfileidmap):
        super(TreeDeltaBuildEditor, self).__init__(revmeta, mapping)
        self._parent_idmap = oldfileidmap
        self._idmap = newfileidmap
        self.delta = delta.TreeDelta()
        self.delta.unversioned = []
        # To make sure we fall over if anybody tries to use it:
        self.delta.unchanged = None

    def _open_root(self, base_revnum):
        return DirectoryTreeDeltaBuildEditor(self, u"")

    def _was_renamed(self, path):
        fileid = self._get_new_file_id(path)
        return self._parent_idmap.has_fileid(fileid)

    def _get_bzr_base_file_id(self, path):
        return self._parent_idmap.lookup(self.mapping, path)[0]

    def _get_new_file_id(self, path):
        return self._idmap.lookup(self.mapping, path)[0]


@convert_svn_error
def report_inventory_contents(reporter, revnum, start_empty):
    """Report the contents of a Bazaar inventory to a Subversion reporter.

    :param reporter: Subversion reporter
    :param revnum: Revision number the report is for
    :param start_empty: Whether or not to start with an empty tree.
    """
    try:
        reporter.set_path("", revnum, start_empty)
    except:
        reporter.abort()
        raise
    else:
        reporter.finish()


class FetchRevisionFinder(object):
    """Simple object that can gather a list of revmeta, mapping tuples
    to fetch."""

    def __init__(self, source, target, target_is_empty=False):
        self.source = source
        self.target = target
        self.target_is_empty = target_is_empty
        self.needed = list()
        self.checked = set()
        self.extra = list()
        self._check_present_interval = 1

    def get_missing(self, limit=None):
        """Return the revisions that should be fetched, children before parents.
        """
        if limit is not None:
            return self.needed[:limit]
        return self.needed

    def check_revmetas(self, revmetas):
        if self.target_is_empty:
            return revmetas
        ret = set()
        map = {}
        for revmeta, mapping in revmetas:
            try:
                map[revmeta, mapping] = revmeta.get_revision_id(mapping)
            except SubversionException, (_, num):
                if num == ERR_FS_NOT_DIRECTORY:
                    continue
                else:
                    raise
        present_revids = self.target.has_revisions(map.values())
        return [k for k in revmetas if k in map and map[k] not in present_revids]

    def find_iter_revisions(self, iter, master_mapping, needs_manual_check,
                            heads=None, pb=None):
        """Find revisions to fetch based on an iterator over available revmetas.

        :param iter: Iterator over RevisionMetadata objects
        :param master_mapping: Mapping to use
        """
        if heads is None:
            needed_mappings = defaultdict(lambda: set([master_mapping]))
        else:
            needed_mappings = defaultdict(set)
            for head in heads:
                needed_mappings[head].add(master_mapping)
        needs_checking = []
        for i, revmeta in enumerate(iter):
            if pb is not None:
                pb.update("checking revisions to fetch", i)
            for m in needed_mappings[revmeta]:
                try:
                    (m, lhsm) = revmeta.get_appropriate_mappings(m)
                except SubversionException, (_, num):
                    if num == ERR_FS_NOT_DIRECTORY:
                        continue
                    else:
                        raise
                if (m != master_mapping and
                    not m.is_branch_or_tag(revmeta.branch_path)):
                    continue
                lhs_parent_revmeta = revmeta.get_lhs_parent_revmeta(m)
                if (lhs_parent_revmeta is not None and
                    needs_manual_check(lhs_parent_revmeta)):
                    self.needed.extend(self.find_mainline(
                        lhs_parent_revmeta.get_foreign_revid(), lhsm))
                if lhsm != master_mapping or heads is not None:
                    needed_mappings[lhs_parent_revmeta].add(lhsm)
                if not revmeta.is_hidden(m):
                    needs_checking.append((revmeta, m))
        self.needed.extend(reversed(self.check_revmetas(needs_checking)))

    def find_all(self, mapping):
        """Find all revisions from the source repository that are not
        yet in the target repository.

        :return: List with revmeta, mapping tuples to fetch
        """
        from_revnum = self.source.get_latest_revnum()
        pb = ui.ui_factory.nested_progress_bar()
        try:
            all_revs = self.source._revmeta_provider.iter_all_revisions(
                    self.source.get_layout(),
                    check_unusual_path=mapping.is_branch_or_tag,
                    from_revnum=from_revnum, pb=pb)
            return self.find_iter_revisions(all_revs, mapping, lambda x: False)
        finally:
            pb.finished()

    def find_mainline(self, foreign_revid, mapping, find_ghosts=False):
        if (foreign_revid, mapping) in self.checked:
            return []
        revmetas = deque()
        (uuid, branch_path, revnum) = foreign_revid
        # TODO: Do binary search to find first revision to fetch if
        # fetch_ghosts=False ?
        needs_checking = []
        pb = ui.ui_factory.nested_progress_bar()
        try:
            for revmeta, mapping in self.source._iter_reverse_revmeta_mapping_history(
                branch_path, revnum, to_revnum=0, mapping=mapping):
                if pb:
                    pb.update("determining revisions to fetch",
                              revnum-revmeta.revnum, revnum)
                if (revmeta.get_foreign_revid(), mapping) in self.checked:
                    # This revision (and its ancestry) has already been checked
                    break
                needs_checking.append((revmeta, mapping))
                if (not find_ghosts and not self.target_is_empty and
                    len(needs_checking) == self._check_present_interval):
                    missing_revmetas = self.check_revmetas(needs_checking)
                    for r in missing_revmetas:
                        revmetas.appendleft(r)
                    done = (len(missing_revmetas) != len(needs_checking))
                    needs_checking = []
                    if done:
                        break
                    self._check_present_interval = min(
                        self._check_present_interval*2, MAX_CHECK_PRESENT_INTERVAL)
                self.checked.add((revmeta.get_foreign_revid(), mapping))
        finally:
            pb.finished()
        for r in self.check_revmetas(needs_checking):
            revmetas.appendleft(r)
        # Determine if there are any RHS parents to fetch
        self.extra.extend(self.find_rhs_parents(revmetas))
        return revmetas

    def find_rhs_parents(self, revmetas):
        for revmeta, mapping in revmetas:
            for p in revmeta.get_rhs_parents(mapping):
                try:
                    foreign_revid, rhs_mapping = self.source.lookup_bzr_revision_id(p, foreign_sibling=revmeta.get_foreign_revid())
                except NoSuchRevision:
                    pass # Ghost
                else:
                    if self.source.has_foreign_revision(foreign_revid):
                        yield (foreign_revid, rhs_mapping)

    def find_until(self, foreign_revid, mapping, find_ghosts=False):
        """Find all missing revisions until revision_id

        :param revision_id: Stop revision
        :param find_ghosts: Find ghosts
        :return: List with revmeta, mapping tuples to fetch
        """
        self.extra.append((foreign_revid, mapping))
        while len(self.extra) > 0:
            foreign_revid, mapping = self.extra.pop()
            self.needed.extend(self.find_mainline(foreign_revid, mapping,
                find_ghosts=find_ghosts))


class InterFromSvnRepository(InterRepository):
    """Svn to any repository actions."""

    _matching_repo_format = SvnRepositoryFormat()

    @staticmethod
    def _get_repo_format_to_test():
        return None

    def __init__(self, source, target):
        super(InterFromSvnRepository, self).__init__(source, target)
        def chunks_to_size(chunks):
            return sum(map(len, chunks))
        self._text_cache = lru_cache.LRUSizeCache(TEXT_CACHE_SIZE,
                                                  compute_size=chunks_to_size)

        self._use_replay_range = self.source.transport.has_capability(
            "partial-replay") and False
        self._use_replay = self.source.transport.has_capability(
            "partial-replay") and False

    def copy_content(self, revision_id=None, pb=None):
        """See InterRepository.copy_content."""
        self.fetch(revision_id, pb, find_ghosts=False)

    def _get_tree(self, revid):
        """Retrieve an inventory, optionally using a inventory previously
        cached.

        :param revid: Revision id to use.
        """
        if self._prev_tree is not None and self._prev_tree.get_revision_id() == revid:
            return self._prev_tree
        if "check" in debug.debug_flags:
            # This uses 'assert' rather than raising AssertionError
            # intentionally, it's a fairly expensive check.
            assert self.target.has_revision(revid)
        return self.target.revision_tree(revid)

    def _inconsistent_lhs_parent(self, revid, stored_lhs_parent_revid,
            found_lhs_parent_revid):
        trace.warning(
            "Recorded left hand side parent %s for revision "
            "%s does not match actual left hand side parent "
            "%s.", revid, stored_lhs_parent_revid, found_lhs_parent_revid)
        self.fetch(found_lhs_parent_revid)

    def _get_parent_trees(self, revmeta, mapping):
        parent_revids = revmeta.get_parent_ids(mapping)
        if not parent_revids:
            parent_revids = (NULL_REVISION,)
        # We always need the base inventory for the first parent
        parent_trees = [self.target.revision_tree(parent_revids[0])]
        for revid in parent_revids[1:]:
            try:
                tree = self.target.revision_tree(revid)
            except NoSuchRevision:
                trace.mutter("Parent tree %s is a ghost.", revid)
            else:
                parent_trees.append(tree)
        return parent_trees

    def _get_editor(self, revmeta, mapping):
        revid = revmeta.get_revision_id(mapping)
        assert revid is not None
        svn_base_revid = revmeta.get_implicit_lhs_parent_revid(mapping)
        try:
            bzr_parent_trees = self._get_parent_trees(revmeta, mapping)
            svn_base_tree = bzr_parent_trees[0]
            return RevisionBuildEditor(self.source, self.target, revid,
                bzr_parent_trees, svn_base_tree,
                revmeta, mapping, self._text_cache)
        except NoSuchRevision:
            lhs_parent_revmeta = revmeta.get_lhs_parent_revmeta(mapping)
            if revmeta.get_stored_lhs_parent_revid(mapping) not in (
                None, svn_base_revid):
                self._inconsistent_lhs_parent(revmeta.get_revision_id(mapping),
                    revmeta.get_stored_lhs_parent_revid(mapping),
                    svn_base_revid)
            raise

    def _fetch_revision_switch(self, editor, revmeta, parent_revmeta):
        """Fetch a revision using svn.ra.do_switch() or svn.ra.do_update().

        :param editor: Editor to report changes to
        :param revmeta: RevisionMetadata object for the revision to fetch
        :param parent_revmeta: RevisionMetadata object for the parent revision.
        """
        if parent_revmeta is None:
            parent_branch = revmeta.branch_path
            parent_revnum = revmeta.revnum
            start_empty = True
        else:
            parent_branch = parent_revmeta.branch_path
            parent_revnum = parent_revmeta.revnum
            start_empty = False

        conn = self.source.transport.get_connection(parent_branch)
        try:
            assert revmeta.revnum > parent_revnum or start_empty

            if parent_branch != revmeta.branch_path:
                reporter = conn.do_switch(revmeta.revnum, "", True,
                    url_join_unescaped_path(conn.get_repos_root(), revmeta.branch_path),
                    editor)
            else:
                reporter = conn.do_update(revmeta.revnum, "", True, editor)

            try:
                report_inventory_contents(reporter, parent_revnum, start_empty)
            except SubversionException, (_, num):
                if num != subvertpy.ERR_FS_PATH_SYNTAX:
                    raise
                # This seems to occur sometimes when we try to accidently
                # import a file over HTTP
                if conn.check_path("", revmeta.revnum) == NODE_FILE:
                    raise SubversionException("path is a file", ERR_FS_NOT_DIRECTORY)
                raise
        finally:
            if not conn.busy:
                self.source.transport.add_connection(conn)

    def _fetch_revision_replay(self, editor, revmeta, parent_revmeta):
        assert revmeta.revnum >= 0
        conn = self.source.transport.get_connection(revmeta.branch_path)
        try:
            conn.replay(revmeta.revnum, 0, editor_strip_prefix(editor, revmeta.branch_path),
                True)
        finally:
            if not conn.busy:
                self.source.transport.add_connection(conn)

    def _fetch_revisions_nochunks(self, revs, pb=None, use_replay=False):
        """Copy a set of related revisions using svn.ra.switch.

        :param revids: List of revision ids of revisions to copy,
                       newest first.
        :param pb: Optional progress bar.
        """
        accidental_file_revs = set()
        self._prev_tree = None
        batch_size = 100
        total = len(revs)
        pack_hints = []
        for offset in range(0, total, batch_size):
            self.target.start_write_group()
            try:
                for num, (revmeta, mapping) in enumerate(revs[offset:offset+batch_size]):
                    try:
                        revid = revmeta.get_revision_id(mapping)
                    except SubversionException, (_, ERR_FS_NOT_DIRECTORY):
                        continue
                    assert revid != NULL_REVISION
                    if pb is not None:
                        pb.update('copying revision', offset+num, total)

                    parent_revmeta = revmeta.get_lhs_parent_revmeta(mapping)
                    if parent_revmeta in accidental_file_revs:
                        accidental_file_revs.add(revmeta)
                        continue

                    editor = self._get_editor(revmeta, mapping)
                    try:
                        if use_replay:
                            self._fetch_revision_replay(editor, revmeta,
                                                        parent_revmeta)
                        else:
                            try:
                                self._fetch_revision_switch(editor, revmeta,
                                                            parent_revmeta)
                            except SubversionException, (_, ERR_FS_NOT_DIRECTORY):
                                accidental_file_revs.add(revmeta)
                                editor.abort()
                                continue
                    except:
                        editor.abort()
                        raise
                    self._prev_tree = InventoryRevisionTree(self.target,
                        editor.inventory, revid)
            except:
                self.target.abort_write_group()
                raise
            else:
                hint = self.target.commit_write_group()
                if hint is not None:
                    pack_hints.extend(hint)
        return pack_hints

    def _fetch_revisions(self, needed, pb):
        """Fetch a specified set of revisions.

        :param needed: Sequence of revision ids to fetch, topo-sorted
        :param pb: Progress bar to use for reporting progress
        :return: Pack hint
        """
        if self._use_replay_range:
            return self._fetch_revisions_chunks(needed, pb)
        else:
            return self._fetch_revisions_nochunks(needed, pb,
                use_replay=self._use_replay)

    def get_revision_finder(self, target_is_empty=False):
        return FetchRevisionFinder(self.source, self.target, target_is_empty)

    def _get_needed(self, revision_id=None, fetch_spec=None, project=None,
                    target_is_empty=False, find_ghosts=False):
        """Find the set of revisions that is missing.

        :note: revision_id and fetch_spec are mutually exclusive

        :param revision_id: Optional target revision id
        :param fetch_spec: Specifier describing the revisions to fetch
        :param project: Project name as used by the repository layout, if applicable
        :param target_is_empty: Whether the target is empty
            (aka: should has_revision be called)
        :param pb: Optional progress bar to use.
        :param find_ghosts: Whether to find ghosts
        :return: Iterable over missing revision ids
        """
        revisionfinder = self.get_revision_finder(target_is_empty)
        if revision_id is not None:
            foreign_revid, mapping = self.source.lookup_bzr_revision_id(
                revision_id, project=project)
            revisionfinder.find_until(foreign_revid, mapping,
                                      find_ghosts=find_ghosts)
        elif fetch_spec is not None:
            recipe = fetch_spec.get_recipe()
            if recipe[0] in ("search", "proxy-search"):
                heads = recipe[1]
            else:
                raise AssertionError("Unsupported search result type %s" % recipe[0])
            for head in heads:
                foreign_revid, mapping = self.source.lookup_bzr_revision_id(
                    head, project=project)
                revisionfinder.find_until(foreign_revid, mapping,
                    find_ghosts=find_ghosts)
        else:
            revisionfinder.find_all(self.source.get_mapping())
        return revisionfinder.get_missing()

    def fetch(self, revision_id=None, pb=None, find_ghosts=False,
              needed=None, mapping=None, project=None, fetch_spec=None,
              target_is_empty=False):
        """Fetch revisions. """
        if revision_id == NULL_REVISION:
            return
        # Dictionary with paths as keys, revnums as values

        if pb:
            pb.update("fetch phase", 0, 2)

        # Loop over all the revnums until revision_id
        # (or youngest_revnum) and call self.target.add_revision()
        # or self.target.add_inventory() each time
        self.target.lock_write()
        try:
            if needed is None:
                needed = self._get_needed(target_is_empty=target_is_empty,
                    revision_id=revision_id, fetch_spec=fetch_spec,
                    find_ghosts=find_ghosts, project=project)

            if len(needed) == 0:
                # Nothing to fetch
                return

            if pb:
                pb.update("fetch phase", 1, 2)

            if pb is None:
                pb = ui.ui_factory.nested_progress_bar()
                nested_pb = pb
            else:
                nested_pb = None
            try:
                pack_hint = self._fetch_revisions(needed, pb)
                if (pack_hint is not None and
                    self.target._format.pack_compresses):
                    self.target.pack(hint=pack_hint)
            finally:
                if nested_pb is not None:
                    nested_pb.finished()
            # Double check that we can actually find the revision that we
            # attempted to fetch.
            # This uses 'assert' rather than raising AssertionError
            # intentionally, it's a fairly expensive check.
            assert revision_id is None or self.target.has_revision(
                revision_id)
        finally:
            self.target.unlock()

    def _fetch_revisions_chunks(self, revs, pb=None):
        """Copy a set of related revisions using svn.ra.replay.

        :param revids: Revision ids to copy.
        :param pb: Optional progress bar
        """
        self._prev_inv = None
        activeranges = defaultdict(list)
        pb = ui.ui_factory.nested_progress_bar()
        try:
            for i, (revmeta, mapping) in enumerate(revs):
                pb.update("determining revision ranges", i, len(revs))
                p = revmeta.get_direct_lhs_parent_revmeta()
                range = activeranges[p,mapping]
                range.append(revmeta)
                del activeranges[p,mapping]
                activeranges[revmeta,mapping] = range
        finally:
            pb.finished()

        def cmprange((ak, av),(bk, bv)):
            return cmp(av[0], bv[0])
        ranges = sorted(activeranges.iteritems(), cmp=cmprange)

        self.target.start_write_group()
        try:
            for (head, mapping), revmetas in ranges:
                def revstart(revnum, revprops):
                    if pb is not None:
                        pb.update("fetching revisions", revnum, len(revs))
                    revmeta = None
                    for r in revmetas:
                        if r.revnum == revnum:
                            revmeta = r
                            break
                    revmeta._revprops = revprops
                    return editor_strip_prefix(
                        self._get_editor(revmeta, mapping),
                        revmeta.branch_path)

                def revfinish(revision, revprops, editor):
                    assert editor.inventory is not None
                    self._prev_inv = editor.inventory

                conn = self.source.transport.get_connection(revmetas[-1].branch_path)
                try:
                    conn.replay_range(revmetas[0].revnum, revmetas[-1].revnum, 0, (revstart, revfinish), True)
                finally:
                    if not conn.busy:
                        self.source.transport.add_connection(conn)
        except:
            self.target.abort_write_group()
            raise
        else:
            return self.target.commit_write_group()

    @staticmethod
    def is_compatible(source, target):
        """Be compatible with SvnRepository."""
        if not isinstance(source, SvnRepository):
            return False
        if not target.supports_rich_root():
            return False
        if not getattr(target._format, "supports_full_versioned_files", True):
            return False
        return True

