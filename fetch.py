# Copyright (C) 2005-2007 Jelmer Vernooij <jelmer@samba.org>

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
"""Fetching revisions from Subversion repositories in batches."""

from bzrlib import debug, delta, osutils, ui, urlutils
from bzrlib.errors import NoSuchRevision
from bzrlib.inventory import (
        Inventory, 
        InventoryDirectory, 
        InventoryFile,
        InventoryLink,
        )
from bzrlib.osutils import md5
from bzrlib.revision import NULL_REVISION
from bzrlib.repository import InterRepository
from bzrlib.trace import mutter

from collections import deque, defaultdict
from cStringIO import StringIO

from subvertpy import properties, SubversionException
from subvertpy.delta import apply_txdelta_handler

from bzrlib.plugins.svn.errors import InvalidFileName
from bzrlib.plugins.svn.foreign import escape_commit_message
from bzrlib.plugins.svn.mapping import SVN_PROP_BZR_PREFIX
from bzrlib.plugins.svn.repository import SvnRepository, SvnRepositoryFormat
from bzrlib.plugins.svn.revmeta import iter_with_mapping
from bzrlib.plugins.svn.transport import _url_escape_uri

FETCH_COMMIT_WRITE_SIZE = 500

def md5_strings(strings):
    """Return the MD5sum of the concatenation of strings.

    :param strings: Strings to find the MD5sum of.
    :return: MD5sum
    """
    s = md5()
    map(s.update, strings)
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
    if path == "":
        return editor
    return PathStrippingEditor(editor, path)


class PathStrippingDirectoryEditor(object):

    def __init__(self, editor, path, actual=None):
        self.editor = editor
        self.path = path
        self.actual = actual

    def open_directory(self, path, base_revnum):
        if path.strip("/") == self.editor.prefix:
            t = self.editor.actual.open_root(base_revnum)
        elif self.actual is not None:
            t = self.actual.open_directory(self.editor.strip_prefix(path), base_revnum)
        else:
            t = None
        return PathStrippingDirectoryEditor(self.editor, path, t)

    def add_directory(self, path, copyfrom_path=None, copyfrom_rev=-1):
        if path.strip("/") == self.editor.prefix:
            t = self.editor.actual.open_root(copyfrom_rev)
        elif self.actual is not None:
            t = self.actual.add_directory(self.editor.strip_prefix(path), 
               self.editor.strip_copy_prefix(copyfrom_path), copyfrom_rev)
        else:
            t = None
        return PathStrippingDirectoryEditor(self.editor, path, t)

    def close(self):
        if self.actual is not None:
            self.actual.close()

    def change_prop(self, name, value):
        if self.actual is not None:
            self.actual.change_prop(name, value)

    def delete_entry(self, path, revnum):
        if self.actual is not None:
            self.actual.delete_entry(self.editor.strip_prefix(path), revnum)
        else:
            raise AssertionError("delete_entry should not be called")

    def add_file(self, path, copyfrom_path=None, copyfrom_rev=-1):
        if self.actual is not None:
            return self.actual.add_file(self.editor.strip_prefix(path),
                self.editor.strip_copy_prefix(copyfrom_path), copyfrom_rev)
        raise AssertionError("add_file should not be called")

    def open_file(self, path, base_revnum):
        if self.actual is not None:
            return self.actual.open_file(self.editor.strip_prefix(path),
                   base_revnum)
        raise AssertionError("open_file should not be called")


class PathStrippingEditor(object):

    def __init__(self, actual, path):
        self.actual = actual
        self.prefix = path.strip("/")

    def __getattr__(self, name):
        return getattr(super(PathStrippingEditor, self), name, getattr(self.actual, name))

    def strip_prefix(self, path):
        path = path.strip("/")
        if not path.startswith(self.prefix):
            raise AssertionError("Invalid path %r doesn't start with %r" % (path, self.prefix))
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
    def __init__(self, revmeta, mapping):
        self.revmeta = revmeta
        self._id_map = None
        self.mapping = mapping

    def set_target_revision(self, revnum):
        assert self.revmeta.revnum == revnum, "Expected %d, got %d" % (self.revmeta.revnum, revnum)

    def open_root(self, base_revnum=None):
        return self._open_root(base_revnum)

    def close(self):
        pass

    def abort(self):
        pass


class DirectoryBuildEditor(object):
    def __init__(self, editor, path):
        self.editor = editor
        self.path = path

    def close(self):
        if self.path == "":
            # If it has changed, it has definitely been reported by now
            if not self.editor.revmeta.knows_fileprops():
                if self.editor.revmeta._fileprops.dict is None:
                    self.editor.revmeta._fileprops.dict = dict(self.editor.revmeta.get_previous_fileprops())
                self.editor.revmeta._fileprops.is_loaded = True
                self.editor.revmeta._fileprops.create_fn = None
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

    def change_prop(self, name, value):
        if self.path == "":
            # Replay lazy_dict, since it may be more expensive
            if not self.editor.revmeta.knows_fileprops():
                self.editor.revmeta._fileprops.dict = dict(self.editor.revmeta.get_previous_fileprops())
                self.editor.revmeta._fileprops.is_loaded = True
                self.editor.revmeta._fileprops.create_fn = None
            self.editor.revmeta._fileprops.dict[name] = value

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
            mutter('unsupported dir property %r', name)

        if (not name.startswith(properties.PROP_ENTRY_PREFIX) and
            not name.startswith(properties.PROP_WC_PREFIX)):
            self._metadata_changed = True

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
        elif (name == properties.PROP_SPECIAL):
            self.is_special = (value != None)
        elif name == properties.PROP_ENTRY_COMMITTED_REV:
            self.last_file_rev = int(value)
        elif name == properties.PROP_EXTERNALS:
            mutter('svn:externals property on file!')
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
            mutter('unsupported file property %r', name)

    def close(self, checksum=None):
        assert isinstance(self.path, unicode)
        return self._close()


class DirectoryRevisionBuildEditor(DirectoryBuildEditor):

    def __init__(self, editor, old_path, path, old_id, new_id, parent_file_id, parent_revids=[]):
        super(DirectoryRevisionBuildEditor, self).__init__(editor, path)
        assert isinstance(new_id, str)
        self.old_id = old_id
        self.new_id = new_id
        self.old_path = old_path
        self.parent_revids = parent_revids
        self._metadata_changed = False
        self.new_ie = InventoryDirectory(self.new_id, urlutils.basename(self.path), parent_file_id)
        if self.editor.old_inventory.has_id(self.new_id):
            self.new_ie.revision = self.editor.old_inventory[self.new_id].revision

    def _delete_entry(self, path, revnum):
        def rec_del(ie):
            self.editor._inv_delta.append((self.editor.old_inventory.id2path(ie.file_id), None, ie.file_id, None))
            if ie.kind != 'directory':
                return
            for c in ie.children.values():
                rec_del(c)
        rec_del(self.editor.old_inventory[self.editor._get_old_id(self.old_id, path)])

    def _close(self):
        if (not self.editor.old_inventory.has_id(self.new_id) or 
            (self._metadata_changed and self.path != "") or 
            self.new_ie != self.editor.old_inventory[self.new_id] or
            self.old_path != self.path or 
            self.editor._get_text_revid(self.path) is not None):
            assert self.editor.revid is not None

            text_revision = self.editor._get_text_revid(self.path) or self.editor.revid
            self.new_ie.revision = text_revision
            text_parents = self.editor._get_text_parents(self.path)
            if text_parents is None:
                text_parents = self.parent_revids
            self.editor.texts.add_lines(
                (self.new_id, text_revision),
                [(self.new_id, revid) for revid in text_parents], [])
            self.editor._inv_delta.append((self.old_path, self.path, self.new_id, self.new_ie))

        if self.path == "":
            self.editor._finish_commit()

    def _add_directory(self, path, copyfrom_path=None, copyfrom_revnum=-1):
        file_id = self.editor._get_new_id(path)

        if self.editor.old_inventory.has_id(file_id):
            # This directory was moved here from somewhere else, but the 
            # other location hasn't been removed yet. 
            if copyfrom_path is None:
                # This should ideally never happen!
                copyfrom_path = self.editor.old_inventory.id2path(file_id)
                mutter('no copyfrom path set, assuming %r', copyfrom_path)
            assert copyfrom_path == self.editor.old_inventory.id2path(file_id)
            old_path = copyfrom_path
            old_file_id = file_id
        else:
            old_path = None
            old_file_id = None

        return DirectoryRevisionBuildEditor(self.editor, old_path, path, old_file_id, file_id, self.new_id, [])

    def _open_directory(self, path, base_revnum):
        base_file_id = self.editor._get_old_id(self.old_id, path)
        base_revid = self.editor.old_inventory[base_file_id].revision
        file_id = self.editor._get_existing_id(self.old_id, self.new_id, path)
        if file_id == base_file_id:
            file_parents = [base_revid]
            old_path = path
        else:
            old_path = None
            file_parents = []
            self._delete_entry(path, base_revnum)
        return DirectoryRevisionBuildEditor(self.editor, old_path, path, base_file_id, file_id, self.new_id, file_parents)

    def _add_file(self, path, copyfrom_path=None, copyfrom_revnum=-1):
        file_id = self.editor._get_new_id(path)
        if self.editor.old_inventory.has_id(file_id):
            # This file was moved here from somewhere else, but the 
            # other location hasn't been removed yet. 
            if copyfrom_path is None:
                # This should ideally never happen
                copyfrom_path = self.editor.old_inventory.id2path(file_id)
                mutter('no copyfrom path set, assuming %r', copyfrom_path)
            assert copyfrom_path == self.editor.old_inventory.id2path(file_id)
            # No need to rename if it's already in the right spot
            old_path = copyfrom_path
        else:
            old_path = None
        # TODO: Retrieve base text here when used by replay/replay_range()
        return FileRevisionBuildEditor(self.editor, old_path, path, 
                                       file_id, self.new_id)

    def _get_record_stream(self, file_id, revision_id):
        return self.editor.texts.get_record_stream([(file_id, revision_id)], 
                                                   'unordered', True).next()

    def _open_file(self, path, base_revnum):
        base_file_id = self.editor._get_old_id(self.old_id, path)
        base_revid = self.editor.old_inventory[base_file_id].revision
        file_id = self.editor._get_existing_id(self.old_id, self.new_id, path)
        base_ie = self.editor.old_inventory[base_file_id]
        is_symlink = (base_ie.kind == 'symlink')
        record = self._get_record_stream(base_file_id, base_revid)
        file_data = record.get_bytes_as('fulltext')
        if file_id == base_file_id:
            file_parents = [base_revid]
            old_path = path
        else:
            # Replace with historical version
            old_path = None
            file_parents = []
            self._delete_entry(path, base_revnum)
        return FileRevisionBuildEditor(self.editor, old_path, path, file_id, self.new_id,
                file_parents, file_data, is_symlink=is_symlink)


class FileRevisionBuildEditor(FileBuildEditor):
    def __init__(self, editor, old_path, path, file_id, parent_file_id, file_parents=[], data="", 
                 is_symlink=False):
        super(FileRevisionBuildEditor, self).__init__(editor, path)
        self.old_path = old_path
        self.file_id = file_id
        self.file_data = data
        self.is_symlink = is_symlink
        self.file_parents = file_parents
        self.file_stream = None
        self.parent_file_id = parent_file_id

    def _apply_textdelta(self, base_checksum=None):
        actual_checksum = md5(self.file_data).hexdigest()
        assert (base_checksum is None or base_checksum == actual_checksum,
            "base checksum mismatch: %r != %r" % (base_checksum, 
                                                  actual_checksum))
        self.file_stream = StringIO()
        return apply_txdelta_handler(self.file_data, self.file_stream)

    def _close(self, checksum=None):
        if self.file_stream is not None:
            self.file_stream.seek(0)
            lines = osutils.split_lines(self.file_stream.read())
        else:
            # Data didn't change or file is new
            lines = osutils.split_lines(self.file_data)

        actual_checksum = md5_strings(lines)
        assert checksum is None or checksum == actual_checksum

        text_revision = self.editor._get_text_revid(self.path) or self.editor.revid
        text_parents = self.editor._get_text_parents(self.path)
        if text_parents is None:
            text_parents = self.file_parents
        self.editor.texts.add_lines((self.file_id, text_revision), 
                [(self.file_id, revid) for revid in text_parents], lines)

        if self.is_special is not None:
            self.is_symlink = (self.is_special and len(lines) > 0 and lines[0].startswith("link "))
        elif (len(lines) > 0 and lines[0].startswith("link ")):
            # This file just might be a file that is svn:special but didn't contain a symlink
            # but does now
            if not self.is_symlink:
                pass # FIXME: Query whether this file has svn:special set.
        else:
            self.is_symlink = False

        assert self.is_symlink in (True, False)

        if self.editor.old_inventory.has_id(self.file_id):
            if self.is_executable is None:
                self.is_executable = self.editor.old_inventory[self.file_id].executable

        if self.is_symlink:
            ie = InventoryLink(self.file_id, urlutils.basename(self.path), self.parent_file_id)
            ie.symlink_target = "".join(lines)[len("link "):]
            if "\n" in ie.symlink_target:
                raise AssertionError("bzr doesn't support newlines in symlink targets yet")
            ie.text_sha1 = None
            ie.text_size = None
            ie.executable = False
        else:
            ie = InventoryFile(self.file_id, urlutils.basename(self.path), self.parent_file_id)
            ie.symlink_target = None
            ie.text_sha1 = osutils.sha_strings(lines)
            ie.text_size = sum(map(len, lines))
            assert ie.text_size is not None
            ie.executable = self.is_executable
        ie.revision = text_revision
        self.editor._inv_delta.append((self.old_path, self.path, self.file_id, ie))

        self.file_stream = None


class RevisionBuildEditor(DeltaBuildEditor):
    """Implementation of the Subversion commit editor interface that builds a 
    Bazaar revision.
    """
    def __init__(self, source, target, revid, prev_inventory, revmeta, mapping):
        self.target = target
        self.source = source
        self.texts = target.texts
        self.revid = revid
        self._text_revids = None
        self._text_parents = None
        self.old_inventory = prev_inventory
        self._inv_delta = []
        super(RevisionBuildEditor, self).__init__(revmeta, mapping)

    def _finish_commit(self):
        rev = self.revmeta.get_revision(self.mapping)
        # Escaping the commit message is really the task of the serialiser
        rev.message = escape_commit_message(rev.message)
        if getattr(self.target, "add_inventory_delta", None) is not None:
            try:
                basis_id = rev.parent_ids[0]
            except IndexError:
                basis_id = NULL_REVISION
            rev.inventory_sha1, self.inventory = self.target.add_inventory_delta(basis_id,
                                  self._inv_delta, rev.revision_id,
                                  [r for r in rev.parent_ids if self.target.has_revision(r)])
        else:
            self.inventory = self.old_inventory
            self.inventory.apply_delta(self._inv_delta)
            self.inventory.revision_id = rev.revision_id

            rev.inventory_sha1 = self.target.add_inventory(rev.revision_id, 
                    self.inventory, rev.parent_ids)
        self.target.add_revision(self.revid, rev)

        # Only fetch signature if it's cheap
        if self.source.transport.has_capability("log-revprops"):
            signature = self.revmeta.get_signature()
            if signature is not None:
                self.target.add_signature_text(self.revid, signature)

    def _open_root(self, base_revnum):
        assert self.revid is not None
        if self.old_inventory.root is None:
            # First time the root is set
            old_file_id = None
            file_id = self.mapping.generate_file_id(self.revmeta.uuid, self.revmeta.revnum, self.revmeta.branch_path, u"")
            file_parents = []
        else:
            assert self.old_inventory.root.revision is not None
            old_file_id = self.old_inventory.root.file_id
            file_id = self._get_id_map().get("", old_file_id)
            file_parents = [self.old_inventory.root.revision]

        assert isinstance(file_id, str)

        if self.old_inventory.root is not None and \
                file_id == self.old_inventory.root.file_id:
            old_path = ""
        else:
            old_path = None
        return DirectoryRevisionBuildEditor(self, old_path, "", old_file_id, file_id, None, file_parents)

    def _get_id_map(self):
        if self._id_map is not None:
            return self._id_map

        self._id_map = self.source.fileid_map.apply_changes(self.revmeta, self.mapping)[0]

        return self._id_map

    def _get_map_id(self, new_path):
        return self._get_id_map().get(new_path)

    def _get_old_id(self, parent_id, old_path):
        assert isinstance(old_path, unicode)
        assert isinstance(parent_id, str)
        basename = urlutils.basename(old_path)
        parent_id_basename_index = getattr(self.old_inventory, "parent_id_basename_to_file_id", None)
        if parent_id_basename_index is None:
            return self.old_inventory[parent_id].children[basename].file_id
        else:
            ret = parent_id_basename_index.iteritems([(parent_id, basename)])
            return ret.next()[1]

    def _get_existing_id(self, old_parent_id, new_parent_id, path):
        assert isinstance(path, unicode)
        assert isinstance(old_parent_id, str)
        assert isinstance(new_parent_id, str)
        ret = self._get_id_map().get(path)
        if ret is not None:
            return ret
        return self._get_old_id(old_parent_id, path)

    def _get_new_id(self, new_path):
        assert isinstance(new_path, unicode)
        ret = self._get_map_id(new_path)
        if ret is not None:
            return ret
        return self.mapping.generate_file_id(self.revmeta.uuid, self.revmeta.revnum, 
                                             self.revmeta.branch_path, new_path)

    def _get_text_revid(self, path):
        if self._text_revids is None:
            self._text_revids = self.mapping.import_text_revisions(self.revmeta.get_revprops(), self.revmeta.get_changed_fileprops())
        return self._text_revids.get(path)

    def _get_text_parents(self, path):
        if self._text_parents is None:
            self._text_parents = self.mapping.import_text_parents(self.revmeta.get_revprops(), self.revmeta.get_changed_fileprops())
        return self._text_parents.get(path)


class FileTreeDeltaBuildEditor(FileBuildEditor):

    def __init__(self, editor, path, copyfrom_path, kind):
        super(FileTreeDeltaBuildEditor, self).__init__(editor, path)
        self.copyfrom_path = copyfrom_path
        self.base_checksum = None
        self.change_kind = kind

    def _close(self, checksum=None):
        text_changed = (self.base_checksum != checksum)
        metadata_changed = (self.is_special is not None or self.is_executable is not None)
        if self.is_special:
            # FIXME: A special file doesn't necessarily mean a symlink
            # we need to fetch it and see if it starts with "link "...
            entry_kind = 'symlink'
        else:
            entry_kind = 'file'
        if self.change_kind == 'add':
            if self.copyfrom_path is not None and self._get_map_id(self.path) is not None:
                self.editor.delta.renamed.append((self.copyfrom_path, self.path, self.editor._get_new_id(self.path), entry_kind, 
                                                 text_changed, metadata_changed))
            else:
                self.editor.delta.added.append((self.path, self.editor._get_new_id(self.path), entry_kind))
        else:
            self.editor.delta.modified.append((self.path, self.editor._get_new_id(self.path), entry_kind, text_changed, metadata_changed))

    def _apply_textdelta(self, base_checksum=None):
        self.base_checksum = None
        return lambda window: None


class DirectoryTreeDeltaBuildEditor(DirectoryBuildEditor):

    def _close(self):
        pass

    def _open_directory(self, path, base_revnum):
        return DirectoryTreeDeltaBuildEditor(self.editor, path)

    def _open_file(self, path, base_revnum):
        return FileTreeDeltaBuildEditor(self.editor, path, None, 'open')

    def _add_file(self, path, copyfrom_path=None, copyfrom_revnum=-1):
        return FileTreeDeltaBuildEditor(self.editor, path, copyfrom_path, 'add')

    def _delete_entry(self, path, revnum):
        # FIXME: old kind
        self.editor.delta.removed.append((path, self.editor._get_old_id(path), 'unknown'))

    def _add_directory(self, path, copyfrom_path=None, copyfrom_revnum=-1):
        if copyfrom_path is not None and self.editor._was_renamed(path) is not None:
            self.editor.delta.renamed.append((copyfrom_path, path, self.editor._get_new_id(path), 'directory', False, False))
        else:
            self.editor.delta.added.append((path, self.editor._get_new_id(path), 'directory'))
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
        return DirectoryTreeDeltaBuildEditor(self, "")

    def _was_renamed(self, path):
        fileid = self._get_new_id(path)
        for fid, _ in self._parent_idmap.values():
            if fileid == fid:
                return True
        return False

    def _get_old_id(self, path):
        return self._parent_idmap[path][0]

    def _get_new_id(self, path):
        return self._idmap[path][0]


def report_inventory_contents(reporter, revnum, start_empty):
    try:
        reporter.set_path("", revnum, start_empty)
    except:
        reporter.abort()
        raise
    reporter.finish()


class FetchRevisionFinder(object):
    """Simple object that can gather a list of revmeta, mapping tuples
    to fetch."""

    def __init__(self, source, target, target_is_empty):
        self.source = source
        self.target = target
        self.target_is_empty = target_is_empty
        self.checked = set()

    def needs_fetching(self, revmeta, mapping):
        try:
            if revmeta.is_hidden(mapping):
                return False
            if self.target_is_empty:
                return True
            return not self.target.has_revision(revmeta.get_revision_id(mapping))
        except SubversionException, (_, ERR_FS_NOT_DIRECTORY):
            return False

    def find_iter(self, iter, master_mapping, heads=None, pb=None):
        needed = deque()
        if heads is None:
            needed_mappings = defaultdict(lambda: set([master_mapping]))
        else:
            needed_mappings = defaultdict(set)
            for head in heads:
                needed_mappings[head].add(master_mapping)
        for i, revmeta in enumerate(iter):
            if pb is not None:
                pb.update("checking revisions to fetch", i)
            for m in needed_mappings[revmeta]:
                try:
                    (m, lhsm) = revmeta.get_appropriate_mappings(m)
                except SubversionException, (_, ERR_FS_NOT_DIRECTORY):
                    continue
                if (m != master_mapping and 
                    not m.is_branch_or_tag(revmeta.branch_path)):
                    continue
                if self.needs_fetching(revmeta, m):
                    if lhsm != master_mapping or heads is not None:
                        needed_mappings[revmeta.get_direct_lhs_parent_revmeta()].add(lhsm)
                    needed.appendleft((revmeta, m))
                    self.checked.add((revmeta.get_foreign_revid(), m))

        return needed

    def find_all(self, mapping, pb=None):
        """Find all revisions from the source repository that are not 
        yet in the target repository.

        :return: List with revmeta, mapping tuples to fetch
        """
        from_revnum = self.source.get_latest_revnum()
        return self.find_iter(self.source._revmeta_provider.iter_all_revisions(self.source.get_layout(), check_unusual_path=mapping.is_branch_or_tag, from_revnum=from_revnum, pb=pb), mapping)

    def find_until(self, foreign_revid, mapping, find_ghosts=False, pb=None,
                    project=None):
        """Find all missing revisions until revision_id

        :param revision_id: Stop revision
        :param find_ghosts: Find ghosts
        :return: List with revmeta, mapping tuples to fetch
        """
        extra = list()
        def check_revid(foreign_revid, mapping, project=None):
            if (foreign_revid, mapping) in self.checked:
                return []
            revmetas = deque()
            (uuid, branch_path, revnum) = foreign_revid
            # TODO: Do binary search to find first revision to fetch if
            # fetch_ghosts=False ?
            for revmeta, mapping in iter_with_mapping(self.source._revmeta_provider.iter_reverse_branch_changes(
                branch_path, revnum, to_revnum=0), mapping):
                if pb:
                    pb.update("determining revisions to fetch", 
                              revnum-revmeta.revnum, revnum)
                if (revmeta.get_foreign_revid(), mapping) in self.checked:
                    # This revision (and its ancestry) has already been checked
                    break
                if self.needs_fetching(revmeta, mapping):
                    revmetas.appendleft((revmeta, mapping))
                    for p in revmeta.get_rhs_parents(mapping):
                        try:
                            foreign_revid, rhs_mapping = self.source.lookup_revision_id(p, project=project)
                        except NoSuchRevision:
                            pass # Ghost
                        else:
                            extra.append((foreign_revid, project, rhs_mapping))
                elif not find_ghosts:
                    break
                self.checked.add((revmeta.get_foreign_revid(), mapping))
            return revmetas

        needed = check_revid(foreign_revid, mapping, project)

        while len(extra) > 0:
            foreign_revid, project, mapping = extra.pop()
            needed.extend(check_revid(foreign_revid, mapping, project))

        return needed


class InterFromSvnRepository(InterRepository):
    """Svn to any repository actions."""

    _matching_repo_format = SvnRepositoryFormat()

    _supports_revmetas = True

    @staticmethod
    def _get_repo_format_to_test():
        return None

    def copy_content(self, revision_id=None, pb=None):
        """See InterRepository.copy_content."""
        self.fetch(revision_id, pb, find_ghosts=False)

    def _get_inventory(self, revid):
        """Retrieve an inventory, optionally using a inventory previously 
        cached.

        :param revid: Revision id to use.
        """
        if revid == NULL_REVISION:
            return Inventory(root_id=None)
        if self._prev_inv is not None and self._prev_inv.revision_id == revid:
            return self._prev_inv
        if "validate" in debug.debug_flags:
            assert self.target.has_revision(revid)
        return self.target.get_inventory(revid)

    def _get_editor(self, revmeta, mapping):
        revid = revmeta.get_revision_id(mapping)
        assert revid is not None
        return RevisionBuildEditor(self.source, self.target, revid, 
            self._get_inventory(revmeta.get_lhs_parent_revid(mapping)), 
            revmeta, mapping)

    def _fetch_revision_switch(self, editor, revmeta, parent_revmeta):
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
                    _url_escape_uri(urlutils.join(conn.get_repos_root(), revmeta.branch_path)), 
                    editor)
            else:
                reporter = conn.do_update(revmeta.revnum, "", True, editor)

            report_inventory_contents(reporter, parent_revnum, start_empty)
        finally:
            if not conn.busy:
                self.source.transport.add_connection(conn)

    def _fetch_revision_replay(self, editor, revmeta, parent_revmeta):
        assert revmeta.revnum >= 0
        conn = self.source.transport.get_connection(revmeta.branch_path)
        try:
            conn.replay(revmeta.revnum, 0, editor_strip_prefix(editor, revmeta.branch_path), True)
        finally:
            if not conn.busy:
                self.source.transport.add_connection(conn)

    def _fetch_revisions(self, revs, pb=None, use_replay=False):
        """Copy a set of related revisions using svn.ra.switch.

        :param revids: List of revision ids of revisions to copy, 
                       newest first.
        :param pb: Optional progress bar.
        """
        self._prev_inv = None

        for num, (revmeta, mapping) in enumerate(revs):
            try:
                revid = revmeta.get_revision_id(mapping)
            except SubversionException, (_, ERR_FS_NOT_DIRECTORY):
                continue
            assert revid != NULL_REVISION
            if pb is not None:
                pb.update('copying revision', num, len(revs))

            parent_revmeta = revmeta.get_lhs_parent_revmeta(mapping)

            if not self.target.is_in_write_group():
                self.target.start_write_group()
            try:
                editor = self._get_editor(revmeta, mapping)
                try:
                    if use_replay:
                        self._fetch_revision_replay(editor, revmeta, 
                                                    parent_revmeta)
                    else:
                        self._fetch_revision_switch(editor, revmeta, 
                                                    parent_revmeta)
                except:
                    editor.abort()
                    raise
            except:
                if self.target.is_in_write_group():
                    self.target.abort_write_group()
                raise
            if num % FETCH_COMMIT_WRITE_SIZE == 0:
                self.target.commit_write_group()

            self._prev_inv = editor.inventory
            assert self._prev_inv.revision_id == revid
        if self.target.is_in_write_group():
            self.target.commit_write_group()

    def fetch(self, revision_id=None, pb=None, find_ghosts=False, 
              needed=None, mapping=None):
        """Fetch revisions. """
        if revision_id == NULL_REVISION:
            return
        # Dictionary with paths as keys, revnums as values

        if pb:
            pb.update("determining revisions to fetch", 0, 2)

        use_replay_range = self.source.transport.has_capability("partial-replay") and False
        use_replay = self.source.transport.has_capability("partial-replay") and False

        # Loop over all the revnums until revision_id
        # (or youngest_revnum) and call self.target.add_revision() 
        # or self.target.add_inventory() each time
        self.target.lock_write()
        try:
            nested_pb = ui.ui_factory.nested_progress_bar()
            try:
                # FIXME: Specify target_is_empt
                target_is_empty = False
                revisionfinder = FetchRevisionFinder(self.source, self.target, target_is_empty)
                if needed is None:
                    if revision_id is None:
                        needed = revisionfinder.find_all(self.source.get_mapping(), pb=nested_pb)
                    else:
                        foreign_revid, mapping = self.source.lookup_revision_id(revision_id)
                        needed = revisionfinder.find_until(foreign_revid, mapping, find_ghosts, pb=nested_pb)
            finally:
                nested_pb.finished()

            if len(needed) == 0:
                # Nothing to fetch
                return

            if pb is None:
                pb = ui.ui_factory.nested_progress_bar()
                nested_pb = pb
            else:
                nested_pb = None
            try:
                if use_replay_range:
                    self._fetch_revision_chunks(needed, pb)
                else:
                    self._fetch_revisions(needed, pb, use_replay=use_replay)
            finally:
                if nested_pb is not None:
                    nested_pb.finished()
        finally:
            if self.target.is_in_write_group():
                self.target.abort_write_group()
            self.target.unlock()

    def _fetch_revision_chunks(self, revs, pb=None):
        """Copy a set of related revisions using svn.ra.replay.

        :param revids: Revision ids to copy.
        :param pb: Optional progress bar
        """
        self._prev_inv = None
        currange = None
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

        if not self.target.is_in_write_group():
            self.target.start_write_group()

        try:
            def cmprange((ak, av),(bk, bv)):
                return cmp(av[0], bv[0])
            ranges = sorted(activeranges.iteritems(), cmp=cmprange)
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
                    return editor_strip_prefix(self._get_editor(revmeta, mapping), revmeta.branch_path)

                def revfinish(revision, revprops, editor):
                    self._prev_inv = editor.inventory

                conn = self.source.transport.get_connection(revmetas[-1].branch_path)
                try:
                    conn.replay_range(revmetas[0].revnum, revmetas[-1].revnum, 0, (revstart, revfinish), True)
                finally:
                    if not conn.busy:
                        self.source.transport.add_connection(conn)

                if i % FETCH_COMMIT_WRITE_SIZE == 0:
                    self.target.commit_write_group()
        finally:
            if self.target.is_in_write_group():
                self.target.commit_write_group()

    @staticmethod
    def is_compatible(source, target):
        """Be compatible with SvnRepository."""
        # FIXME: Also check target uses VersionedFile
        return isinstance(source, SvnRepository) and target.supports_rich_root()

