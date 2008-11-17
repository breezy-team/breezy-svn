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
from bzrlib.inventory import Inventory
from bzrlib.osutils import md5
from bzrlib.revision import NULL_REVISION
from bzrlib.repository import InterRepository
from bzrlib.trace import mutter

from cStringIO import StringIO

from subvertpy import properties
from subvertpy.delta import apply_txdelta_handler

from bzrlib.plugins.svn.errors import InvalidFileName
from bzrlib.plugins.svn.foreign import escape_commit_message
from bzrlib.plugins.svn.mapping import SVN_PROP_BZR_PREFIX
from bzrlib.plugins.svn.repository import SvnRepository, SvnRepositoryFormat
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


class DeltaBuildEditor(object):
    """Implementation of the Subversion commit editor interface that 
    converts Subversion to Bazaar semantics.
    """
    def __init__(self, revmeta, mapping):
        self.revmeta = revmeta
        self._id_map = None
        self.mapping = mapping

    def set_target_revision(self, revnum):
        assert self.revmeta.revnum == revnum

    def open_root(self, base_revnum=None):
        if base_revnum is None:
            base_revnum = self.revmeta.revnum
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
        self._close()

    def add_directory(self, path, copyfrom_path=None, copyfrom_revnum=-1):
        assert isinstance(path, str)
        path = path.decode("utf-8")
        check_filename(path)
        return self._add_directory(path, copyfrom_path, copyfrom_revnum)

    def open_directory(self, path, base_revnum):
        assert isinstance(path, str)
        path = path.decode("utf-8")
        assert base_revnum >= 0
        return self._open_directory(path, base_revnum)

    def change_prop(self, name, value):
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
    def __init__(self, editor, path, old_id, new_id, parent_revids=[]):
        super(DirectoryRevisionBuildEditor, self).__init__(editor, path)
        assert isinstance(new_id, str)
        self.old_id = old_id
        self.new_id = new_id
        self.parent_revids = parent_revids
        self._metadata_changed = False

    def _delete_entry(self, path, revnum):
        if path in self.editor._premature_deletes:
            # Delete recursively
            self.editor._premature_deletes.remove(path)
            for p in self.editor._premature_deletes.copy():
                if p.startswith("%s/" % path):
                    self.editor._premature_deletes.remove(p)
        else:
            self.editor.inventory.remove_recursive_id(self.editor._get_old_id(self.old_id, path))

    def _close(self):
        if (not self.new_id in self.editor.old_inventory or 
            (self._metadata_changed and self.path != "") or 
            self.editor.inventory[self.new_id] != self.editor.old_inventory[self.new_id] or
            self.editor._get_text_revid(self.path) is not None):
            ie = self.editor.inventory[self.new_id]
            assert self.editor.revid is not None
            ie.revision = self.editor.revid

            text_revision = self.editor._get_text_revid(self.path) or ie.revision
            text_parents = self.editor._get_text_parents(self.path)
            if text_parents is None:
                text_parents = self.parent_revids
            self.editor.texts.add_lines(
                (self.new_id, text_revision),
                [(self.new_id, revid) for revid in text_parents], [])

        if self.new_id == self.editor.inventory.root.file_id:
            assert self.editor.inventory.root.revision is not None
            self.editor._finish_commit()

    def _add_directory(self, path, copyfrom_path=None, copyfrom_revnum=-1):
        file_id = self.editor._get_new_id(path)

        if file_id in self.editor.inventory:
            # This directory was moved here from somewhere else, but the 
            # other location hasn't been removed yet. 
            if copyfrom_path is None:
                # This should ideally never happen!
                copyfrom_path = self.editor.old_inventory.id2path(file_id)
                mutter('no copyfrom path set, assuming %r', copyfrom_path)
            assert copyfrom_path == self.editor.old_inventory.id2path(file_id)
            assert copyfrom_path not in self.editor._premature_deletes
            self.editor._premature_deletes.add(copyfrom_path)
            self.editor._rename(file_id, self.new_id, copyfrom_path, path, 'directory')
            ie = self.editor.inventory[file_id]
            old_file_id = file_id
        else:
            old_file_id = None
            ie = self.editor.inventory.add_path(path, 'directory', file_id)

        return DirectoryRevisionBuildEditor(self.editor, path, old_file_id, file_id)

    def _open_directory(self, path, base_revnum):
        base_file_id = self.editor._get_old_id(self.old_id, path)
        base_revid = self.editor.old_inventory[base_file_id].revision
        file_id = self.editor._get_existing_id(self.old_id, self.new_id, path)
        if file_id == base_file_id:
            file_parents = [base_revid]
            ie = self.editor.inventory[file_id]
        else:
            # Replace if original was inside this branch
            # change id of base_file_id to file_id
            ie = self.editor.inventory[base_file_id]
            for name in ie.children:
                ie.children[name].parent_id = file_id
            # FIXME: Don't touch inventory internals
            del self.editor.inventory._byid[base_file_id]
            self.editor.inventory._byid[file_id] = ie
            ie.file_id = file_id
            file_parents = []
        return DirectoryRevisionBuildEditor(self.editor, path, base_file_id, file_id, 
                                    file_parents)

    def _add_file(self, path, copyfrom_path=None, copyfrom_revnum=-1):
        file_id = self.editor._get_new_id(path)
        if file_id in self.editor.inventory:
            # This file was moved here from somewhere else, but the 
            # other location hasn't been removed yet. 
            if copyfrom_path is None:
                # This should ideally never happen
                copyfrom_path = self.editor.old_inventory.id2path(file_id)
                mutter('no copyfrom path set, assuming %r', copyfrom_path)
            assert copyfrom_path == self.editor.old_inventory.id2path(file_id)
            assert copyfrom_path not in self.editor._premature_deletes
            self.editor._premature_deletes.add(copyfrom_path)
            # No need to rename if it's already in the right spot
            self.editor._rename(file_id, self.new_id, copyfrom_path, path, 'file')
        return FileRevisionBuildEditor(self.editor, path, file_id)

    def _open_file(self, path, base_revnum):
        base_file_id = self.editor._get_old_id(self.old_id, path)
        base_revid = self.editor.old_inventory[base_file_id].revision
        file_id = self.editor._get_existing_id(self.old_id, self.new_id, path)
        is_symlink = (self.editor.inventory[base_file_id].kind == 'symlink')
        record = self.editor.texts.get_record_stream([(base_file_id, base_revid)], 'unordered', True).next()
        file_data = record.get_bytes_as('fulltext')
        if file_id == base_file_id:
            file_parents = [base_revid]
        else:
            # Replace with historical version
            del self.editor.inventory[base_file_id]
            file_parents = []
        return FileRevisionBuildEditor(self.editor, path, file_id, 
                               file_parents, file_data, is_symlink=is_symlink)


class FileRevisionBuildEditor(FileBuildEditor):
    def __init__(self, editor, path, file_id, file_parents=[], data="", 
                 is_symlink=False):
        super(FileRevisionBuildEditor, self).__init__(editor, path)
        self.file_id = file_id
        self.file_data = data
        self.is_symlink = is_symlink
        self.file_parents = file_parents
        self.file_stream = None

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

        assert self.is_symlink in (True, False)

        if self.file_id in self.editor.inventory:
            if self.is_executable is None:
                self.is_executable = self.editor.inventory[self.file_id].executable
            del self.editor.inventory[self.file_id]

        if self.is_symlink:
            ie = self.editor.inventory.add_path(self.path, 'symlink', self.file_id)
            ie.symlink_target = "".join(lines)[len("link "):]
            ie.text_sha1 = None
            ie.text_size = None
            ie.executable = False
        else:
            ie = self.editor.inventory.add_path(self.path, 'file', self.file_id)
            ie.kind = 'file'
            ie.symlink_target = None
            ie.text_sha1 = osutils.sha_strings(lines)
            ie.text_size = sum(map(len, lines))
            assert ie.text_size is not None
            ie.executable = self.is_executable
        ie.revision = self.editor._get_text_revid(self.path) or self.editor.revid

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
        self._premature_deletes = set()
        self.old_inventory = prev_inventory
        self.inventory = prev_inventory.copy()
        assert prev_inventory.root is None or self.inventory.root.revision == prev_inventory.root.revision
        super(RevisionBuildEditor, self).__init__(revmeta, mapping)

    def _finish_commit(self):
        if len(self._premature_deletes) > 0:
            raise AssertionError("Remaining deletes in %s: %r" % (self.revid, self._premature_deletes))
        rev = self.revmeta.get_revision(self.mapping)
        self.inventory.revision_id = self.revid
        # Escaping the commit message is really the task of the serialiser
        rev.message = escape_commit_message(rev.message)
        rev.inventory_sha1 = None
        assert self.inventory.root.revision is not None
        self.target.add_revision(self.revid, rev, self.inventory)

        # Only fetch signature if it's cheap
        if self.source.transport.has_capability("log-revprops"):
            signature = self.revmeta.get_signature()
            if signature is not None:
                self.target.add_signature_text(self.revid, signature)

    def _rename(self, file_id, parent_id, old_path, new_path, kind):
        assert isinstance(new_path, unicode)
        assert isinstance(parent_id, str)
        # Only rename if not right yet
        if (self.inventory[file_id].parent_id == parent_id and 
            self.inventory[file_id].name == urlutils.basename(new_path)):
            return
        self.inventory.rename(file_id, parent_id, urlutils.basename(new_path))

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

        if self.inventory.root is not None and \
                file_id == self.inventory.root.file_id:
            ie = self.inventory.root
        else:
            ie = self.inventory.add_path("", 'directory', file_id)
            ie.revision = self.revid
        assert ie.revision is not None
        return DirectoryRevisionBuildEditor(self, "", old_file_id, file_id, file_parents)

    def _get_id_map(self):
        if self._id_map is not None:
            return self._id_map

        self._id_map = self.source.transform_fileid_map(self.revmeta, self.mapping)

        return self._id_map

    def _get_map_id(self, new_path):
        return self._get_id_map().get(new_path)

    def _get_old_id(self, parent_id, old_path):
        assert isinstance(old_path, unicode)
        assert isinstance(parent_id, str)
        return self.old_inventory[parent_id].children[urlutils.basename(old_path)].file_id

    def _get_existing_id(self, old_parent_id, new_parent_id, path):
        assert isinstance(path, unicode)
        assert isinstance(old_parent_id, str)
        assert isinstance(new_parent_id, str)
        ret = self._get_id_map().get(path)
        if ret is not None:
            return ret
        return self.old_inventory[old_parent_id].children[urlutils.basename(path)].file_id

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


class InterFromSvnRepository(InterRepository):
    """Svn to any repository actions."""

    _matching_repo_format = SvnRepositoryFormat()

    _supports_branches = True

    @staticmethod
    def _get_repo_format_to_test():
        return None

    def _find_all(self, mapping, pb=None):
        """Find all revisions from the source repository that are not 
        yet in the target repository.
        """
        meta_map = {}
        graph = self.source.get_graph()
        available_revs = set()
        for revmeta in self.source._revmeta_provider.iter_all_changes(self.source.get_layout(), mapping=mapping, from_revnum=self.source.get_latest_revnum(), pb=pb):
            if revmeta.is_hidden(mapping):
                continue
            revid = revmeta.get_revision_id(mapping)
            available_revs.add(revid)
            meta_map[revid] = revmeta
        missing = available_revs.difference(self.target.has_revisions(available_revs))
        needed = list(graph.iter_topo_order(missing))
        return [(meta_map[revid], mapping) for revid in needed]

    def _find_until(self, foreign_revid, mapping, find_ghosts=False, pb=None,
                    checked=None, project=None):
        """Find all missing revisions until revision_id

        :param revision_id: Stop revision
        :param find_ghosts: Find ghosts
        :return: Tuple with revisions missing and a dictionary with 
            parents for those revision.
        """
        if checked is None:
            checked = set()
        if (foreign_revid, mapping) in checked:
            return []
        extra = list()
        def check_revid((uuid, branch_path, revnum), mapping, project=None):
            revmetas = []
            for revmeta in self.source._revmeta_provider.iter_reverse_branch_changes(
                branch_path, revnum, to_revnum=0, mapping=mapping):
                if pb:
                    pb.update("determining revisions to fetch", 
                              revnum-revmeta.revnum, revnum)
                if (revmeta.get_foreign_revid(), mapping) in checked:
                    # This revision (and its ancestry) has already been checked
                    break
                if revmeta.is_hidden(mapping):
                    continue
                if not self.target.has_revision(revmeta.get_revision_id(mapping)):
                    revmetas.append(revmeta)
                    for p in revmeta.get_rhs_parents(mapping):
                        try:
                            foreign_revid, mapping = self.source.lookup_revision_id(p, project=project)
                        except NoSuchRevision:
                            pass # Ghost
                        else:
                            extra.append((foreign_revid, project, mapping))
                elif not find_ghosts:
                    break
                checked.add((revmeta.get_foreign_revid(), mapping))
            return [(revmeta, mapping) for revmeta in reversed(revmetas)]

        needed = check_revid(foreign_revid, mapping, project)

        while len(extra) > 0:
            foreign_revid, project, mapping = extra.pop()
            if (foreign_revid, mapping) not in checked:
                needed += check_revid(foreign_revid, mapping, project)

        return needed

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
            self._get_inventory(revmeta.get_lhs_parent(mapping)), 
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
        if parent_revmeta is None:
            low_water_mark = 0
        else:
            low_water_mark = parent_revmeta.revnum
        assert revmeta.revnum >= 0
        assert low_water_mark >= 0
        conn = self.source.transport.get_connection(revmeta.branch_path)
        try:
            conn.replay(revmeta.revnum, low_water_mark, editor, True)
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
            revid = revmeta.get_revision_id(mapping)
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
                        self._fetch_revision_replay(editor, revmeta, parent_revmeta)
                    else:
                        self._fetch_revision_switch(editor, revmeta, parent_revmeta)
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
              revmetas=None, mapping=None):
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
                if revmetas is not None:
                    needed = [(revmeta, mapping) for revmeta in revmetas]
                elif revision_id is None:
                    needed = self._find_all(self.source.get_mapping(), pb=nested_pb)
                else:
                    foreign_revid, mapping = self.source.lookup_revision_id(revision_id)
                    needed = self._find_until(foreign_revid, mapping, find_ghosts, pb=nested_pb)
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
            self.target.unlock()

    def _fetch_revision_chunks(self, revs, pb=None):
        """Copy a set of related revisions using svn.ra.replay.

        :param revids: Revision ids to copy.
        :param pb: Optional progress bar
        """
        self._prev_inv = None
        ranges = []
        curmetabranch = None
        currange = None
        revmetas = {}
        pb = ui.ui_factory.nested_progress_bar()
        try:
            for i, (revmeta, mapping) in enumerate(revs):
                pb.update("determining revision ranges", i, len(revs))
                if revmeta.metabranch is not None and curmetabranch == revmeta.metabranch:
                    (branch_path, low_water_mark, from_revnum, to_revum, revmetas) = currange
                    revmetas[revmeta.revnum] = (revmeta, mapping)
                    currange = (revmeta.branch_path, low_water_mark, from_revnum, revmeta.revnum, revmetas)
                else:
                    if currange is not None:
                        ranges.append(currange)
                    parentrevmeta = revmeta.get_lhs_parent_revmeta(mapping)
                    if parentrevmeta is None:
                        low_water_mark = 0
                    else:
                        low_water_mark = parentrevmeta.revnum
                    currange = (revmeta.branch_path, low_water_mark, revmeta.revnum, revmeta.revnum,
                                {revmeta.revnum: (revmeta, mapping)})
        finally:
            pb.finished()
        if currange is not None:
            ranges.append(currange)

        mutter("fetching ranges: %r" % ranges)
        if not self.target.is_in_write_group():
            self.target.start_write_group()

        try:
            for (branch_path, low_water_mark, start_revision, end_revision, revmetas) in ranges:
                def revstart(revnum, revprops):
                    if pb is not None:
                        pb.update("fetching revisions", revnum, len(revs))
                    revmeta, mapping = revmetas[revnum]
                    revmeta._revprops = revprops
                    return self._get_editor(revmeta, mapping)

                def revfinish(revision, revprops, editor):
                    self._prev_inv = editor.inventory

                conn = self.source.transport.get_connection(revmeta.branch_path)
                try:
                    conn.replay_range(start_revision, end_revision, low_water_mark, (revstart, revfinish), True)
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

