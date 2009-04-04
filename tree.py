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
"""Access to stored Subversion basis trees."""

from cStringIO import StringIO
import os
from subvertpy import (
    delta,
    properties,
    wc,
    NODE_DIR,
    )
import urllib

from bzrlib import (
    rules,
    osutils,
    urlutils,
    )
from bzrlib.branch import Branch
from bzrlib.inventory import (
    Inventory,
    InventoryDirectory,
    TreeReference,
    )
from bzrlib.osutils import md5
from bzrlib.revision import CURRENT_REVISION
from bzrlib.revisiontree import (
    RevisionTree,
    )
from bzrlib.trace import mutter

from bzrlib.plugins.svn.fileids import (
    idmap_lookup,
    )

class SubversionTree(object):

    def get_file_properties(self, file_id, path=None):
        raise NotImplementedError(self.get_file_properties)

    def supports_content_filtering(self):
        return True

    def _get_rules_searcher(self, default_searcher):
        """Get the RulesSearcher for this tree given the default one."""
        return rules._StackedRulesSearcher(
            [SvnRulesSearcher(self), default_searcher])


# This maps SVN names for eol-styles to bzr names:
eol_style = {
    "native": "native",
    "CRLF": "crlf",
    "LF": "lf",
    "CR": "cr"
    }

class SvnRulesSearcher(object):

    def __init__(self, tree):
        self.tree = tree

    def _map_property(self, name, value):
        if name == "svn:eol-style":
            if value in eol_style:
                return ("eol", eol_style[value])
            mutter("Unknown svn:eol-style setting '%r'", value)
            return None
        elif name == "svn:keywords":
            return ("svn-keywords", value)
        else:
            # Unknown or boring setting
            return None

    def get_items(self, path):
        file_id = self.tree.path2id(path)
        for k, v in self.tree.get_file_properties(file_id, path).iteritems():
            prop = self._map_property(k, v)
            if prop is not None:
                yield prop

    def get_selected_items(self, path, names):
        return tuple([(k, v) for (k, v) in self.get_items(path) if k in names])


def inventory_add_external(inv, parent_id, path, revid, ref_revnum, url):
    """Add an svn:externals entry to an inventory as a tree-reference.
    
    :param inv: Inventory to add to.
    :param parent_id: File id of directory the entry was set on.
    :param path: Path of the entry, relative to entry with parent_id.
    :param revid: Revision to store in newly created inventory entries.
    :param ref_revnum: Referenced revision of tree that's being referenced, or 
        None if no specific revision is being referenced.
    :param url: URL of referenced tree.
    """
    assert ref_revnum is None or isinstance(ref_revnum, int)
    assert revid is None or isinstance(revid, str)
    (dir, name) = os.path.split(path)
    parent = inv[parent_id]
    if dir != "":
        for part in dir.split("/"):
            if parent.children.has_key(part):
                parent = parent.children[part]
            else:
                # Implicitly add directory if it doesn't exist yet
                # TODO: Generate a file id
                parent = inv.add(InventoryDirectory('someid', part, 
                                 parent_id=parent.file_id))
                parent.revision = revid

    reference_branch = Branch.open(url)
    file_id = reference_branch.get_root_id()
    ie = TreeReference(file_id, name, parent.file_id, revision=revid)
    if ref_revnum is not None:
        ie.reference_revision = reference_branch.get_rev_id(ref_revnum)
    inv.add(ie)


class SvnRevisionTree(SubversionTree,RevisionTree):
    """A tree that existed in a historical Subversion revision."""

    def __init__(self, repository, revision_id):
        self._repository = repository
        self._revision_id = revision_id
        self._revmeta, self.mapping = repository._get_revmeta(revision_id)
        self._inventory = Inventory()
        self._inventory.revision_id = revision_id
        self._rules_searcher = None
        self.id_map = repository.get_fileid_map(self._revmeta, self.mapping)
        editor = TreeBuildEditor(self)
        self.file_data = {}
        self.file_properties = {}
        root_repos = repository.transport.get_svn_repos_root()
        conn = repository.transport.get_connection()
        try:
            reporter = conn.do_switch(
                self._revmeta.revnum, "", True, 
                urlutils.join(root_repos, self._revmeta.branch_path).rstrip("/"), editor)
            try:
                reporter.set_path("", 0, True, None)
                reporter.finish()
            except:
                reporter.abort()
                raise
        finally:
            repository.transport.add_connection(conn)

    def get_file_text(self, file_id, path=None):
        return self.file_data[file_id]

    def get_file_properties(self, file_id, path=None):
        return self.file_properties[file_id]


class TreeBuildEditor(object):
    """Builds a tree given Subversion tree transform calls."""
    def __init__(self, tree):
        self.tree = tree
        self.repository = tree._repository
        self.last_revnum = {}

    def set_target_revision(self, revnum):
        self.revnum = revnum

    def open_root(self, revnum):
        file_id, revision_id, _ = idmap_lookup(self.tree.id_map, 
                                               self.tree.mapping, "")
        ie = self.tree._inventory.add_path("", 'directory', file_id)
        ie.revision = revision_id
        return DirectoryTreeEditor(self.tree, file_id)

    def close(self):
        pass

    def abort(self):
        pass


class DirectoryTreeEditor(object):
    def __init__(self, tree, file_id):
        self.tree = tree
        self.file_id = file_id
        self.tree.file_properties[file_id] = {}

    def add_directory(self, path, copyfrom_path=None, copyfrom_revnum=-1):
        path = path.decode("utf-8")
        file_id, revision_id, _ = idmap_lookup(self.tree.id_map, 
                                               self.tree.mapping, path)
        ie = self.tree._inventory.add_path(path, 'directory', file_id)
        ie.revision = revision_id
        return DirectoryTreeEditor(self.tree, file_id)

    def change_prop(self, name, value):
        self.tree.file_properties[self.file_id][name] = value
        if name in (properties.PROP_ENTRY_COMMITTED_DATE,
                    properties.PROP_ENTRY_LAST_AUTHOR,
                    properties.PROP_ENTRY_LOCK_TOKEN,
                    properties.PROP_ENTRY_COMMITTED_REV,
                    properties.PROP_ENTRY_UUID,
                    properties.PROP_IGNORE,
                    properties.PROP_EXECUTABLE):
            pass
        elif name.startswith(properties.PROP_WC_PREFIX):
            pass
        elif name.startswith(properties.PROP_PREFIX):
            mutter('unsupported dir property %r', name)

    def add_file(self, path, copyfrom_path=None, copyfrom_revnum=-1):
        path = path.decode("utf-8")
        return FileTreeEditor(self.tree, path)

    def close(self):
        pass


class FileTreeEditor(object):

    def __init__(self, tree, path):
        self.tree = tree
        self.path = path
        self.is_executable = False
        self.is_special = None
        self.is_symlink = False
        self.last_file_rev = None
        self.file_id, self.revision_id, _ = idmap_lookup(self.tree.id_map, 
                                               self.tree.mapping, self.path)
        self.tree.file_properties[self.file_id] = {}

    def change_prop(self, name, value):
        self.tree.file_properties[self.file_id][name] = value
        if name == properties.PROP_EXECUTABLE:
            self.is_executable = (value != None)
        elif name == properties.PROP_SPECIAL:
            self.is_special = (value is not None)
        elif name == properties.PROP_EXTERNALS:
            mutter('%r property on file!', name)
        elif name == properties.PROP_ENTRY_COMMITTED_REV:
            self.last_file_rev = int(value)
        elif name in (properties.PROP_ENTRY_COMMITTED_DATE,
                      properties.PROP_ENTRY_LAST_AUTHOR,
                      properties.PROP_ENTRY_LOCK_TOKEN,
                      properties.PROP_ENTRY_UUID,
                      properties.PROP_MIME_TYPE):
            pass
        elif name.startswith(properties.PROP_WC_PREFIX):
            pass
        elif name.startswith(properties.PROP_PREFIX):
            mutter('unsupported file property %r', name)

    def close(self, checksum=None):

        if self.file_stream:
            self.file_stream.seek(0)
            file_data = self.file_stream.read()
        else:
            file_data = ""

        if self.is_special is not None:
            self.is_symlink = (self.is_special and file_data.startswith("link "))

        if self.is_symlink:
            ie = self.tree._inventory.add_path(self.path, 'symlink', self.file_id)
        else:
            ie = self.tree._inventory.add_path(self.path, 'file', self.file_id)
        ie.revision = self.revision_id

        actual_checksum = md5(file_data).hexdigest()
        assert checksum is None or checksum == actual_checksum, \
                "checksum mismatch: %r != %r" % (checksum, actual_checksum)

        if self.is_symlink:
            ie.symlink_target = file_data[len("link "):]
            ie.text_sha1 = None
            ie.text_size = None
            ie.text_id = None
            ie.executable = False
        else:
            ie.text_sha1 = osutils.sha_string(file_data)
            ie.text_size = len(file_data)
            self.tree.file_data[self.file_id] = file_data
            ie.executable = self.is_executable

        self.file_stream = None

    def apply_textdelta(self, base_checksum):
        self.file_stream = StringIO()
        return delta.apply_txdelta_handler("", self.file_stream)


class SvnBasisTree(SubversionTree,RevisionTree):
    """Optimized version of SvnRevisionTree."""

    def __init__(self, workingtree):
        mutter("opening basistree for %r at %d; %s" % (workingtree, workingtree.base_revnum, workingtree.base_revid))
        self.workingtree = workingtree
        self._revision_id = workingtree.base_revid
        self.id_map = workingtree.branch.repository.get_fileid_map(
                workingtree._get_base_revmeta(),
                workingtree.branch.mapping)
        self._inventory = Inventory(root_id=None)
        self._repository = workingtree.branch.repository

        def add_file_to_inv(relpath, id, revid, adm):
            (propchanges, props) = adm.get_prop_diffs(self.workingtree.abspath(relpath).encode("utf-8"))
            if props.has_key(properties.PROP_SPECIAL):
                is_symlink = (self.get_file_byname(relpath).read(5) == "link ")
            else:
                is_symlink = False

            if is_symlink:
                ie = self._inventory.add_path(relpath, 'symlink', id)
                ie.symlink_target = self.get_file_byname(relpath).read()[len("link "):]
                ie.text_sha1 = None
                ie.text_size = None
                ie.text_id = None
                ie.executable = False
            else:
                ie = self._inventory.add_path(relpath, 'file', id)
                data = osutils.fingerprint_file(self.get_file_byname(relpath))
                ie.text_sha1 = data['sha1']
                ie.text_size = data['size']
                ie.executable = props.has_key(properties.PROP_EXECUTABLE)
            ie.revision = revid
            return ie

        def find_ids(entry):
            relpath = urllib.unquote(entry.url[len(entry.repos):].strip("/"))
            if entry.schedule in (wc.SCHEDULE_NORMAL, 
                                  wc.SCHEDULE_DELETE, 
                                  wc.SCHEDULE_REPLACE):
                return idmap_lookup(self.id_map, workingtree.branch.mapping, workingtree.branch.unprefix(relpath.decode("utf-8")))[:2]
            return (None, None)

        def add_dir_to_inv(relpath, adm, parent_id):
            entries = adm.entries_read(False)
            entry = entries[""]
            (id, revid) = find_ids(entry)
            if id == None:
                return

            # First handle directory itself
            ie = self._inventory.add_path(relpath, 'directory', id)
            ie.revision = revid
            if relpath == u"":
                self._inventory.revision_id = revid

            for name, entry in entries.iteritems():
                name = name.decode("utf-8")
                if name == u"":
                    continue

                assert isinstance(relpath, unicode)
                assert isinstance(name, unicode)

                subrelpath = os.path.join(relpath, name)

                assert entry
                
                if entry.kind == NODE_DIR:
                    subwc = self.workingtree._get_wc(subrelpath)
                    try:
                        add_dir_to_inv(subrelpath, subwc, id)
                    finally:
                        subwc.close()
                else:
                    (subid, subrevid) = find_ids(entry)
                    if subid is not None:
                        add_file_to_inv(subrelpath, subid, subrevid, adm)

        adm = workingtree._get_wc() 
        try:
            add_dir_to_inv(u"", adm, None)
        finally:
            adm.close()

    def abspath(self, relpath):
        return wc.get_pristine_copy_path(self.workingtree.abspath(relpath).encode("utf-8"))

    def get_file_byname(self, name):
        return open(self.abspath(name))

    def get_file_text(self, file_id, path=None):
        if path is None:
            path = self.id2path(file_id)
        return self.get_file_byname(path).read()

    def get_file_properties(self, file_id, path=None):
        if path is None:
            path = self.id2path(file_id)
        wc = self.workingtree._get_wc(path)
        try:
            (_, orig_props) = wc.get_prop_diffs(self.workingtree.abspath(path).encode("utf-8"))
        finally:
            wc.close()
        return orig_props

    def annotate_iter(self, file_id, default_revision=CURRENT_REVISION):
        return self.workingtree.branch.repository._annotate(urlutils.join(self.workingtree.branch.get_branch_path(), self.inventory.id2path(file_id)).strip("/"), self.workingtree.base_revnum,  file_id, self.get_revision_id(), self.workingtree.branch.mapping)

    def iter_files_bytes(self, file_ids):
        for file_id, identifier in file_ids:
            cur_file = (self.get_file_text(file_id),)
            yield identifier, cur_file
