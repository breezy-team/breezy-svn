# Copyright (C) 2005-2006 Jelmer Vernooij <jelmer@samba.org>

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

from bzrlib.inventory import Inventory
import bzrlib.osutils as osutils
from bzrlib.trace import mutter
from bzrlib.revisiontree import RevisionTree

import os
import md5
from cStringIO import StringIO
import urllib

import svn.core, svn.wc, svn.delta
from svn.core import Pool

from errors import InvalidExternalsDescription

def parse_externals_description(val):
    """Parse an svn:externals property value.

    :returns: dictionary with local names as keys, (revnum, url)
              as value. revnum is the revision number and is 
              set to None if not applicable.
    """
    ret = {}
    for l in val.splitlines():
        if l == "" or l[0] == "#":
            continue
        pts = l.rsplit(None, 2) 
        if len(pts) == 3:
            assert pts[1].startswith("-r")
            ret[pts[0]] = (int(pts[1][2:]), pts[2])
        elif len(pts) == 2:
            ret[pts[0]] = (None, pts[1])
        else:
            raise InvalidExternalsDescription
    return ret


def inventory_add_external(inv, parent_id, name, rev, url):
    """Add an svn:externals entry to an inventory as a tree-reference.
    
    :param inv: Inventory to add to.
    :param parent_id: File id of directory the entry was set on.
    :param name: Name of the entry, relative to entry with parent_id.
    :param rev: Revision of tree that's being referenced, or None if no 
                specific revision is being referenced.
    :param url: URL of referenced tree.
    """
    # FIXME: Find id of parent of name
    (dir, base) = os.path.split(name)
    os.path.join(inv.id2path(dir), dir)
    pass


def apply_txdelta_handler(src_stream, target_stream, pool):
    assert hasattr(src_stream, 'read')
    assert hasattr(target_stream, 'write')
    ret = svn.delta.svn_txdelta_apply(
            src_stream, 
            target_stream,
            None,
            pool)

    def wrapper(window):
        svn.delta.invoke_txdelta_window_handler(
            ret[1], window, ret[2])

    return wrapper

class SvnRevisionTree(RevisionTree):
    def __init__(self, repository, revision_id):
        self._repository = repository
        self._revision_id = revision_id
        pool = Pool()
        (self.branch_path, self.revnum) = repository.parse_revision_id(revision_id)
        self._inventory = Inventory()
        self.id_map = repository.get_fileid_map(self.revnum, self.branch_path)
        self.editor = TreeBuildEditor(self, pool)
        self.file_data = {}
        editor, baton = svn.delta.make_editor(self.editor, pool)
        root_repos = repository.transport.get_repos_root()
        reporter = repository.transport.do_switch(
                self.revnum, "", True, 
                os.path.join(root_repos, self.branch_path), editor, baton, pool)
        reporter.set_path("", 0, True, None, pool)
        reporter.finish_report(pool)
        pool.destroy()

    def get_file_lines(self, file_id):
        return osutils.split_lines(self.file_data[file_id])


class TreeBuildEditor(svn.delta.Editor):
    def __init__(self, tree, pool):
        self.tree = tree
        self.repository = tree._repository
        self.last_revnum = {}
        self.dir_revnum = {}
        self.dir_ignores = {}
        self.externals = {}
        self.pool = pool

    def set_target_revision(self, revnum):
        self.revnum = revnum

    def open_root(self, revnum, baton):
        file_id, revision_id = self.tree.id_map[""]
        ie = self.tree._inventory.add_path("", 'directory', file_id)
        ie.revision = revision_id
        self.tree._inventory.revision_id = revision_id
        return file_id

    def add_directory(self, path, parent_baton, copyfrom_path, copyfrom_revnum, pool):
        path = path.decode("utf-8")
        file_id, revision_id = self.tree.id_map[path]
        ie = self.tree._inventory.add_path(path, 'directory', file_id)
        ie.revision = revision_id
        return file_id

    def change_dir_prop(self, id, name, value, pool):
        from repository import (SVN_PROP_BZR_MERGE, SVN_PROP_SVK_MERGE, 
                        SVN_PROP_BZR_PREFIX, SVN_PROP_BZR_REVPROP_PREFIX, 
                        SVN_PROP_BZR_FILEIDS)

        if name == svn.core.SVN_PROP_ENTRY_COMMITTED_REV:
            self.dir_revnum[id] = int(value)
        elif name == svn.core.SVN_PROP_IGNORE:
            self.dir_ignores[id] = value
        elif name == svn.core.SVN_PROP_EXTERNALS:
            self.externals[id] = parse_externals_description(value)
        elif name == SVN_PROP_BZR_MERGE or name == SVN_PROP_SVK_MERGE:
            if id != self.tree._inventory.root.file_id:
                mutter('%r set on non-root dir!' % SVN_PROP_BZR_MERGE)
                return
        elif name == SVN_PROP_BZR_FILEIDS:
            if id != self.tree._inventory.root.file_id:
                mutter('%r set on non-root dir!' % SVN_PROP_BZR_FILEIDS)
                return
        elif name in (svn.core.SVN_PROP_ENTRY_COMMITTED_DATE,
                      svn.core.SVN_PROP_ENTRY_LAST_AUTHOR,
                      svn.core.SVN_PROP_ENTRY_LOCK_TOKEN,
                      svn.core.SVN_PROP_ENTRY_UUID,
                      svn.core.SVN_PROP_EXECUTABLE):
            pass
        elif name.startswith(svn.core.SVN_PROP_WC_PREFIX):
            pass
        elif name.startswith(SVN_PROP_BZR_REVPROP_PREFIX):
            pass
        elif (name.startswith(svn.core.SVN_PROP_PREFIX) or
              name.startswith(SVN_PROP_BZR_PREFIX)):
            mutter('unsupported dir property %r' % name)

    def change_file_prop(self, id, name, value, pool):
        from repository import SVN_PROP_BZR_PREFIX

        if name == svn.core.SVN_PROP_EXECUTABLE:
            self.is_executable = (value != None)
        elif name == svn.core.SVN_PROP_SPECIAL:
            self.is_symlink = (value != None)
        elif name == svn.core.SVN_PROP_EXTERNALS:
            mutter('svn:externals property on file!')
        elif name == svn.core.SVN_PROP_ENTRY_COMMITTED_REV:
            self.last_file_rev = int(value)
        elif name in (svn.core.SVN_PROP_ENTRY_COMMITTED_DATE,
                      svn.core.SVN_PROP_ENTRY_LAST_AUTHOR,
                      svn.core.SVN_PROP_ENTRY_LOCK_TOKEN,
                      svn.core.SVN_PROP_ENTRY_UUID,
                      svn.core.SVN_PROP_MIME_TYPE):
            pass
        elif name.startswith(svn.core.SVN_PROP_WC_PREFIX):
            pass
        elif (name.startswith(svn.core.SVN_PROP_PREFIX) or
              name.startswith(SVN_PROP_BZR_PREFIX)):
            mutter('unsupported file property %r' % name)

    def add_file(self, path, parent_id, copyfrom_path, copyfrom_revnum, baton):
        path = path.decode("utf-8")
        self.is_symlink = False
        self.is_executable = False
        return path

    def close_directory(self, id):
        if id in self.tree._inventory and self.dir_ignores.has_key(id):
            self.tree._inventory[id].ignores = self.dir_ignores[id]

        if self.externals.has_key(id):
            # Add externals. This happens after all children have been added
            # as they can be grandchildren.
            for name in self.externals[id]:
                inventory_add_external(self.inventory, id, name, rev, url)

    def close_file(self, path, checksum):
        file_id, revision_id = self.tree.id_map[path]
        if self.is_symlink:
            ie = self.tree._inventory.add_path(path, 'symlink', file_id)
        else:
            ie = self.tree._inventory.add_path(path, 'file', file_id)
        ie.revision = revision_id

        if self.file_stream:
            self.file_stream.seek(0)
            file_data = self.file_stream.read()
        else:
            file_data = ""

        actual_checksum = md5.new(file_data).hexdigest()
        assert(checksum is None or checksum == actual_checksum,
                "checksum mismatch: %r != %r" % (checksum, actual_checksum))

        if self.is_symlink:
            ie.symlink_target = file_data[len("link "):]
            ie.text_sha1 = None
            ie.text_size = None
            ie.text_id = None
            ie.executable = False
        else:
            ie.text_sha1 = osutils.sha_string(file_data)
            ie.text_size = len(file_data)
            self.tree.file_data[file_id] = file_data
            ie.executable = self.is_executable

        self.file_stream = None

    def close_edit(self):
        pass

    def abort_edit(self):
        pass

    def apply_textdelta(self, file_id, base_checksum):
        self.file_stream = StringIO()
        return apply_txdelta_handler(StringIO(""), self.file_stream, self.pool)


class SvnBasisTree(RevisionTree):
    """Optimized version of SvnRevisionTree."""
    def __init__(self, workingtree):
        self.workingtree = workingtree
        self._revision_id = workingtree.branch.generate_revision_id(workingtree.base_revnum)
        self.id_map = workingtree.branch.repository.get_fileid_map(
                workingtree.base_revnum, workingtree.branch.branch_path)
        self._inventory = Inventory(root_id=None)
        self._repository = workingtree.branch.repository

        def add_file_to_inv(relpath, id, revid, wc):
            props = svn.wc.get_prop_diffs(self.workingtree.abspath(relpath), wc)
            if props.has_key(svn.core.SVN_PROP_SPECIAL):
                ie = self._inventory.add_path(relpath, 'symlink', id)
                ie.symlink_target = open(self._abspath(relpath)).read()[len("link "):]
                ie.text_sha1 = None
                ie.text_size = None
                ie.text_id = None
                ie.executable = False
            else:
                ie = self._inventory.add_path(relpath, 'file', id)
                data = osutils.fingerprint_file(open(self._abspath(relpath)))
                ie.text_sha1 = data['sha1']
                ie.text_size = data['size']
                ie.executable = props.has_key(svn.core.SVN_PROP_EXECUTABLE)
            ie.revision = revid
            return ie

        def find_ids(entry):
            relpath = urllib.unquote(entry.url[len(entry.repos):].strip("/"))
            if entry.schedule in (svn.wc.schedule_normal, 
                                  svn.wc.schedule_delete, 
                                  svn.wc.schedule_replace):
                return self.id_map[workingtree.branch.repository.scheme.unprefix(relpath)[1]]
            return (None, None)

        def add_dir_to_inv(relpath, wc, parent_id):
            entries = svn.wc.entries_read(wc, False)
            entry = entries[""]
            (id, revid) = find_ids(entry)
            if id == None:
                return

            # First handle directory itself
            ie = self._inventory.add_path(relpath, 'directory', id)
            ie.revision = revid
            if relpath == "":
                self._inventory.revision_id = revid

            for name in entries:
                if name == "":
                    continue

                subrelpath = os.path.join(relpath, name)

                entry = entries[name]
                assert entry
                
                if entry.kind == svn.core.svn_node_dir:
                    subwc = svn.wc.adm_open3(wc, 
                            self.workingtree.abspath(subrelpath), 
                                             False, 0, None)
                    try:
                        add_dir_to_inv(subrelpath, subwc, id)
                    finally:
                        svn.wc.adm_close(subwc)
                else:
                    (subid, subrevid) = find_ids(entry)
                    if subid is not None:
                        add_file_to_inv(subrelpath, subid, subrevid, wc)

        wc = workingtree._get_wc() 
        try:
            add_dir_to_inv("", wc, None)
        finally:
            svn.wc.adm_close(wc)

    def _abspath(self, relpath):
        return svn.wc.get_pristine_copy_path(self.workingtree.abspath(relpath))

    def get_file_lines(self, file_id):
        base_copy = self._abspath(self.id2path(file_id))
        return osutils.split_lines(open(base_copy).read())

