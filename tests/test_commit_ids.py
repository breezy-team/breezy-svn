# -*- coding: utf-8 -*-

# Copyright (C) 2011 Jelmer Vernooij <jelmer@samba.org>

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

"""Tests for the file id/revision generation in commit."""

import os

from bzrlib.inventory import (
    InventoryFile,
    )
from bzrlib.tests import (
    TestCaseWithTransport,
    TestCase,
    treeshape,
    )
from bzrlib.workingtree import WorkingTree

from bzrlib.plugins.svn.commit import SvnCommitBuilder
from bzrlib.plugins.svn.tests import SubversionTestCase

class CommitIdTesting:
    """Test that file ids and file revisions are appropriately recorded."""

    def commit_tree(self, tree, revision_id=None):
        revid = tree.commit("This is a message", rev_id=revision_id)
        return tree.branch.repository.revision_tree(revid)

    def tree_items(self, tree):
        ret = {}
        for (path, versioned, kind, file_id, ie) in tree.list_files(
                include_root=True, recursive=True):
            ret[path] = (ie.file_id, ie.revision)
        return ret

    def commit_tree_items(self, tree, revision_id=None):
        return self.tree_items(self.commit_tree(tree, revision_id))

    def prepare_wt(self, path):
        raise NotImplementedError(self.prepare_wt)

    def test_set_root(self):
        tree = self.prepare_wt('.')
        tree.lock_write()
        self.addCleanup(tree.unlock)
        tree.set_root_id("THEROOTID")
        items = self.commit_tree_items(tree, "reva")
        self.assertEquals({
            "": ("THEROOTID", "reva")}, items)

    def test_add_file(self):
        tree = self.prepare_wt('.')
        tree.lock_write()
        self.addCleanup(tree.unlock)
        tree.set_root_id("THEROOTID")
        self.build_tree_contents([('afile', 'contents')])
        tree.add(["afile"], ['THEFILEID'])
        items = self.commit_tree_items(tree, "reva")
        self.assertEquals({
            "": ("THEROOTID", "reva"),
            "afile": ("THEFILEID", "reva"),
            }, items)

    def test_modify_file(self):
        tree = self.prepare_wt('.')
        tree.lock_write()
        self.addCleanup(tree.unlock)
        tree.set_root_id("therootid")
        self.build_tree_contents([
            ('afile', 'contents'),
            ("unchanged", "content")])
        tree.add(["afile", "unchanged"], ['thefileid', "unchangedid"])
        self.commit_tree(tree, "reva")
        self.build_tree_contents([('afile', 'contents2')])
        items = self.commit_tree_items(tree, "revb")
        self.assertEquals({
            "": ("therootid", "reva"),
            "afile": ("thefileid", "revb"),
            "unchanged": ("unchangedid", "reva"),
            }, items)

    def test_rename_unmodified(self):
        tree = self.prepare_wt('.')
        tree.lock_write()
        self.addCleanup(tree.unlock)
        tree.set_root_id("therootid")
        self.build_tree_contents([
            ('afile', 'contents'),
            ('cdir/', ),
            ('adir/', )])
        tree.add(["afile", "adir", "cdir"],
                 ['thefileid', "thedirid", 'cdirid'])
        self.commit_tree(tree, "reva")
        tree.rename_one("afile", "adir/afile")
        tree.rename_one("cdir", "ddir")
        items = self.commit_tree_items(tree, "revb")
        self.assertEquals({
            "": ("therootid", "reva"),
            "adir": ("thedirid", "reva"),
            "adir/afile": ("thefileid", "revb"),
            "ddir": ("cdirid", "revb"),
            }, items)

    def test_change_mode(self):
        tree = self.prepare_wt('.')
        tree.lock_write()
        self.addCleanup(tree.unlock)
        tree.set_root_id("therootid")
        self.build_tree_contents([
            ('afile', 'contents')])
        tree.add(["afile"], ['thefileid'])
        self.commit_tree(tree, "reva")
        os.chmod("afile", 0755)
        items = self.commit_tree_items(tree, "revb")
        self.assertEquals({
            "": ("therootid", "reva"),
            "afile": ("thefileid", "revb"),
            }, items)

    def test_rename_parent(self):
        tree = self.prepare_wt('.')
        tree.lock_write()
        self.addCleanup(tree.unlock)
        tree.set_root_id("therootid")
        self.build_tree_contents([
            ('adir/', ),
            ('adir/afile', 'contents')])
        tree.add(["adir", "adir/afile"], ['thedirid', 'thefileid'])
        self.commit_tree(tree, "reva")
        tree.rename_one('adir', 'bdir')
        items = self.commit_tree_items(tree, "revb")
        self.assertEquals({
            "": ("therootid", "reva"),
            "bdir": ("thedirid", "revb"),
            "bdir/afile": ("thefileid", "reva"),
            }, items)

    def test_new_child(self):
        tree = self.prepare_wt('.')
        tree.lock_write()
        self.addCleanup(tree.unlock)
        tree.set_root_id("therootid")
        self.build_tree_contents([
            ('adir/', )])
        tree.add(["adir"], ['thedirid'])
        self.commit_tree(tree, "reva")
        self.build_tree_contents([
            ('adir/afile', 'contents')])
        tree.add(['adir/afile'], ['thefileid'])
        items = self.commit_tree_items(tree, "revb")
        self.assertEquals({
            "": ("therootid", "reva"),
            "adir": ("thedirid", "reva"),
            "adir/afile": ("thefileid", "revb"),
            }, items)


class BzrCommitIdTesting(TestCaseWithTransport,CommitIdTesting):

    def prepare_wt(self, path):
        tree = self.make_branch_and_tree(path)
        tree.commit("Add root")
        return tree


class SvnCommitIdTesting(SubversionTestCase,CommitIdTesting):

    def setUp(self):
        SubversionTestCase.setUp(self)
        os.mkdir("repo")

    build_tree_contents = staticmethod(treeshape.build_tree_contents)

    def prepare_wt(self, path):
        repo_url = self.make_repository(os.path.join("repo", path))
        dc = self.get_commit_editor(repo_url)
        trunk = dc.add_dir("trunk")
        dc.close()

        self.make_checkout(repo_url+"/trunk", path)
        return WorkingTree.open(path)


class GetTextRevisionTests(TestCase):
    """Tests for SvnCommitBuilder._get_text_revision."""

    def test_new_no_parents(self):
        self.assertEquals((None, None),
            SvnCommitBuilder._get_text_revision(
                InventoryFile("fileid", "name", "root"), "dir/name",
                []))
