# Copyright (C) 2007-2009 Jelmer Vernooij <jelmer@samba.org>

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
"""Basis and revision tree tests."""

import os
import subvertpy

from bzrlib.inventory import (
    Inventory,
    TreeReference,
    )
from bzrlib.repository import Repository
from bzrlib.revision import (
    NULL_REVISION,
    CURRENT_REVISION,
    )
from bzrlib.tests import (
    TestSkipped,
    )
try:
    from bzrlib.tests.features import (
        SymlinkFeature,
        )
except ImportError: # bzr < 2.5
    from bzrlib.tests import (
        SymlinkFeature,
        )
from bzrlib.workingtree import WorkingTree

from bzrlib.plugins.svn.tests import SubversionTestCase
from bzrlib.plugins.svn.tree import (
    SvnBasisTree,
    inventory_add_external,
    )


class TestBasisTree(SubversionTestCase):

    def test_executable(self):
        tree = self.make_svn_branch_and_tree("d", "dc")

        dc = self.get_commit_editor(tree.branch.base)
        f = dc.add_file("file")
        f.modify("x")
        f.change_prop("svn:executable", "*")
        dc.close()

        self.client_update("dc")

        tree = SvnBasisTree(tree)
        self.assertTrue(tree.is_executable(tree.path2id("file")))

    def test_executable_changed(self):
        tree = self.make_svn_branch_and_tree("d", "dc")

        dc = self.get_commit_editor(tree.branch.base)
        dc.add_file("file").modify("x")
        dc.close()

        self.client_update("dc")
        self.client_set_prop("dc/file", "svn:executable", "*")
        tree = SvnBasisTree(tree)
        self.assertFalse(tree.is_executable(tree.path2id("file")))

    def test_symlink(self):
        tree = self.make_svn_branch_and_tree("d", "dc")

        dc = self.get_commit_editor(tree.branch.base)
        file = dc.add_file("file")
        file.modify("link target")
        file.change_prop("svn:special", "*")
        dc.close()

        self.client_update("dc")
        tree = SvnBasisTree(tree)
        self.assertEqual('symlink',
                         tree.kind(tree.path2id("file")))
        self.assertEqual("target",
                         tree.get_symlink_target(tree.path2id("file")))

    def test_symlink_with_newlines_in_target(self):
        repos_url = self.make_client("d", "dc")

        dc = self.get_commit_editor(repos_url)
        file = dc.add_file("file")
        file.modify("link target\nbar\nbla")
        file.change_prop("svn:special", "*")
        dc.close()

        self.client_update("dc")
        tree = SvnBasisTree(WorkingTree.open("dc"))
        self.assertEqual('symlink',
                         tree.kind(tree.path2id("file")))
        self.assertEqual("target\nbar\nbla",
                         tree.get_symlink_target(tree.path2id("file")))

    def test_symlink_not_special(self):
        tree = self.make_svn_branch_and_tree("d", "dc")

        dc = self.get_commit_editor(tree.branch.base)
        file1 = dc.add_file("file")
        file1.modify("fsdfdslhfdsk h")
        file1.change_prop("svn:special", "*")
        file2 = dc.add_file("file2")
        file2.modify("a")
        file2.change_prop("svn:special", "*")
        dc.close()

        try:
            self.client_update("dc")
        except subvertpy.SubversionException, (msg, num):
            if num == subvertpy.ERR_WC_BAD_ADM_LOG:
                raise TestSkipped("Unable to run test with svn 1.4")
            raise
        tree = SvnBasisTree(tree)
        self.assertEqual('file', tree.kind(tree.path2id("file")))

    def test_symlink_next(self):
        tree = self.make_svn_branch_and_tree("d", "dc")

        dc = self.get_commit_editor(tree.branch.base)
        dc.add_file("bla").modify("p")
        file = dc.add_file("file")
        file.modify("link target")
        file.change_prop("svn:special", "*")
        dc.close()

        dc = self.get_commit_editor(tree.branch.base)
        dc.open_file("bla").modify("pa")
        dc.close()

        self.client_update("dc")

        tree = SvnBasisTree(tree)
        self.assertEqual('symlink', tree.kind(tree.path2id("file")))
        self.assertEqual("target",
                         tree.get_symlink_target(tree.path2id("file")))

    def test_annotate_iter(self):
        repos_url = self.make_client("d", "dc")

        dc = self.get_commit_editor(repos_url)
        dc.add_file("file").modify("x\n")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.open_file("file").modify("x\ny\n")
        dc.close()

        self.client_update('dc')
        wt = WorkingTree.open("dc")
        tree = SvnBasisTree(wt)
        repo = wt.branch.repository
        self.assertEquals([
            (repo.generate_revision_id(1, "", repo.get_mapping()), "x\n"),
            (repo.generate_revision_id(2, "", repo.get_mapping()), "y\n")],
            tree.annotate_iter(tree.path2id("file")))

    def test_get_dir_properties(self):
        repos_url = self.make_client('a', 'dc')
        self.build_tree({"dc/bla": None})
        self.client_add("dc/bla")
        self.client_set_prop("dc/bla", "bzrbla", "bloe")
        self.client_commit('dc', 'msg')
        self.client_set_prop("dc/bla", "bzrbla", "bloe2")
        t = WorkingTree.open('dc').basis_tree()
        props = t.get_file_properties(t.path2id('bla'), 'bla')
        self.assertEquals("bloe", props["bzrbla"])

    def test_get_file_properties(self):
        repos_url = self.make_client('a', 'dc')
        self.build_tree({"dc/bla": "data"})
        self.client_add("dc/bla")
        self.client_set_prop("dc/bla", "bzrbla", "bloe")
        self.client_commit('dc', 'msg')
        self.client_set_prop("dc/bla", "bzrbla", "bloe2")
        t = WorkingTree.open('dc').basis_tree()
        props = t.get_file_properties(t.path2id('bla'), 'bla')
        self.assertEquals("bloe", props["bzrbla"])

    def test_executable_link(self):
        self.requireFeature(SymlinkFeature)
        repos_url = self.make_client("d", "dc")

        dc = self.get_commit_editor(repos_url)
        file = dc.add_file("file")
        file.modify("link target")
        file.change_prop("svn:special", "*")
        file.change_prop("svn:executable", "*")
        dc.close()

        try:
            self.client_update("dc")
        except subvertpy.SubversionException, (msg, num):
            if num == subvertpy.ERR_WC_BAD_ADM_LOG:
                raise TestSkipped("Unable to run test with svn 1.4")
            raise

        wt = WorkingTree.open("dc")
        tree = SvnBasisTree(wt)
        self.assertFalse(tree.is_executable(tree.path2id("file")))
        self.assertFalse(wt.is_executable(wt.path2id("file")))


class TestInventoryExternals(SubversionTestCase):

    def test_add_nested_norev(self):
        """Add a nested tree with no specific revision referenced."""
        branch = self.make_svn_branch('d')
        repos = branch.repository
        repos_url = repos.base
        mapping = repos.get_mapping()
        inv = Inventory(root_id='blabloe')
        inventory_add_external(inv, 'blabloe', 'blie/bla',
                mapping.revision_id_foreign_to_bzr((repos.uuid, branch.get_branch_path(), 1)),
                None, repos_url)
        self.assertEqual(TreeReference(
            mapping.generate_file_id((repos.uuid, branch.get_branch_path(), 1), u""),
             'bla', inv.path2id('blie'),
             reference_revision=CURRENT_REVISION,
             revision=mapping.revision_id_foreign_to_bzr((repos.uuid, branch.get_branch_path(), 1))),
             inv[inv.path2id('blie/bla')])

    def test_add_simple_norev(self):
        branch = self.make_svn_branch('d')
        repos = branch.repository
        mapping = repos.get_mapping()
        inv = Inventory(root_id='blabloe')
        inventory_add_external(inv, 'blabloe', 'bla',
            mapping.revision_id_foreign_to_bzr((repos.uuid, branch.get_branch_path(), 1)), None,
            branch.repository.base)

        self.assertEqual(TreeReference(
            mapping.generate_file_id((repos.uuid, branch.get_branch_path(), 1), u""),
             'bla', 'blabloe',
             reference_revision=CURRENT_REVISION,
             revision=mapping.revision_id_foreign_to_bzr((repos.uuid, branch.get_branch_path(), 1))),
             inv[inv.path2id('bla')])

    def test_add_simple_rev(self):
        branch = self.make_svn_branch('d')
        repos = branch.repository
        inv = Inventory(root_id='blabloe')
        mapping = repos.get_mapping()
        inventory_add_external(inv, 'blabloe', 'bla',
            mapping.revision_id_foreign_to_bzr((repos.uuid, branch.get_branch_path(), 1)), 1, branch.repository.base)
        expected_ie = TreeReference(mapping.generate_file_id((repos.uuid, branch.get_branch_path(), 1), u""),
            'bla', 'blabloe',
            revision=mapping.revision_id_foreign_to_bzr((repos.uuid, branch.get_branch_path(), 1)),
            reference_revision=NULL_REVISION)
        ie = inv[inv.path2id('bla')]
        self.assertEqual(NULL_REVISION, ie.reference_revision)
        self.assertEqual(mapping.revision_id_foreign_to_bzr((repos.uuid, branch.get_branch_path(), 1)),
                         ie.revision)
        self.assertEqual(expected_ie, inv[inv.path2id('bla')])


class TestSvnRevisionTree(SubversionTestCase):

    def setUp(self):
        super(TestSvnRevisionTree, self).setUp()
        tree = self.make_svn_branch_and_tree('d', 'dc')
        self.build_tree({'dc/foo/bla': "data"})
        self.client_add("dc/foo")
        self.client_commit("dc", "My Message")
        self.branch = tree.branch
        self.repos = tree.branch.repository
        mapping = self.repos.get_mapping()
        self.tree = self.repos.revision_tree(
                self.repos.generate_revision_id(2, "trunk", mapping))

    def test_get_parent_ids(self):
        mapping = self.repos.get_mapping()
        self.assertEqual(
            (self.repos.generate_revision_id(1, self.branch.get_branch_path(), mapping),),
            self.tree.get_parent_ids())

    def test_get_parent_ids_zero(self):
        mapping = self.repos.get_mapping()
        tree = self.repos.revision_tree(
                self.repos.generate_revision_id(1, self.branch.get_branch_path(), mapping))
        self.assertEqual((), tree.get_parent_ids())

    def test_get_revision_id(self):
        mapping = self.repos.get_mapping()
        self.assertEqual(self.repos.generate_revision_id(2, self.branch.get_branch_path(), mapping),
                         self.tree.get_revision_id())

    def test_get_file_lines(self):
        self.assertEqual(["data"],
                self.tree.get_file_lines(self.tree.path2id("foo/bla")))

    def test_executable(self):
        self.client_set_prop("dc/foo/bla", "svn:executable", "*")
        self.client_commit("dc", "My Message")

        mapping = self.repos.get_mapping()

        tree = self.repos.revision_tree(
                self.repos.generate_revision_id(3, self.branch.get_branch_path(), mapping))

        self.assertTrue(tree.is_executable(tree.path2id("foo/bla")))

    def test_symlink(self):
        self.requireFeature(SymlinkFeature)
        os.symlink('foo/bla', 'dc/bar')
        self.client_add('dc/bar')
        self.client_commit("dc", "My Message")

        mapping = self.repos.get_mapping()

        tree = self.repos.revision_tree(
                self.repos.generate_revision_id(2, self.branch.get_branch_path(), mapping))

        self.assertEqual('symlink', tree.kind(tree.path2id("bar")))
        self.assertEqual('foo/bla',
                tree.get_symlink_target(tree.path2id("bar")))

    def test_not_executable(self):
        self.assertFalse(self.tree.is_executable(self.tree.path2id("foo/bla")))
