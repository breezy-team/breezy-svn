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

from breezy import osutils
from breezy.bzr.inventory import (
    Inventory,
    TreeReference,
    )
from breezy.revision import (
    CURRENT_REVISION,
    )
from breezy.tests import (
    TestSkipped,
    )
from breezy.tests.features import (
    SymlinkFeature,
    )
from breezy.workingtree import WorkingTree

from breezy.plugins.svn.tests import SubversionTestCase
from breezy.plugins.svn.tree import (
    SvnBasisTree,
    inventory_add_external,
    )


class TestBasisTree(SubversionTestCase):

    def test_file_verifiers(self):
        tree = self.make_svn_branch_and_tree("d", "dc")

        dc = self.get_commit_editor(tree.branch.base)
        f = dc.add_file("file")
        f.modify(b"new contents")
        dc.close()

        self.client_update("dc")

        tree = SvnBasisTree(tree)
        self.assertEquals(tree.get_file_verifier("file"),
            ("MD5", osutils.md5("new contents").hexdigest()))

    def test_root_id(self):
        tree = self.make_svn_branch_and_tree("d", "dc")
        tree = SvnBasisTree(tree)
        self.assertIs(None, tree.path2id(''))

    def test_executable(self):
        tree = self.make_svn_branch_and_tree("d", "dc")

        dc = self.get_commit_editor(tree.branch.base)
        f = dc.add_file("file")
        f.modify(b"x")
        f.change_prop("svn:executable", "*")
        dc.close()

        self.client_update("dc")

        tree = SvnBasisTree(tree)
        self.assertTrue(tree.is_executable("file"))

    def test_executable_changed(self):
        tree = self.make_svn_branch_and_tree("d", "dc")

        dc = self.get_commit_editor(tree.branch.base)
        dc.add_file("file").modify(b"x")
        dc.close()

        self.client_update("dc")
        self.client_set_prop("dc/file", "svn:executable", "*")
        tree = SvnBasisTree(tree)
        self.assertFalse(tree.is_executable("file"))

    def test_symlink(self):
        tree = self.make_svn_branch_and_tree("d", "dc")

        dc = self.get_commit_editor(tree.branch.base)
        file = dc.add_file("file")
        file.modify(b"link target")
        file.change_prop("svn:special", "*")
        dc.close()

        self.client_update("dc")
        tree = SvnBasisTree(tree)
        self.assertEqual('symlink',
                         tree.kind("file"))
        self.assertEqual("target",
                         tree.get_symlink_target("file"))

    def test_symlink_with_newlines_in_target(self):
        repos_url = self.make_client("d", "dc")

        dc = self.get_commit_editor(repos_url)
        file = dc.add_file("file")
        file.modify(b"link target\nbar\nbla")
        file.change_prop("svn:special", "*")
        dc.close()

        self.client_update("dc")
        tree = SvnBasisTree(WorkingTree.open("dc"))
        self.assertEqual('symlink',
                         tree.kind("file"))
        self.assertEqual("target\nbar\nbla",
                         tree.get_symlink_target("file"))

    def test_symlink_not_special(self):
        tree = self.make_svn_branch_and_tree("d", "dc")

        dc = self.get_commit_editor(tree.branch.base)
        file1 = dc.add_file("file")
        file1.modify(b"fsdfdslhfdsk h")
        file1.change_prop("svn:special", "*")
        file2 = dc.add_file("file2")
        file2.modify(b"a")
        file2.change_prop("svn:special", "*")
        dc.close()

        try:
            self.client_update("dc")
        except subvertpy.SubversionException as e:
            if e.args[1] == subvertpy.ERR_WC_BAD_ADM_LOG:
                raise TestSkipped("Unable to run test with svn 1.4")
            raise
        tree = SvnBasisTree(tree)
        self.assertEqual('file', tree.kind("file"))

    def test_symlink_next(self):
        tree = self.make_svn_branch_and_tree("d", "dc")

        dc = self.get_commit_editor(tree.branch.base)
        dc.add_file("bla").modify(b"p")
        file = dc.add_file("file")
        file.modify(b"link target")
        file.change_prop("svn:special", "*")
        dc.close()

        dc = self.get_commit_editor(tree.branch.base)
        dc.open_file("bla").modify(b"pa")
        dc.close()

        self.client_update("dc")

        tree = SvnBasisTree(tree)
        self.assertEqual('symlink', tree.kind("file"))
        self.assertEqual("target",
                         tree.get_symlink_target("file"))

    def test_annotate_iter(self):
        repos_url = self.make_client("d", "dc")

        dc = self.get_commit_editor(repos_url)
        dc.add_file("file").modify(b"x\n")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.open_file("file").modify(b"x\ny\n")
        dc.close()

        self.client_update('dc')
        wt = WorkingTree.open("dc")
        tree = SvnBasisTree(wt)
        repo = wt.branch.repository
        self.assertEquals([
            (repo.generate_revision_id(1, u"", repo.get_mapping()), "x\n"),
            (repo.generate_revision_id(2, u"", repo.get_mapping()), "y\n")],
            tree.annotate_iter("file"))

    def test_get_dir_properties(self):
        repos_url = self.make_client('a', 'dc')
        self.build_tree({"dc/bla": None})
        self.client_add("dc/bla")
        self.client_set_prop("dc/bla", "bzrbla", "bloe")
        self.client_commit('dc', 'msg')
        self.client_set_prop("dc/bla", "bzrbla", "bloe2")
        t = WorkingTree.open('dc').basis_tree()
        props = t.get_file_properties('bla')
        self.assertEquals("bloe", props["bzrbla"])

    def test_get_file_properties(self):
        repos_url = self.make_client('a', 'dc')
        self.build_tree({"dc/bla": b"data"})
        self.client_add("dc/bla")
        self.client_set_prop("dc/bla", "bzrbla", "bloe")
        self.client_commit('dc', 'msg')
        self.client_set_prop("dc/bla", "bzrbla", "bloe2")
        t = WorkingTree.open('dc').basis_tree()
        props = t.get_file_properties('bla')
        self.assertEquals("bloe", props["bzrbla"])

    def test_executable_link(self):
        self.requireFeature(SymlinkFeature)
        repos_url = self.make_client("d", "dc")

        dc = self.get_commit_editor(repos_url)
        file = dc.add_file("file")
        file.modify(b"link target")
        file.change_prop("svn:special", "*")
        file.change_prop("svn:executable", "*")
        dc.close()

        try:
            self.client_update("dc")
        except subvertpy.SubversionException as e:
            if e.args[1] == subvertpy.ERR_WC_BAD_ADM_LOG:
                raise TestSkipped("Unable to run test with svn 1.4")
            raise

        wt = WorkingTree.open("dc")
        tree = SvnBasisTree(wt)
        self.assertFalse(tree.is_executable("file"))
        self.assertFalse(wt.is_executable("file"))


class TestInventoryExternals(SubversionTestCase):

    def test_add_nested_norev(self):
        """Add a nested tree with no specific revision referenced."""
        branch = self.make_svn_branch('d', lossy=True)
        repos = branch.repository
        mapping = repos.get_mapping()
        inv = Inventory(root_id=b'blabloe')
        inventory_add_external(inv, b'blabloe', 'blie/bla',
                mapping.revision_id_foreign_to_bzr((repos.uuid, branch.get_branch_path(), 1)),
                None, branch.base)
        expected_ie = TreeReference(
            mapping.generate_file_id((repos.uuid, branch.get_branch_path(), 1), u""),
             'bla', inv.path2id('blie'),
             reference_revision=CURRENT_REVISION,
             revision=mapping.revision_id_foreign_to_bzr((repos.uuid, branch.get_branch_path(), 1)))
        ie = inv.get_entry(inv.path2id('blie/bla'))
        self.assertEquals(expected_ie.file_id, ie.file_id)
        self.assertEquals(expected_ie.revision, ie.revision)
        self.assertEquals(expected_ie.name, ie.name)
        self.assertEquals(expected_ie.reference_revision, ie.reference_revision)
        self.assertEquals(expected_ie.parent_id, ie.parent_id)
        self.assertEqual(expected_ie, ie)

    def test_add_simple_norev(self):
        branch = self.make_svn_branch('d', lossy=True)
        repos = branch.repository
        mapping = repos.get_mapping()
        inv = Inventory(root_id=b'blabloe')
        inventory_add_external(inv, b'blabloe', 'bla',
            mapping.revision_id_foreign_to_bzr((repos.uuid, branch.get_branch_path(), 1)), None,
            branch.base)

        self.assertEqual(TreeReference(
            mapping.generate_file_id((repos.uuid, branch.get_branch_path(), 1), u""),
             'bla', b'blabloe',
             reference_revision=CURRENT_REVISION,
             revision=mapping.revision_id_foreign_to_bzr((repos.uuid, branch.get_branch_path(), 1))),
             inv.get_entry(inv.path2id('bla')))

    def test_add_simple_rev(self):
        branch = self.make_svn_branch('d', lossy=True) #1
        repos = branch.repository
        inv = Inventory(root_id=b'blabloe')
        mapping = repos.get_mapping()
        inventory_add_external(inv, b'blabloe', 'bla',
            mapping.revision_id_foreign_to_bzr(
                (repos.uuid, branch.get_branch_path(), 1)), 1, branch.base)
        expected_ie = TreeReference(
            mapping.generate_file_id((repos.uuid, branch.get_branch_path(), 1), u""),
            'bla', b'blabloe',
            revision=mapping.revision_id_foreign_to_bzr(
                (repos.uuid, branch.get_branch_path(), 1)),
            reference_revision=branch.last_revision())
        ie = inv.get_entry(inv.path2id('bla'))
        self.assertEqual(branch.last_revision(), ie.reference_revision)
        self.assertEquals(expected_ie.file_id, ie.file_id)
        self.assertEquals(expected_ie.revision, ie.revision)
        self.assertEquals(expected_ie.name, ie.name)
        self.assertEquals(expected_ie.reference_revision, ie.reference_revision)
        self.assertEquals(expected_ie.parent_id, ie.parent_id)
        self.assertEqual(expected_ie, ie)


class TestSvnRevisionTree(SubversionTestCase):

    def setUp(self):
        super(TestSvnRevisionTree, self).setUp()
        tree = self.make_svn_branch_and_tree('d', 'dc') #1
        self.build_tree({'dc/foo/bla': b"data"})
        self.client_add("dc/foo")
        self.client_commit("dc", "My Message") #2
        self.branch = tree.branch
        self.repos = tree.branch.repository
        mapping = self.repos.get_mapping()
        self.tree = self.repos.revision_tree(
                self.repos.generate_revision_id(2, u"trunk", mapping))

    def test_get_parent_ids_first(self):
        mapping = self.repos.get_mapping()
        self.assertEqual((), self.tree.get_parent_ids())

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
        self.assertEqual([b"data"], self.tree.get_file_lines("foo/bla"))

    def test_executable(self):
        self.client_set_prop("dc/foo/bla", "svn:executable", "*")
        self.client_commit("dc", "My Message")

        mapping = self.repos.get_mapping()

        tree = self.repos.revision_tree(
                self.repos.generate_revision_id(3, self.branch.get_branch_path(), mapping))

        self.assertTrue(tree.is_executable("foo/bla"))

    def test_symlink(self):
        self.requireFeature(SymlinkFeature)
        os.symlink('foo/bla', 'dc/bar')
        self.client_add('dc/bar')
        self.client_commit("dc", "My Message") #3

        mapping = self.repos.get_mapping()

        tree = self.repos.revision_tree(
            self.repos.generate_revision_id(3, self.branch.get_branch_path(), mapping))

        self.assertEqual('symlink', tree.kind("bar"))
        self.assertEqual('foo/bla', tree.get_symlink_target("bar"))

    def test_not_executable(self):
        self.assertFalse(self.tree.is_executable("foo/bla"))
