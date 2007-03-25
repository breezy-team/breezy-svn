# Copyright (C) 2007 Jelmer Vernooij <jelmer@samba.org>

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

from bzrlib.inventory import Inventory, TreeReference
from bzrlib.repository import Repository
from bzrlib.tests import TestCase
from bzrlib.workingtree import WorkingTree

from fileids import generate_svn_file_id
from tree import (SvnBasisTree, parse_externals_description, 
                  inventory_add_external)
from tests import TestCaseWithSubversionRepository

class TestBasisTree(TestCaseWithSubversionRepository):
    def test_executable(self):
        self.make_client("d", "dc")
        self.build_tree({"dc/file": "x"})
        self.client_add("dc/file")
        self.client_set_prop("dc/file", "svn:executable", "*")
        self.client_commit("dc", "executable")
        tree = SvnBasisTree(WorkingTree.open("dc"))
        self.assertTrue(tree.inventory[tree.inventory.path2id("file")].executable)

    def test_executable_changed(self):
        self.make_client("d", "dc")
        self.build_tree({"dc/file": "x"})
        self.client_add("dc/file")
        self.client_commit("dc", "executable")
        self.client_update("dc")
        self.client_set_prop("dc/file", "svn:executable", "*")
        tree = SvnBasisTree(WorkingTree.open("dc"))
        self.assertFalse(tree.inventory[tree.inventory.path2id("file")].executable)

    def test_symlink(self):
        self.make_client("d", "dc")
        import os
        os.symlink("target", "dc/file")
        self.build_tree({"dc/file": "x"})
        self.client_add("dc/file")
        self.client_commit("dc", "symlink")
        self.client_update("dc")
        tree = SvnBasisTree(WorkingTree.open("dc"))
        self.assertEqual('symlink', 
                         tree.inventory[tree.inventory.path2id("file")].kind)
        self.assertEqual("target",
                         tree.inventory[tree.inventory.path2id("file")].symlink_target)

    def test_symlink_next(self):
        self.make_client("d", "dc")
        import os
        os.symlink("target", "dc/file")
        self.build_tree({"dc/file": "x", "dc/bla": "p"})
        self.client_add("dc/file")
        self.client_add("dc/bla")
        self.client_commit("dc", "symlink")
        self.build_tree({"dc/bla": "pa"})
        self.client_commit("dc", "change")
        self.client_update("dc")
        tree = SvnBasisTree(WorkingTree.open("dc"))
        self.assertEqual('symlink', 
                         tree.inventory[tree.inventory.path2id("file")].kind)
        self.assertEqual("target",
                         tree.inventory[tree.inventory.path2id("file")].symlink_target)

    def test_executable_link(self):
        self.make_client("d", "dc")
        import os
        os.symlink("target", "dc/file")
        self.build_tree({"dc/file": "x"})
        self.client_add("dc/file")
        self.client_set_prop("dc/file", "svn:executable", "*")
        self.client_commit("dc", "exe1")
        wt = WorkingTree.open("dc")
        tree = SvnBasisTree(wt)
        self.assertFalse(tree.inventory[tree.inventory.path2id("file")].executable)
        self.assertFalse(wt.inventory[wt.inventory.path2id("file")].executable)


class TestExternalsParser(TestCase):
    def test_parse_externals(self):
        self.assertEqual({
                'third-party/sounds': (None, "http://sounds.red-bean.com/repos"),
                'third-party/skins': (None, "http://skins.red-bean.com/repositories/skinproj"),
                'third-party/skins/toolkit': (21, "http://svn.red-bean.com/repos/skin-maker")},
            parse_externals_description(
"""third-party/sounds             http://sounds.red-bean.com/repos
third-party/skins              http://skins.red-bean.com/repositories/skinproj
third-party/skins/toolkit -r21 http://svn.red-bean.com/repos/skin-maker"""))

    def test_parse_comment(self):
        self.assertEqual({
            'third-party/sounds': (None, "http://sounds.red-bean.com/repos")
                },
            parse_externals_description(
"""

third-party/sounds             http://sounds.red-bean.com/repos
#third-party/skins              http://skins.red-bean.com/repositories/skinproj
#third-party/skins/toolkit -r21 http://svn.red-bean.com/repos/skin-maker"""))

class TestInventoryExternals(TestCaseWithSubversionRepository):
    def test_add_nested_norev(self):
        repos_url = self.make_client('d', 'dc')
        repos = Repository.open(repos_url)
        inv = Inventory(root_id='blabloe')
        inventory_add_external(inv, 'blabloe', 'blie/bla', repos.generate_revision_id(1, ""), None, repos_url)
        self.assertEqual(TreeReference(generate_svn_file_id(repos.uuid, 0, "", ""),
             'bla', inv.path2id('blie'), revision=repos.generate_revision_id(1, "")), inv[inv.path2id('blie/bla')])

    def test_add_simple_norev(self):
        repos_url = self.make_client('d', 'dc')
        repos = Repository.open(repos_url)
        inv = Inventory(root_id='blabloe')
        inventory_add_external(inv, 'blabloe', 'bla', repos.generate_revision_id(1, ""), None, repos_url)
        self.assertEqual(TreeReference(generate_svn_file_id(repos.uuid, 0, "", ""),
             'bla', 'blabloe', revision=repos.generate_revision_id(1, "")), inv[inv.path2id('bla')])

    def test_add_simple_rev(self):
        repos_url = self.make_client('d', 'dc')
        repos = Repository.open(repos_url)
        inv = Inventory(root_id='blabloe')
        inventory_add_external(inv, 'blabloe', 'bla', repos.generate_revision_id(1, ""), 20, repos_url)
        self.assertEqual(TreeReference(generate_svn_file_id(repos.uuid, 0, "", ""),
             'bla', 'blabloe', revision=repos.generate_revision_id(1, ""),
             reference_revision=repos.generate_revision_id(20, "")
             ), inv[inv.path2id('bla')])
        ie = inv[inv.path2id('bla')]
        self.assertEqual(repos.generate_revision_id(20, ""), ie.reference_revision)
        self.assertEqual(repos.generate_revision_id(1, ""), ie.revision)
