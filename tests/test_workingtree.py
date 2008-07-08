# Copyright (C) 2006 Jelmer Vernooij <jelmer@samba.org>
# -*- coding: utf-8 -*-

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

"""Working tree tests."""

from bzrlib.branch import Branch
from bzrlib.bzrdir import BzrDir
from bzrlib.errors import NoSuchFile, OutOfDateTree
from bzrlib.inventory import Inventory
from bzrlib.osutils import has_symlinks, supports_executable
from bzrlib.tests import KnownFailure, TestCase
from bzrlib.trace import mutter
from bzrlib.workingtree import WorkingTree

from bzrlib.plugins.svn import wc
from bzrlib.plugins.svn.transport import svn_config
from bzrlib.plugins.svn.tests import TestCaseWithSubversionRepository
from bzrlib.plugins.svn.workingtree import generate_ignore_list

import os, sys

class TestWorkingTree(TestCaseWithSubversionRepository):
    def test_add_duplicate(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        self.client_add("dc/bl")
        tree = self.open_checkout("dc")
        tree.add(["bl"])

    def test_add_unexisting(self):
        self.make_client('a', 'dc')
        tree = self.open_checkout("dc")
        self.assertRaises(NoSuchFile, tree.add, ["bl"])

    def test_add(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        tree = self.open_checkout("dc")
        tree.add(["bl"])

        inv = tree.read_working_inventory()
        self.assertIsInstance(inv, Inventory)
        self.assertTrue(inv.has_filename("bl"))
        self.assertFalse(inv.has_filename("aa"))

    def test_special_char(self):
        self.make_client('a', 'dc')
        self.build_tree({u"dc/I²C": "data"})
        self.client_add("dc/I²C")
        tree = self.open_checkout("dc")
        inv = tree.read_working_inventory()
        self.assertIsInstance(inv, Inventory)
        self.assertTrue(inv.has_filename(u"I²C"))

    def test_smart_add_file(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        tree = self.open_checkout("dc")
        tree.smart_add(["dc/bl"])

        inv = tree.read_working_inventory()
        self.assertIsInstance(inv, Inventory)
        self.assertTrue(inv.has_filename("bl"))
        self.assertFalse(inv.has_filename("aa"))

    def test_smart_add_recurse(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl/foo": "data"})
        tree = self.open_checkout("dc")
        tree.smart_add(["dc/bl"])

        inv = tree.read_working_inventory()
        self.assertIsInstance(inv, Inventory)
        self.assertTrue(inv.has_filename("bl"))
        self.assertTrue(inv.has_filename("bl/foo"))
        self.assertFalse(inv.has_filename("aa"))

    def test_smart_add_recurse_more(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl/foo/da": "data"})
        tree = self.open_checkout("dc")
        tree.smart_add(["dc/bl"])

        inv = tree.read_working_inventory()
        self.assertIsInstance(inv, Inventory)
        self.assertTrue(inv.has_filename("bl"))
        self.assertTrue(inv.has_filename("bl/foo"))
        self.assertTrue(inv.has_filename("bl/foo/da"))
        self.assertFalse(inv.has_filename("aa"))

    def test_smart_add_more(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl/foo/da": "data", "dc/ha": "contents"})
        tree = self.open_checkout("dc")
        tree.smart_add(["dc/bl", "dc/ha"])

        inv = tree.read_working_inventory()
        self.assertIsInstance(inv, Inventory)
        self.assertTrue(inv.has_filename("bl"))
        self.assertTrue(inv.has_filename("bl/foo"))
        self.assertTrue(inv.has_filename("bl/foo/da"))
        self.assertTrue(inv.has_filename("ha"))
        self.assertFalse(inv.has_filename("aa"))

    def test_smart_add_ignored(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/.bzrignore": "bl/ha"})
        self.build_tree({"dc/bl/foo/da": "data", "dc/bl/ha": "contents"})
        tree = self.open_checkout("dc")
        tree.smart_add(["dc/bl"])

        inv = tree.read_working_inventory()
        self.assertIsInstance(inv, Inventory)
        self.assertTrue(inv.has_filename("bl"))
        self.assertTrue(inv.has_filename("bl/foo"))
        self.assertTrue(inv.has_filename("bl/foo/da"))
        self.assertFalse(inv.has_filename("ha"))
        self.assertFalse(inv.has_filename("aa"))

    def test_smart_add_toplevel(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl/foo/da": "data", "dc/ha": "contents"})
        tree = self.open_checkout("dc")
        tree.smart_add(["dc"])

        inv = tree.read_working_inventory()
        self.assertIsInstance(inv, Inventory)
        self.assertTrue(inv.has_filename("bl"))
        self.assertTrue(inv.has_filename("bl/foo"))
        self.assertTrue(inv.has_filename("bl/foo/da"))
        self.assertTrue(inv.has_filename("ha"))
        self.assertFalse(inv.has_filename("aa"))

    def test_add_nolist(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        tree = self.open_checkout("dc")
        tree.add("bl")

        inv = tree.read_working_inventory()
        self.assertIsInstance(inv, Inventory)
        self.assertTrue(inv.has_filename("bl"))
        self.assertFalse(inv.has_filename("aa"))

    def test_add_nolist_withid(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        tree = self.open_checkout("dc")
        tree.add("bl", "bloe")

        inv = tree.read_working_inventory()
        self.assertIsInstance(inv, Inventory)
        self.assertTrue(inv.has_filename("bl"))
        self.assertFalse(inv.has_filename("aa"))
        self.assertEqual("bloe", tree.inventory.path2id("bl"))

    def test_add_not_recursive(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl/file": "data"})
        tree = self.open_checkout("dc")
        tree.add(["bl"])

        tree = WorkingTree.open("dc")
        self.assertTrue(tree.inventory.has_filename("bl"))
        self.assertFalse(tree.inventory.has_filename("bl/file"))

    def test_add_nested(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl/file": "data"})
        tree = self.open_checkout("dc")
        tree.add(["bl", "bl/file"])

        tree = WorkingTree.open("dc")
        self.assertTrue(tree.inventory.has_filename("bl"))
        self.assertTrue(tree.inventory.has_filename("bl/file"))

    def test_lock_write(self):
        self.make_client('a', 'dc')
        tree = self.open_checkout("dc")
        tree.lock_write()

    def test_lock_read(self):
        self.make_client('a', 'dc')
        tree = self.open_checkout("dc")
        tree.lock_read()

    def test_unlock(self):
        self.make_client('a', 'dc')
        tree = self.open_checkout("dc")
        tree.lock_read()
        tree.unlock()

    def test_get_ignore_list_empty(self):
        self.make_client('a', 'dc')
        tree = self.open_checkout("dc")
        self.assertEqual(set([".svn"] + svn_config.get_default_ignores()), tree.get_ignore_list())

    def test_get_ignore_list_onelevel(self):
        self.make_client('a', 'dc')
        self.client_set_prop("dc", "svn:ignore", "*.d\n*.c\n")
        tree = self.open_checkout("dc")
        self.assertEqual(set([".svn"] + svn_config.get_default_ignores() + ["./*.d", "./*.c"]), tree.get_ignore_list())

    def test_get_ignore_list_morelevel(self):
        self.make_client('a', 'dc')
        self.client_set_prop("dc", "svn:ignore", "*.d\n*.c\n")
        self.build_tree({'dc/x': None})
        self.client_add("dc/x")
        self.client_set_prop("dc/x", "svn:ignore", "*.e\n")
        tree = self.open_checkout("dc")
        self.assertEqual(set([".svn"] + svn_config.get_default_ignores() + ["./*.d", "./*.c", "./x/*.e"]), tree.get_ignore_list())

    def test_add_reopen(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        tree = self.open_checkout("dc")
        tree.add(["bl"])

        inv = WorkingTree.open("dc").read_working_inventory()
        self.assertTrue(inv.has_filename("bl"))

    def test_remove(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        tree = self.open_checkout("dc")
        tree.add(["bl"])
        tree.remove(["bl"])
        inv = tree.read_working_inventory()
        self.assertFalse(inv.has_filename("bl"))

    def test_remove_dup(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        tree = self.open_checkout("dc")
        tree.add(["bl"])
        os.remove("dc/bl")
        inv = tree.read_working_inventory()
        self.assertFalse(inv.has_filename("bl"))

    def test_is_control_file(self):
        self.make_client('a', 'dc')
        tree = self.open_checkout("dc")
        self.assertTrue(tree.is_control_filename(".svn"))
        self.assertFalse(tree.is_control_filename(".bzr"))

    def test_revert(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        self.client_add("dc/bl")
        self.client_commit("dc", "Bla")
        self.client_update("dc")
        tree = WorkingTree.open("dc")
        os.remove("dc/bl")
        raise KnownFailure("revert not supported yet")
        tree.revert(["bl"])
        self.assertEqual("data", open('dc/bl').read())

    def test_rename_one(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        self.client_add("dc/bl")
        self.client_commit("dc", "Bla")
        tree = self.open_checkout("dc")
        tree.rename_one("bl", "bloe")
        
        basis_inv = tree.basis_tree().inventory
        inv = tree.read_working_inventory()
        self.assertFalse(inv.has_filename("bl"))
        self.assertTrue(inv.has_filename("bloe"))
        self.assertEqual(basis_inv.path2id("bl"), 
                         inv.path2id("bloe"))
        self.assertIs(None, inv.path2id("bl"))
        self.assertIs(None, basis_inv.path2id("bloe"))

    def test_empty_basis_tree(self):
        self.make_client('a', 'dc')
        wt = self.open_checkout("dc")
        self.assertEqual(wt.branch.generate_revision_id(0), 
                         wt.basis_tree().inventory.revision_id)
        inv = Inventory()
        root_id = wt.branch.repository.get_mapping().generate_file_id(wt.branch.repository.uuid, 0, "", u"")
        inv.revision_id = wt.branch.generate_revision_id(0)
        inv.add_path('', 'directory', root_id).revision = inv.revision_id
                              
        self.assertEqual(inv, wt.basis_tree().inventory)

    def test_basis_tree(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        self.client_add("dc/bl")
        self.client_commit("dc", "Bla")
        self.client_update("dc")
        tree = self.open_checkout("dc")
        self.assertEqual(
            tree.branch.generate_revision_id(1),
            tree.basis_tree().get_revision_id())

    def test_move(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data", "dc/a": "data2", "dc/dir": None})
        self.client_add("dc/bl")
        self.client_add("dc/a")
        self.client_add("dc/dir")
        self.client_commit("dc", "Bla")
        tree = self.open_checkout("dc")
        tree.move(["bl", "a"], "dir")
        
        basis_inv = tree.basis_tree().inventory
        inv = tree.read_working_inventory()
        self.assertFalse(inv.has_filename("bl"))
        self.assertFalse(inv.has_filename("a"))
        self.assertTrue(inv.has_filename("dir/bl"))
        self.assertTrue(inv.has_filename("dir/a"))
        mutter('basis: %r' % basis_inv.entries())
        mutter('working: %r' % inv.entries())
        self.assertFalse(inv.has_filename("bl"))
        self.assertFalse(basis_inv.has_filename("dir/bl"))

    def test_get_parent_ids_no_merges(self):
        repos_url = self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        tree = self.open_checkout("dc")
        self.assertEqual([Branch.open(repos_url).last_revision()], tree.get_parent_ids())
 
    def test_delta(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        self.client_add("dc/bl")
        self.build_tree({"dc/bl": "data"})
        self.client_commit("dc", "Bla")
        self.build_tree({"dc/bl": "data2"})
        tree = self.open_checkout("dc")
        tree.basis_tree()
        delta = tree.changes_from(tree.basis_tree())
        self.assertEqual("bl", delta.modified[0][0])
 
    def test_working_inventory(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data", "dc/foo/bar": "bla", "dc/foo/bla": "aa"})
        self.client_add("dc/bl")
        self.client_add("dc/foo")
        self.client_commit("dc", "bla")
        self.build_tree({"dc/test": "data"})
        self.client_add("dc/test")
        tree = self.open_checkout("dc")
        inv = tree.read_working_inventory()
        self.assertEqual(inv.path2id(""), inv.root.file_id)
        self.assertTrue(inv.path2id("foo") != "")
        self.assertTrue(inv.has_filename("bl"))
        self.assertTrue(inv.has_filename("foo"))
        self.assertTrue(inv.has_filename("foo/bar"))
        self.assertTrue(inv.has_filename("test"))

    def test_ignore_list(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": None})
        self.client_add("dc/bl")
        self.client_set_prop("dc/bl", "svn:ignore", "test.*\n")
        self.client_commit("dc", "bla")
        self.client_set_prop("dc", "svn:ignore", "foo\nbar\n")

        tree = self.open_checkout("dc")
        ignorelist = tree.get_ignore_list()
        self.assertTrue("./bl/test.*" in ignorelist)
        self.assertTrue("./foo" in ignorelist)
        self.assertTrue("./bar" in ignorelist)

    def test_is_ignored(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": None})
        self.client_add("dc/bl")
        self.client_set_prop("dc/bl", "svn:ignore", "test.*\n")
        self.client_commit("dc", "bla")
        self.client_set_prop("dc", "svn:ignore", "foo\nbar\n")

        tree = self.open_checkout("dc")
        self.assertTrue(tree.is_ignored("bl/test.foo"))
        self.assertFalse(tree.is_ignored("bl/notignored"))
        self.assertTrue(tree.is_ignored("foo"))
        self.assertTrue(tree.is_ignored("bar"))
        self.assertFalse(tree.is_ignored("alsonotignored"))

    def test_ignore_controldir(self):
        self.make_client('a', 'dc')
        tree = self.open_checkout("dc")
        self.assertEqual([], list(tree.unknowns()))

    def test_unknowns(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": None})

        tree = self.open_checkout("dc")
        self.assertEqual(['bl'], list(tree.unknowns()))

    def test_unknown_not_added(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": None})

        tree = self.open_checkout("dc")
        self.assertFalse(tree.inventory.has_filename("bl"))

    def test_extras(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": None})

        tree = self.open_checkout("dc")
        self.assertEqual(['.svn', 'bl'], list(tree.extras()))

    def test_executable(self):
        if not supports_executable():
            return
        self.make_client('a', 'dc')
        self.build_tree({"dc/bla": "data"})
        self.client_add("dc/bla")
        self.client_set_prop("dc/bla", "svn:executable", "*")
        tree = self.open_checkout("dc")
        inv = tree.read_working_inventory()
        self.assertTrue(inv[inv.path2id("bla")].executable)

    def test_symlink(self):
        if not has_symlinks():
            return
        self.make_client('a', 'dc')
        os.symlink("target", "dc/bla")
        self.client_add("dc/bla")
        tree = self.open_checkout("dc")
        inv = tree.read_working_inventory()
        self.assertEqual('symlink', inv[inv.path2id("bla")].kind)
        self.assertEqual("target", inv[inv.path2id("bla")].symlink_target)

    def test_get_parent_ids(self):
        repos_url = self.make_client('a', 'dc')
        self.build_tree({"dc/bl": None})

        lhs_parent_id = Branch.open(repos_url).last_revision()

        tree = self.open_checkout("dc")
        tree.set_pending_merges(["a", "c"])
        self.assertEqual([lhs_parent_id, "a", "c"], tree.get_parent_ids())
        tree.set_pending_merges([])
        self.assertEqual([lhs_parent_id], tree.get_parent_ids())

    def test_set_pending_merges_prop(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": None})
        self.client_add("dc/bl")
        
        tree = self.open_checkout("dc")
        tree.set_pending_merges([
            tree.branch.mapping.generate_revision_id("a-uuid-foo", 1, "branch/fpath"), "c"])
        self.assertEqual(
                "svn-v3-none:a-uuid-foo:branch%2Ffpath:1\tc\n",
                self.client_get_prop("dc", "bzr:ancestry:v3-none"))

    def test_set_pending_merges_svk(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": None})
        self.client_add("dc/bl")
        
        tree = self.open_checkout("dc")
        tree.set_pending_merges([
            tree.branch.mapping.generate_revision_id("a-uuid-foo", 1, "branch/path"), "c"])
        self.assertEqual("a-uuid-foo:/branch/path:1\n", 
                         self.client_get_prop("dc", "svk:merge"))

    def test_commit_callback(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        self.client_add("dc/bl")
        tree = self.open_checkout("dc")
        tree.basis_tree()
        tree.commit(message_callback=lambda x: "data")

    def test_commit_callback_unicode(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        self.client_add("dc/bl")
        tree = self.open_checkout("dc")
        tree.basis_tree()
        tree.commit(message_callback=lambda x: u"data")

    def test_commit_message_unicode(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        self.client_add("dc/bl")
        tree = self.open_checkout("dc")
        orig_tree = tree.basis_tree()
        tree.commit(message=u"data")

    def test_commit_nested(self):
        repos_url = self.make_client('a', 'dc')
        self.build_tree({"dc/branches/foobranch/file": "data"})
        self.client_add("dc/branches")
        self.client_commit("dc", "initial changes")
        self.make_checkout(repos_url + "/branches/foobranch", "de")
        tree = self.open_checkout("de")
        self.build_tree({'de/file': "foo"})
        tree.basis_tree()
        tree.commit(message="data")

    def test_update_after_commit(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        self.client_add("dc/bl")
        tree = self.open_checkout("dc")
        orig_tree = tree.basis_tree()
        tree.commit(message="data")
        self.assertEqual(
                tree.branch.generate_revision_id(1),
                tree.basis_tree().get_revision_id())
        delta = tree.basis_tree().changes_from(orig_tree)
        self.assertTrue(delta.has_changed())
        tree = WorkingTree.open("dc")
        delta = tree.basis_tree().changes_from(tree)
        self.assertEqual(
             tree.branch.generate_revision_id(1),
             tree.basis_tree().get_revision_id())
        self.assertFalse(delta.has_changed())

    def test_status(self):
        self.make_client('a', 'dc')
        tree = self.open_checkout("dc")
        self.assertTrue(os.path.exists(os.path.join("dc", ".svn")))
        self.assertFalse(os.path.exists(os.path.join("dc", ".bzr")))
        tree.read_working_inventory()

    def test_update(self):
        repos_url = self.make_client('a', 'dc')
        self.make_checkout(repos_url, "de")
        self.build_tree({'dc/bla': "data"})
        self.client_add("dc/bla")
        self.client_commit("dc", "msg")
        tree = self.open_checkout("de")
        tree.update()
        self.assertTrue(os.path.exists(os.path.join("de", ".svn")))
        self.assertTrue(os.path.exists(os.path.join("de", "bla")))

    def test_status_bzrdir(self):
        self.make_client('a', 'dc')
        bzrdir = self.open_checkout_bzrdir("dc")
        self.assertTrue(os.path.exists(os.path.join("dc", ".svn")))
        self.assertTrue(not os.path.exists(os.path.join("dc", ".bzr")))
        bzrdir.open_workingtree()

    def test_file_id_consistent(self):
        self.make_client('a', 'dc')
        self.build_tree({'dc/file': 'data'})
        tree = self.open_checkout("dc")
        tree.add(["file"])
        oldid = tree.inventory.path2id("file")
        tree = WorkingTree.open("dc")
        newid = tree.inventory.path2id("file")
        self.assertEqual(oldid, newid)

    def test_file_id_kept(self):
        self.make_client('a', 'dc')
        self.build_tree({'dc/file': 'data'})
        tree = self.open_checkout("dc")
        tree.add(["file"], ["fooid"])
        self.assertEqual("fooid", tree.inventory.path2id("file"))
        tree = WorkingTree.open("dc")
        self.assertEqual("fooid", tree.inventory.path2id("file"))

    def test_file_rename_id(self):
        self.make_client('a', 'dc')
        self.build_tree({'dc/file': 'data'})
        tree = self.open_checkout("dc")
        tree.add(["file"], ["fooid"])
        tree.commit("msg")
        tree.rename_one("file", "file2")
        self.assertEqual(None, tree.inventory.path2id("file"))
        self.assertEqual("fooid", tree.inventory.path2id("file2"))
        tree = WorkingTree.open("dc")
        self.assertEqual("fooid", tree.inventory.path2id("file2"))

    def test_file_id_kept_2(self):
        self.make_client('a', 'dc')
        self.build_tree({'dc/file': 'data', 'dc/other': 'blaid'})
        tree = self.open_checkout("dc")
        tree.add(["file", "other"], ["fooid", "blaid"])
        self.assertEqual("fooid", tree.inventory.path2id("file"))
        self.assertEqual("blaid", tree.inventory.path2id("other"))

    def test_file_remove_id(self):
        self.make_client('a', 'dc')
        self.build_tree({'dc/file': 'data'})
        tree = self.open_checkout("dc")
        tree.add(["file"], ["fooid"])
        tree.commit("msg")
        tree.remove(["file"])
        self.assertEqual(None, tree.inventory.path2id("file"))
        tree = WorkingTree.open("dc")
        self.assertEqual(None, tree.inventory.path2id("file"))

    def test_file_move_id(self):
        self.make_client('a', 'dc')
        self.build_tree({'dc/file': 'data', 'dc/dir': None})
        tree = self.open_checkout("dc")
        tree.add(["file", "dir"], ["fooid", "blaid"])
        tree.commit("msg")
        tree.move(["file"], "dir")
        self.assertEqual(None, tree.inventory.path2id("file"))
        self.assertEqual("fooid", tree.inventory.path2id("dir/file"))
        tree = WorkingTree.open("dc")
        self.assertEqual(None, tree.inventory.path2id("file"))
        self.assertEqual("fooid", tree.inventory.path2id("dir/file"))

    def test_escaped_char_filename(self):
        self.make_client('a', 'dc')
        self.build_tree({'dc/file with spaces': 'data'})
        tree = self.open_checkout("dc")
        tree.add(["file with spaces"], ["fooid"])
        tree.commit("msg")
        self.assertEqual("fooid", tree.inventory.path2id("file with spaces"))

    def test_get_branch_nick(self):
        self.make_client('a', 'dc')
        self.build_tree({'dc/some strange file': 'data'})
        tree = self.open_checkout("dc")
        tree.add(["some strange file"])
        tree.commit("message")
        self.assertEqual(None, tree.branch.nick)

    def test_out_of_date(self):
        repos_url = self.make_client('a', 'dc')
        self.build_tree({'dc/some strange file': 'data'})
        self.client_add("dc/some strange file")
        self.client_commit("dc", "msg")
        self.client_update("dc")
        self.make_checkout(repos_url, 'de')
        self.build_tree({'dc/some strange file': 'data-x'})
        self.client_commit("dc", "msg")
        self.client_update("dc")
        tree = self.open_checkout("de")
        self.build_tree({'de/some strange file': 'data-y'})
        self.assertRaises(OutOfDateTree, lambda: tree.commit("bar"))


class IgnoreListTests(TestCase):
    def test_empty(self):
        self.assertEquals([], generate_ignore_list({}))

    def test_simple(self):
        self.assertEquals(["./twin/peaks"], 
                generate_ignore_list({"twin": "peaks"}))

    def test_toplevel(self):
        self.assertEquals(["./twin*"], 
                generate_ignore_list({"": "twin*"}))

    def test_multiple(self):
        self.assertEquals(["./twin*", "./twin/peaks"], 
                generate_ignore_list({"twin": "peaks", "": "twin*"}))


