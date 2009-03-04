# Copyright (C) 2006-2009 Jelmer Vernooij <jelmer@samba.org>
# -*- coding: utf-8 -*-

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

"""Working tree tests."""

import os

from bzrlib import osutils
from bzrlib.branch import Branch
from bzrlib.bzrdir import BzrDir
from bzrlib.errors import (
    NoSuchFile,
    NotBranchError,
    OutOfDateTree,
    )
from bzrlib.inventory import Inventory
from bzrlib.osutils import (
    has_symlinks,
    supports_executable,
    )
from bzrlib.repository import Repository
from bzrlib.tests import (
    TestCase,
    TestSkipped,
    )
from bzrlib.trace import mutter
from bzrlib.workingtree import WorkingTree

from bzrlib.plugins.svn.layout.standard import TrunkLayout
from bzrlib.plugins.svn.mapping3.base import config_set_scheme
from bzrlib.plugins.svn.mapping3.scheme import TrunkBranchingScheme
from bzrlib.plugins.svn.transport import svn_config
from bzrlib.plugins.svn.tests import SubversionTestCase
from bzrlib.plugins.svn.workingtree import generate_ignore_list

class TestWorkingTree(SubversionTestCase):
    def test_add_duplicate(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        self.client_add("dc/bl")
        tree = WorkingTree.open("dc")
        tree.add(["bl"])

    def test_add_unexisting(self):
        self.make_client('a', 'dc')
        tree = WorkingTree.open("dc")
        self.assertRaises(NoSuchFile, tree.add, ["bl"])

    def test_add(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        tree = WorkingTree.open("dc")
        tree.add(["bl"])

        inv = tree.read_working_inventory()
        self.assertIsInstance(inv, Inventory)
        self.assertTrue(inv.has_filename("bl"))
        self.assertFalse(inv.has_filename("aa"))

    def test_special_char(self):
        self.make_client('a', 'dc')
        try:
            self.build_tree({u"dc/I²C": "data"})
        except UnicodeError:
            raise TestSkipped("This platform does not support unicode paths")
        self.client_add("dc/I²C")
        tree = WorkingTree.open("dc")
        inv = tree.read_working_inventory()
        self.assertIsInstance(inv, Inventory)
        self.assertTrue(inv.has_filename(u"I²C"))

    def test_not_branch_path(self):
        repos_url = self.make_client('a', 'dc')
        self.build_tree({"dc/trunk/file": "data"})
        self.client_add("dc/trunk")
        self.client_commit("dc", "initial")
        self.client_update("dc")
        self.build_tree({"dc/trunk/dir": None})
        self.client_add("dc/trunk/dir")
        config_set_scheme(Repository.open(repos_url), TrunkBranchingScheme(0), 
                          None, True)
        Repository.open(repos_url).store_layout(TrunkLayout(0))
        self.assertRaises(NotBranchError, WorkingTree.open, "dc")
        self.assertRaises(NotBranchError, WorkingTree.open, "dc/trunk/dir")
        tree = WorkingTree.open("dc/trunk")

    def test_smart_add_file(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        tree = WorkingTree.open("dc")
        tree.smart_add(["dc/bl"])

        inv = tree.read_working_inventory()
        self.assertIsInstance(inv, Inventory)
        self.assertTrue(inv.has_filename("bl"))
        self.assertFalse(inv.has_filename("aa"))

    def test_is_control_filename(self):
        self.make_client('a', 'dc')
        bzrdir = BzrDir.open("dc")
        self.assertTrue(bzrdir.is_control_filename(".svn"))
        self.assertTrue(bzrdir.is_control_filename(".svn/lock"))
        self.assertFalse(bzrdir.is_control_filename("lock"))

    def test_smart_add_recurse(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl/foo": "data"})
        tree = WorkingTree.open("dc")
        tree.smart_add(["dc/bl"])

        inv = tree.read_working_inventory()
        self.assertIsInstance(inv, Inventory)
        self.assertTrue(inv.has_filename("bl"))
        self.assertTrue(inv.has_filename("bl/foo"))
        self.assertFalse(inv.has_filename("aa"))

    def test_smart_add_recurse_more(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl/foo/da": "data"})
        tree = WorkingTree.open("dc")
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
        tree = WorkingTree.open("dc")
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
        tree = WorkingTree.open("dc")
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
        tree = WorkingTree.open("dc")
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
        tree = WorkingTree.open("dc")
        tree.add("bl")

        inv = tree.read_working_inventory()
        self.assertIsInstance(inv, Inventory)
        self.assertTrue(inv.has_filename("bl"))
        self.assertFalse(inv.has_filename("aa"))

    def test_add_nolist_withid(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        tree = WorkingTree.open("dc")
        tree.add("bl", "bloe")

        inv = tree.read_working_inventory()
        self.assertIsInstance(inv, Inventory)
        self.assertTrue(inv.has_filename("bl"))
        self.assertFalse(inv.has_filename("aa"))
        self.assertEqual("bloe", tree.inventory.path2id("bl"))

    def test_add_not_recursive(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl/file": "data"})
        tree = WorkingTree.open("dc")
        tree.add(["bl"])

        tree = WorkingTree.open("dc")
        self.assertTrue(tree.inventory.has_filename("bl"))
        self.assertFalse(tree.inventory.has_filename("bl/file"))

    def test_add_nested(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl/file": "data"})
        tree = WorkingTree.open("dc")
        tree.add(["bl", "bl/file"])

        tree = WorkingTree.open("dc")
        self.assertTrue(tree.inventory.has_filename("bl"))
        self.assertTrue(tree.inventory.has_filename("bl/file"))

    def test_lock_write(self):
        self.make_client('a', 'dc')
        tree = WorkingTree.open("dc")
        tree.lock_write()

    def test_lock_read(self):
        self.make_client('a', 'dc')
        tree = WorkingTree.open("dc")
        tree.lock_read()

    def test_unlock(self):
        self.make_client('a', 'dc')
        tree = WorkingTree.open("dc")
        tree.lock_read()
        tree.unlock()

    def test_get_ignore_list_empty(self):
        self.make_client('a', 'dc')
        tree = WorkingTree.open("dc")
        self.assertEqual(set([".svn"] + svn_config.get_default_ignores()), tree.get_ignore_list())

    def test_get_ignore_list_onelevel(self):
        self.make_client('a', 'dc')
        self.client_set_prop("dc", "svn:ignore", "*.d\n*.c\n")
        tree = WorkingTree.open("dc")
        self.assertEqual(set([".svn"] + svn_config.get_default_ignores() + ["./*.d", "./*.c"]), tree.get_ignore_list())

    def test_get_ignore_list_morelevel(self):
        self.make_client('a', 'dc')
        self.client_set_prop("dc", "svn:ignore", "*.d\n*.c\n")
        self.build_tree({'dc/x': None})
        self.client_add("dc/x")
        self.client_set_prop("dc/x", "svn:ignore", "*.e\n")
        tree = WorkingTree.open("dc")
        self.assertEqual(set([".svn"] + svn_config.get_default_ignores() + ["./*.d", "./*.c", "./x/*.e"]), tree.get_ignore_list())

    def test_add_reopen(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        tree = WorkingTree.open("dc")
        tree.add(["bl"])

        inv = WorkingTree.open("dc").read_working_inventory()
        self.assertTrue(inv.has_filename("bl"))

    def test_remove(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        tree = WorkingTree.open("dc")
        tree.add(["bl"])
        tree.remove(["bl"])
        inv = tree.read_working_inventory()
        self.assertFalse(inv.has_filename("bl"))

    def test_remove_dup(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        tree = WorkingTree.open("dc")
        tree.add(["bl"])
        os.remove("dc/bl")
        inv = tree.read_working_inventory()
        self.assertFalse(inv.has_filename("bl"))

    def test_is_control_file(self):
        self.make_client('a', 'dc')
        tree = WorkingTree.open("dc")
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
        tree.revert(["bl"])
        self.assertFalse(tree.changes_from(tree.basis_tree()).has_changed())
        self.assertEqual("data", open('dc/bl').read())

    def test_rename_one(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        self.client_add("dc/bl")
        self.client_commit("dc", "Bla")
        tree = WorkingTree.open("dc")
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
        wt = WorkingTree.open("dc")
        self.assertEqual(wt.branch.generate_revision_id(0), 
                         wt.basis_tree().inventory.revision_id)
        inv = Inventory()
        root_id = wt.branch.repository.get_mapping().generate_file_id((wt.branch.repository.uuid, "", 0), u"")
        inv.revision_id = wt.branch.generate_revision_id(0)
        inv.add_path('', 'directory', root_id).revision = inv.revision_id
                              
        self.assertEqual(inv, wt.basis_tree().inventory)

    def test_basis_tree(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        self.client_add("dc/bl")
        self.client_commit("dc", "Bla")
        self.client_update("dc")
        tree = WorkingTree.open("dc")
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
        tree = WorkingTree.open("dc")
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
        tree = WorkingTree.open("dc")
        self.assertEqual([Branch.open(repos_url).last_revision()], tree.get_parent_ids())
 
    def test_delta(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        self.client_add("dc/bl")
        self.build_tree({"dc/bl": "data"})
        self.client_commit("dc", "Bla")
        self.build_tree({"dc/bl": "data2"})
        tree = WorkingTree.open("dc")
        tree.basis_tree()
        delta = tree.changes_from(tree.basis_tree())
        self.assertEqual("bl", delta.modified[0][0])

    def test_pull(self):
        repos_url = self.make_client('a', 'dc')

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("trunk")
        dc.close()

        self.client_update("dc")

        dc = self.get_commit_editor(repos_url)
        branches = dc.add_dir("branches")
        foo = branches.add_dir("branches/foo", "trunk")
        dc.close()

        tree = WorkingTree.open("dc/trunk")
        old_revid = tree.last_revision()
        br = Branch.open("%s/branches/foo" % repos_url)
        result = tree.pull(br)
        self.assertEquals(tree.last_revision(), br.last_revision())
        self.assertEquals(tree.last_revision(), result.new_revid)
        self.assertEquals(2, result.new_revno)
        self.assertEquals(old_revid, result.old_revid)
        self.assertEquals(1, result.old_revno)
        self.assertEquals(None, result.master_branch)
        self.assertEquals(tree.branch, result.target_branch)
        self.assertEquals(br, result.source_branch)
 
    def test_working_inventory(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data", "dc/foo/bar": "bla", "dc/foo/bla": "aa"})
        self.client_add("dc/bl")
        self.client_add("dc/foo")
        self.client_commit("dc", "bla")
        self.build_tree({"dc/test": "data"})
        self.client_add("dc/test")
        tree = WorkingTree.open("dc")
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

        tree = WorkingTree.open("dc")
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

        tree = WorkingTree.open("dc")
        self.assertTrue(tree.is_ignored("bl/test.foo"))
        self.assertFalse(tree.is_ignored("bl/notignored"))
        self.assertTrue(tree.is_ignored("foo"))
        self.assertTrue(tree.is_ignored("bar"))
        self.assertFalse(tree.is_ignored("alsonotignored"))

    def test_ignore_controldir(self):
        self.make_client('a', 'dc')
        tree = WorkingTree.open("dc")
        self.assertEqual([], list(tree.unknowns()))

    def test_unknowns(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": None})

        tree = WorkingTree.open("dc")
        self.assertEqual(['bl'], list(tree.unknowns()))

    def test_unknown_not_added(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": None})

        tree = WorkingTree.open("dc")
        self.assertFalse(tree.inventory.has_filename("bl"))

    def test_fileid_missing(self):
        self.make_client("repos", "svn-wc")
        self.build_tree({"svn-wc/parent": None})
        self.client_add("svn-wc/parent")
        self.client_commit("svn-wc", "Added parent.")

        self.build_tree({"svn-wc/peer": None})
        self.client_add("svn-wc/peer")
        self.client_commit("svn-wc", "Added peer")

        self.build_tree({"svn-wc/peer/file": "A"})
        self.client_add("svn-wc/peer/file")
        self.client_commit("svn-wc", "Added file")
        self.client_update("svn-wc")

        self.build_tree({"svn-wc/peer/file": "B"})
        self.client_copy("svn-wc/peer", "svn-wc/parent/child")
        self.client_delete("svn-wc/peer")
        self.client_commit("svn-wc", "Made peer a child, with mods.")
        self.client_update("svn-wc")

        WorkingTree.open("svn-wc").update()
        self.client_update("svn-wc/parent")
        WorkingTree.open("svn-wc").update()

    def test_extras(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": None})

        tree = WorkingTree.open("dc")
        self.assertEqual(['.svn', 'bl'], list(tree.extras()))

    def test_executable(self):
        if not supports_executable():
            return
        self.make_client('a', 'dc')
        self.build_tree({"dc/bla": "data"})
        self.client_add("dc/bla")
        self.client_set_prop("dc/bla", "svn:executable", "*")
        tree = WorkingTree.open("dc")
        inv = tree.read_working_inventory()
        self.assertTrue(inv[inv.path2id("bla")].executable)

    def test_symlink(self):
        if not has_symlinks():
            return
        self.make_client('a', 'dc')
        os.symlink("target", "dc/bla")
        self.client_add("dc/bla")
        tree = WorkingTree.open("dc")
        inv = tree.read_working_inventory()
        self.assertEqual('symlink', inv[inv.path2id("bla")].kind)
        self.assertEqual("target", inv[inv.path2id("bla")].symlink_target)

    def test_get_parent_ids(self):
        repos_url = self.make_client('a', 'dc')
        self.build_tree({"dc/bl": None})

        lhs_parent_id = Branch.open(repos_url).last_revision()

        tree = WorkingTree.open("dc")
        tree.set_pending_merges(["a", "c"])
        self.assertEqual([lhs_parent_id, "a", "c"], tree.get_parent_ids())
        tree.set_pending_merges([])
        self.assertEqual([lhs_parent_id], tree.get_parent_ids())

    def test_set_pending_merges_svk(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/branches/foo": None})
        self.client_add("dc/branches")
        self.client_commit("dc", "add")

        self.build_tree({"dc/trunk/bl": None})
        self.client_add("dc/trunk")
        
        tree = WorkingTree.open("dc")
        tree.set_pending_merges([
            tree.branch.repository.generate_revision_id(1, "branches/foo", tree.branch.mapping), "c"])
        self.assertEqual("%s:/branches/foo:1\n" % tree.branch.repository.uuid,  
                         self.client_get_prop("dc", "svk:merge"))

    def test_commit_callback(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        self.client_add("dc/bl")
        tree = WorkingTree.open("dc")
        tree.basis_tree()
        tree.commit(message_callback=lambda x: "data")

    def test_commit_callback_unicode(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        self.client_add("dc/bl")
        tree = WorkingTree.open("dc")
        tree.basis_tree()
        tree.commit(message_callback=lambda x: u"data")

    def test_commit_message_unicode(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        self.client_add("dc/bl")
        tree = WorkingTree.open("dc")
        orig_tree = tree.basis_tree()
        tree.commit(message=u"data")

    def test_commit_nested(self):
        repos_url = self.make_client('a', 'dc')
        self.build_tree({"dc/branches/foobranch/file": "data"})
        self.client_add("dc/branches")
        self.client_commit("dc", "initial changes")
        self.make_checkout(repos_url + "/branches/foobranch", "de")
        tree = WorkingTree.open("de")
        self.build_tree({'de/file': "foo"})
        tree.basis_tree()
        tree.commit(message="data")

    def test_update_after_commit(self):
        self.make_client('a', 'dc')
        self.build_tree({"dc/bl": "data"})
        self.client_add("dc/bl")
        tree = WorkingTree.open("dc")
        orig_tree = tree.basis_tree()
        tree.commit(message="data")
        self.assertEqual(
                tree.branch.generate_revision_id(1),
                tree.basis_tree().get_revision_id())
        delta = tree.basis_tree().changes_from(tree.branch.repository.revision_tree(tree.branch.generate_revision_id(0)))
        self.assertTrue(delta.has_changed())
        tree = WorkingTree.open("dc")
        delta = tree.basis_tree().changes_from(tree)
        self.assertEqual(
             tree.branch.generate_revision_id(1),
             tree.basis_tree().get_revision_id())
        self.assertFalse(delta.has_changed())

    def test_status(self):
        self.make_client('a', 'dc')
        tree = WorkingTree.open("dc")
        self.assertTrue(os.path.exists(os.path.join("dc", ".svn")))
        self.assertFalse(os.path.exists(os.path.join("dc", ".bzr")))
        tree.read_working_inventory()

    def test_update(self):
        repos_url = self.make_client('a', 'dc')
        self.make_checkout(repos_url, "de")
        self.build_tree({'dc/bla': "data"})
        self.client_add("dc/bla")
        self.client_commit("dc", "msg")
        tree = WorkingTree.open("de")
        tree.update()
        self.assertTrue(os.path.exists(os.path.join("de", ".svn")))
        self.assertTrue(os.path.exists(os.path.join("de", "bla")))

    def test_status_bzrdir(self):
        self.make_client('a', 'dc')
        bzrdir = BzrDir.open("dc")
        self.assertTrue(os.path.exists(os.path.join("dc", ".svn")))
        self.assertTrue(not os.path.exists(os.path.join("dc", ".bzr")))
        bzrdir.open_workingtree()

    def test_file_id_consistent(self):
        self.make_client('a', 'dc')
        self.build_tree({'dc/file': 'data'})
        tree = WorkingTree.open("dc")
        tree.add(["file"])
        oldid = tree.inventory.path2id("file")
        tree = WorkingTree.open("dc")
        newid = tree.inventory.path2id("file")
        self.assertEqual(oldid, newid)

    def test_file_id_kept(self):
        self.make_client('a', 'dc')
        self.build_tree({'dc/file': 'data'})
        tree = WorkingTree.open("dc")
        tree.add(["file"], ["fooid"])
        self.assertEqual("fooid", tree.inventory.path2id("file"))
        tree = WorkingTree.open("dc")
        self.assertEqual("fooid", tree.inventory.path2id("file"))

    def test_file_rename_id(self):
        self.make_client('a', 'dc')
        self.build_tree({'dc/file': 'data'})
        tree = WorkingTree.open("dc")
        tree.add(["file"], ["fooid"])
        tree.commit("msg")
        tree.rename_one("file", "file2")
        delta = tree.branch.repository.get_revision_delta(tree.last_revision())
        self.assertEquals([("file", "fooid", "file")], delta.added)
        self.assertEqual(None, tree.inventory.path2id("file"))
        self.assertEqual("fooid", tree.inventory.path2id("file2"))
        tree = WorkingTree.open("dc")
        self.assertEqual("fooid", tree.inventory.path2id("file2"))

    def test_file_id_kept_2(self):
        self.make_client('a', 'dc')
        self.build_tree({'dc/file': 'data', 'dc/other': 'blaid'})
        tree = WorkingTree.open("dc")
        tree.add(["file", "other"], ["fooid", "blaid"])
        self.assertEqual("fooid", tree.inventory.path2id("file"))
        self.assertEqual("blaid", tree.inventory.path2id("other"))

    def test_file_remove_id(self):
        self.make_client('a', 'dc')
        self.build_tree({'dc/file': 'data'})
        tree = WorkingTree.open("dc")
        tree.add(["file"], ["fooid"])
        tree.commit("msg")
        tree.remove(["file"])
        self.assertEqual(None, tree.inventory.path2id("file"))
        tree = WorkingTree.open("dc")
        self.assertEqual(None, tree.inventory.path2id("file"))

    def test_file_move_id(self):
        self.make_client('a', 'dc')
        self.build_tree({'dc/file': 'data', 'dc/dir': None})
        tree = WorkingTree.open("dc")
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
        tree = WorkingTree.open("dc")
        tree.add(["file with spaces"], ["fooid"])
        tree.commit("msg")
        self.assertEqual("fooid", tree.inventory.path2id("file with spaces"))

    def test_get_branch_nick(self):
        self.make_client('a', 'dc')
        self.build_tree({'dc/some strange file': 'data'})
        tree = WorkingTree.open("dc")
        tree.add(["some strange file"])
        tree.commit("message")
        self.assertEqual("a", tree.branch.nick)

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
        tree = WorkingTree.open("de")
        self.build_tree({'de/some strange file': 'data-y'})
        self.assertRaises(OutOfDateTree, lambda: tree.commit("bar"))

    def test_unicode_symlink(self):
        if not has_symlinks():
            return
        repos_url = self.make_client('a', 'dc')
        try:
            self.build_tree({u"dc/\U00020001".encode(osutils._fs_enc): ""})
        except UnicodeError:
            raise TestSkipped("This platform does not support unicode code paths")
        os.symlink(u"\U00020001", "dc/a")
        self.build_tree({"dc/b": ""})
        os.symlink("b", u"dc/\U00020002")
        self.client_add("dc/a")
        self.client_add("dc/b")
        self.client_add(u"dc/\U00020001".encode("utf-8"))
        self.client_add(u"dc/\U00020002".encode("utf-8"))
        self.client_commit("dc", "Added files and links")
        WorkingTree.open("dc").read_working_inventory()


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


