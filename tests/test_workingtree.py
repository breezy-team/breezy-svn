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
import shutil

from subvertpy.wc import (
    check_wc,
    )

from breezy import (
    osutils,
    revision as _mod_revision,
    )
from breezy.branch import Branch
from breezy.config import GlobalStack
from breezy.controldir import ControlDir
from breezy.errors import (
    NoSuchFile,
    NotBranchError,
    OutOfDateTree,
    )
from breezy.bzr.inventory import Inventory
from breezy.osutils import (
    supports_executable,
    )
from breezy.repository import Repository
from breezy.tests import (
    TestCase,
    )
from breezy.tests.features import (
    SymlinkFeature,
    UnicodeFilenameFeature,
    )
from breezy.workingtree import WorkingTree

from breezy.plugins.svn.layout.standard import TrunkLayout
from breezy.plugins.svn.mapping3.base import config_set_scheme
from breezy.plugins.svn.mapping3.scheme import TrunkBranchingScheme
from breezy.plugins.svn.transport import svn_config
from breezy.plugins.svn.tests import SubversionTestCase
from breezy.plugins.svn.workingtree import (
    CorruptWorkingTree,
    )

class TestWorkingTree(SubversionTestCase):

    def setUp(self):
        super(TestWorkingTree, self).setUp()
        config = GlobalStack()
        config.set('allow_metadata_in_file_properties', True)
        config.store.save()

    def assertCleanTree(self, wt):
        delta = wt.changes_from(wt.basis_tree())
        self.assertFalse(delta.has_changed(), delta)

    def test_invalid_entries(self):
        self.make_svn_branch_and_tree('a', 'dc')
        entries_path = os.path.join(self.test_dir, 'dc/.svn/entries')
        os.chmod(entries_path, 0o755)
        num = check_wc("dc")
        f = open(entries_path, "w+")
        try:
            f.write("%d\n" % num)
        finally:
            f.close()
        self.assertRaises(CorruptWorkingTree, WorkingTree.open, "dc")

    def test_add_duplicate(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl": b"data"})
        self.client_add("dc/bl")
        tree.add(["bl"])

    def test_add_unexisting(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.assertRaises(NoSuchFile, tree.add, ["bl"])

    def test_add(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl": b"data"})
        tree.add(["bl"])

        self.assertEquals(
            set(["aa"]), tree.filter_unversioned_files(["bl", "aa"]))

    def test_special_char(self):
        self.requireFeature(UnicodeFilenameFeature)
        self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({u"dc/I²C": b"data"})
        self.client_add("dc/I²C")
        tree = WorkingTree.open("dc")
        self.assertEquals(
            set(),
            tree.filter_unversioned_files([u"I²C"]))

    def test_not_branch_path(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/file": b"data"})
        self.client_add("dc/file")
        self.client_commit("dc", "initial")
        self.client_update("dc")
        self.build_tree({"dc/dir": None})
        self.client_add("dc/dir")
        config_set_scheme(Repository.open(tree.branch.repository.base), TrunkBranchingScheme(0),
                          None, True)
        Repository.open(tree.branch.repository.base).store_layout(TrunkLayout(0))
        self.assertRaises(NotBranchError, WorkingTree.open, "dc/foobar")
        self.assertRaises(NotBranchError, WorkingTree.open, "dc/dir")
        WorkingTree.open("dc")

    def test_smart_add_file(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl": b"data"})
        tree.smart_add(["dc/bl"])
        self.assertEquals(
            set(["aa"]), tree.filter_unversioned_files(["bl", "aa"]))

    def test_smart_add_unicode(self):
        self.requireFeature(UnicodeFilenameFeature)
        tree = self.make_svn_branch_and_tree('a', u'dć'.encode(osutils._fs_enc))
        self.build_tree({u"dć/bé".encode(osutils._fs_enc): b"data"})
        tree.smart_add([u"dć/bé"])
        self.assertEquals(set(), tree.filter_unversioned_files([u"bé"]))

    def test_is_control_filename(self):
        self.make_svn_branch_and_tree('a', 'dc')
        controldir = ControlDir.open("dc")
        self.assertTrue(controldir.is_control_filename(".svn"))
        self.assertTrue(controldir.is_control_filename(".svn/lock"))
        self.assertFalse(controldir.is_control_filename("lock"))

    def test_smart_add_recurse(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl/foo": b"data"})
        tree.smart_add(["dc/bl"])

        self.assertEquals(set(["aa"]),
            tree.filter_unversioned_files(["bl", "bl/foo", "aa"]))

    def test_smart_add_recurse_more(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl/foo/da": b"data"})
        tree.smart_add(["dc/bl"])

        self.assertEquals(set(["aa"]),
            tree.filter_unversioned_files(["bl", "bl/foo", "bl/foo/da", "aa"]))

    def test_smart_add_more(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl/foo/da": b"data", "dc/ha": b"contents"})
        tree.smart_add(["dc/bl", "dc/ha"])

        self.assertEquals(set(["aa"]),
            tree.filter_unversioned_files(["bl", "bl/foo", "bl/foo/da", "ha", "aa"]))

    def test_smart_add_ignored(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/.bzrignore": b"bl/ha"})
        self.build_tree({"dc/bl/foo/da": b"data", "dc/bl/ha": b"contents"})
        tree.smart_add(["dc/bl"])

        self.assertEquals(set(["ha", "aa"]),
            tree.filter_unversioned_files(["bl", "bl/foo", "bl/foo/da", "ha", "aa"]))

    def test_smart_add_toplevel(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl/foo/da": b"data", "dc/ha": b"contents"})
        tree.smart_add(["dc"])

        self.assertEquals(set(["aa"]),
            tree.filter_unversioned_files(["bl", "bl/foo", "bl/foo/da", "ha", "aa"]))

    def test_add_nolist(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl": b"data"})
        tree.add("bl")

        self.assertEquals(
            set(["aa"]), tree.filter_unversioned_files(["bl", "aa"]))

    def test_add_nolist_withid(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl": b"data"})
        tree.add("bl", "bloe")

        self.assertEqual(
            set(["aa"]),
            tree.filter_unversioned_files(["bl", "aa"]))
        self.assertEqual("bloe", tree.path2id("bl"))

    def test_add_not_recursive(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl/file": b"data"})
        tree.add(["bl"])

        tree = WorkingTree.open("dc")
        self.assertEquals(set(["bl/file"]),
            tree.filter_unversioned_files(["bl/file", "bl"]))

    def test_add_nested(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl/file": b"data"})
        tree.add(["bl", "bl/file"])

        tree = WorkingTree.open("dc")
        self.assertEquals(set(),
            tree.filter_unversioned_files(["bl", "bl/file"]))

    def test_lock_write(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        with tree.lock_write():
            pass

    def test_lock_read(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        with tree.lock_read():
            pass

    def test_get_ignore_list_empty(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.assertEqual(set([".svn"] + svn_config.get_default_ignores()),
            tree.get_ignore_list())

    def test_get_ignore_list_onelevel(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.client_set_prop("dc", "svn:ignore", "*.d\n*.c\n")
        self.assertEqual(set([".svn"] + svn_config.get_default_ignores() + ["./*.d", "./*.c"]), tree.get_ignore_list())

    def test_get_ignore_list_morelevel(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.client_set_prop("dc", "svn:ignore", "*.d\n*.c\n")
        self.build_tree({'dc/x': None})
        self.client_add("dc/x")
        self.client_set_prop("dc/x", "svn:ignore", "*.e\n")
        self.assertEqual(set([".svn"] + svn_config.get_default_ignores() + ["./*.d", "./*.c", "./x/*.e"]), tree.get_ignore_list())

    def test_add_reopen(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl": b"data"})
        tree.add(["bl"])

        tree = WorkingTree.open("dc")
        self.assertEquals(set(), tree.filter_unversioned_files(["bl"]))

    def test_remove(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl": b"data"})
        tree.add(["bl"])
        tree.remove(["bl"])
        self.assertEquals(set(["bl"]), tree.filter_unversioned_files(["bl"]))

    def test_remove_dup(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl": b"data"})
        tree.add(["bl"])
        os.remove("dc/bl")
        self.assertEquals(set([]), tree.filter_unversioned_files(["bl"]))

    def test_is_control_file(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.assertTrue(tree.is_control_filename(".svn"))
        self.assertFalse(tree.is_control_filename(".bzr"))

    def test_revert(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl": b"data"})
        self.client_add("dc/bl")
        self.client_commit("dc", "Bla")
        self.client_update("dc")
        os.remove("dc/bl")
        tree.revert(["bl"])
        self.assertCleanTree(tree)
        self.assertEqual("data", open('dc/bl').read())

    def test_rename_one(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')  # 1
        self.build_tree({"dc/bl": b"data"})
        self.client_add("dc/bl")
        self.client_commit("dc", "Bla")  # 2
        tree.rename_one("bl", "bloe")

        basis_tree = tree.basis_tree()
        self.assertEquals(set(["bl"]), tree.filter_unversioned_files(["bl", "bloe"]))
        self.assertEqual(basis_tree.path2id("bl"), tree.path2id("bloe"))
        self.assertIs(None, tree.path2id("bl"))
        self.assertIs(None, basis_tree.path2id("bloe"))

    def test_set_root_id(self):
        wt = self.make_svn_branch_and_tree('a', 'dc')
        wt.set_root_id("somefileid")
        self.assertEquals("somefileid", wt.path2id(""))

    def test_empty_basis_tree(self):
        wt = self.make_svn_branch_and_tree('a', 'dc', lossy=True)
        self.assertEqual(wt.branch.generate_revision_id(1),
                         wt.basis_tree().get_revision_id())
        root_id = wt.branch.repository.get_mapping().generate_file_id(
                (wt.branch.repository.uuid, wt.branch.get_branch_path(), 1), u"")
        self.assertEquals(wt.path2id(''), root_id)

    def test_basis_tree(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl": b"data"})
        self.client_add("dc/bl")
        self.client_commit("dc", "Bla")
        self.client_update("dc")
        self.assertEqual(
            tree.branch.generate_revision_id(2),
            tree.basis_tree().get_revision_id())

    def test_move(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl": b"data", "dc/a": b"data2", "dc/dir": None})
        self.client_add("dc/bl")
        self.client_add("dc/a")
        self.client_add("dc/dir")
        self.client_commit("dc", "Bla")
        tree.move(["bl", "a"], "dir")
        self.assertEquals(
            set(["bl", "a"]),
            tree.filter_unversioned_files(["bl", "a", "dir/bl", "dir/a"]))
        self.assertFalse(tree.basis_tree().has_filename("dir/bl"))

    def test_get_parent_ids_no_merges(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl": b"data"})
        self.client_add("dc/bl")
        self.client_commit('dc', 'msg')
        self.build_tree({"dc/bl": b"data"})
        self.assertEqual([tree.branch.last_revision()], tree.get_parent_ids())

    def test_delta(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl": b"data"})
        self.client_add("dc/bl")
        self.build_tree({"dc/bl": b"data"})
        self.client_commit("dc", "Bla")
        self.build_tree({"dc/bl": b"data2"})
        tree.basis_tree()
        delta = tree.changes_from(tree.basis_tree())
        self.assertEqual("bl", delta.modified[0][0])

    def test_update_unversioned_exists(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')

        with self.get_commit_editor(tree.branch.base) as dc:
            f = dc.add_file("somefile")
            f.modify()

        self.build_tree({'dc/somefile': 'some contents'})
        tree.update()

    def test_pull(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')

        with self.get_commit_editor(tree.branch.repository.base) as dc:
            branches = dc.add_dir("branches")
            foo = branches.add_dir("branches/foo", "trunk")

        old_revid = tree.last_revision()
        br = Branch.open("%s/branches/foo" % tree.branch.repository.base)
        result = tree.pull(br)
        self.assertEquals(tree.last_revision(), br.last_revision())
        self.assertEquals(tree.last_revision(), result.new_revid)
        self.assertEquals(1, result.new_revno)
        self.assertEquals(old_revid, result.old_revid)
        self.assertEquals(0, result.old_revno)
        self.assertEquals(tree.branch, result.master_branch)
        self.assertEquals(tree.branch, result.target_branch)
        self.assertEquals(br, result.source_branch)

    def test_ignore_list(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl": None})
        self.client_add("dc/bl")
        self.client_set_prop("dc/bl", "svn:ignore", "test.*\n")
        self.client_commit("dc", "bla")
        self.client_set_prop("dc", "svn:ignore", "foo\nbar\n")
        ignorelist = tree.get_ignore_list()
        self.assertTrue("./bl/test.*" in ignorelist)
        self.assertTrue("./foo" in ignorelist)
        self.assertTrue("./bar" in ignorelist)

    def test_is_ignored(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl": None})
        self.client_add("dc/bl")
        self.client_set_prop("dc/bl", "svn:ignore", "test.*\n")
        self.client_commit("dc", "bla")
        self.client_set_prop("dc", "svn:ignore", "foo\nbar\n")
        self.assertTrue(tree.is_ignored("bl/test.foo"))
        self.assertFalse(tree.is_ignored("bl/notignored"))
        self.assertTrue(tree.is_ignored("foo"))
        self.assertTrue(tree.is_ignored("bar"))
        self.assertFalse(tree.is_ignored("alsonotignored"))

    def test_ignore_controldir(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.assertEqual([], list(tree.unknowns()))

    def test_unknowns(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl": None})

        self.assertEqual(['bl'], list(tree.unknowns()))

    def test_unknown_not_added(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl": None})
        self.assertEquals(set(["bl"]),
            tree.filter_unversioned_files(["bl"]))

    def test_fileid_missing(self):
        self.make_svn_branch_and_tree("repos", "svn-wc")
        self.build_tree({"svn-wc/parent": None})
        self.client_add("svn-wc/parent")
        self.client_commit("svn-wc", "Added parent.")

        self.build_tree({"svn-wc/peer": None})
        self.client_add("svn-wc/peer")
        self.client_commit("svn-wc", "Added peer")

        self.build_tree({"svn-wc/peer/file": b"A"})
        self.client_add("svn-wc/peer/file")
        self.client_commit("svn-wc", "Added file")
        self.client_update("svn-wc")

        self.build_tree({"svn-wc/peer/file": b"B"})
        self.client_copy("svn-wc/peer", "svn-wc/parent/child")
        self.client_delete("svn-wc/peer")
        self.client_commit("svn-wc", "Made peer a child, with mods.")
        self.client_update("svn-wc")

        WorkingTree.open("svn-wc").update()
        self.client_update("svn-wc/parent")
        WorkingTree.open("svn-wc").update()

    def test_extras(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl": None})
        extras = list(tree.extras())
        self.assertEqual([u'bl'], extras)

    def test_executable(self):
        if not supports_executable():
            return
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bla": b"data"})
        self.client_add("dc/bla")
        self.client_set_prop("dc/bla", "svn:executable", "*")
        self.assertTrue(tree.is_executable("bla"))

    def test_symlink(self):
        self.requireFeature(SymlinkFeature)
        tree = self.make_svn_branch_and_tree('a', 'dc')
        os.symlink("target", "dc/bla")
        self.client_add("dc/bla")
        self.assertEqual('symlink', tree.kind("bla"))
        self.assertEqual("target", tree.get_symlink_target("bla"))

    def test_get_parent_ids(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl": None})
        self.client_add("dc/bl")
        self.client_commit("dc", "msg")

        lhs_parent_id = tree.branch.last_revision()

        tree.set_pending_merges(["a", "c"])
        self.assertEqual([lhs_parent_id, "a", "c"], tree.get_parent_ids())
        tree.set_pending_merges([])
        self.assertEqual([lhs_parent_id], tree.get_parent_ids())

    def test_set_pending_merges_svk(self):
        repos_url = self.make_client('a', 'dc')
        self.build_tree({"dc/branches/foo": None})
        self.client_add("dc/branches")
        self.client_commit("dc", "add")

        self.build_tree({"dc/trunk/bl": None})
        self.client_add("dc/trunk")
        self.client_commit("dc", "add trunk")

        tree = WorkingTree.open("dc/trunk")

        tree.set_pending_merges([
            tree.branch.repository.generate_revision_id(1, u"branches/foo", tree.branch.mapping), "c"])
        self.assertEqual("%s:/branches/foo:1\n" % tree.branch.repository.uuid,
                         self.client_get_prop("dc/trunk", "svk:merge"))

    def test_commit_callback(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl": b"data"})
        self.client_add("dc/bl")
        tree.basis_tree()
        tree.commit(message_callback=lambda x: "data")

    def test_commit_callback_unicode(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl": b"data"})
        self.client_add("dc/bl")
        tree.basis_tree()
        tree.commit(message_callback=lambda x: u"data")

    def test_commit_message_unicode(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bl": b"data"})
        self.client_add("dc/bl")
        orig_tree = tree.basis_tree()
        tree.commit(message=u"data")

    def test_commit_nested(self):
        repos_url = self.make_client('a', 'dc')
        self.build_tree({"dc/branches/foobranch/file": b"data"})
        self.client_add("dc/branches")
        self.client_commit("dc", "initial changes")
        self.make_checkout(repos_url + "/branches/foobranch", "de")
        tree = WorkingTree.open("de")
        self.build_tree({'de/file': b"foo"})
        tree.basis_tree()
        tree.commit(message="data")

    def test_set_root_id_commit(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.assertIsNot(None, tree.path2id(""))
        self.build_tree({"dc/foo": b"data"})
        self.client_add("dc/foo")
        tree.commit("set root")

    def test_update_after_commit(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')  # 1
        self.build_tree({"dc/bl": b"data"})
        self.client_add("dc/bl")
        orig_tree = tree.basis_tree()
        self.assertTrue(tree.changes_from(tree.basis_tree()).has_changed())
        tree.commit(message="data")  # 2
        self.assertCleanTree(tree)
        self.assertEqual(
                tree.branch.generate_revision_id(2),
                tree.basis_tree().get_revision_id())
        rev0tree = tree.branch.repository.revision_tree(
            _mod_revision.NULL_REVISION)
        delta = tree.basis_tree().changes_from(rev0tree)
        self.assertTrue(delta.has_changed(), repr(delta))
        tree = WorkingTree.open("dc")
        self.assertEqual(
             tree.branch.generate_revision_id(2),
             tree.basis_tree().get_revision_id())
        self.assertCleanTree(tree)

    def test_status(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.assertTrue(os.path.exists(os.path.join("dc", ".svn")))
        self.assertFalse(os.path.exists(os.path.join("dc", ".bzr")))

    def test_update(self):
        tree = self.make_svn_branch_and_tree('a', 'de')  # 1
        self.make_checkout(tree.branch.base, "dc")  # 1
        self.build_tree({'dc/bla': b"data"})
        self.client_add("dc/bla")
        self.client_commit("dc", "msg")  # 2
        tree.update()
        self.assertTrue(os.path.exists(os.path.join("de", ".svn")))
        self.assertTrue(os.path.exists(os.path.join("de", "bla")))

    def test_update_revision(self):
        tree = self.make_svn_branch_and_tree('a', 'de')  # 1
        self.make_checkout(tree.branch.base, "dc")
        self.build_tree({'dc/bla': b"data"})
        self.client_add("dc/bla")
        self.client_commit("dc", "msg")  # 2
        self.build_tree({'dc/bla': b"data2"})
        self.client_commit("dc", "msg2")  # 3
        tree.update(revision=tree.branch.generate_revision_id(2))
        self.assertTrue(os.path.exists(os.path.join("de", ".svn")))
        self.assertFileEqual("data", "de/bla")

    def test_update_old_tip(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')  # 1
        self.make_checkout(tree.branch.base, "de")
        self.build_tree({'dc/bla': b"data"})
        self.client_add("dc/bla")
        self.client_commit("dc", "msg")  # 2
        self.build_tree({'dc/bla': b"data2"})
        self.client_commit("dc", "msg2")  # 3
        tree = WorkingTree.open("de")
        tree.update(revision=tree.branch.generate_revision_id(2),
                    old_tip=tree.branch.generate_revision_id(1))
        self.assertTrue(os.path.exists(os.path.join("de", ".svn")))
        self.assertFileEqual("data", "de/bla")

    def test_status_controldir(self):
        self.make_svn_branch_and_tree('a', 'dc')
        controldir = ControlDir.open("dc")
        self.assertTrue(os.path.exists(os.path.join("dc", ".svn")))
        self.assertTrue(not os.path.exists(os.path.join("dc", ".bzr")))
        controldir.open_workingtree()

    def test_file_id_consistent(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({'dc/file': 'data'})
        tree.add(["file"])
        oldid = tree.path2id("file")
        tree = WorkingTree.open("dc")
        newid = tree.path2id("file")
        self.assertEqual(oldid, newid)

    def test_file_id_kept(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({'dc/file': 'data'})
        tree.add(["file"], ["fooid"])
        self.assertEqual("fooid", tree.path2id("file"))
        tree = WorkingTree.open("dc")
        self.assertEqual("fooid", tree.path2id("file"))

    def test_file_rename_id(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({'dc/file': 'data'})
        tree.add(["file"], ["fooid"])
        tree.commit("msg")
        tree.rename_one("file", "file2")
        delta = tree.branch.repository.get_revision_delta(tree.last_revision())
        self.assertEquals([("file", "fooid", "file")], delta.added)
        self.assertEqual(None, tree.path2id("file"))
        self.assertEqual("fooid", tree.path2id("file2"))
        tree = WorkingTree.open("dc")
        self.assertEqual("fooid", tree.path2id("file2"))

    def test_file_id_kept_2(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({'dc/file': 'data', 'dc/other': 'blaid'})
        tree.add(["file", "other"], ["fooid", "blaid"])
        self.assertEqual("fooid", tree.path2id("file"))
        self.assertEqual("blaid", tree.path2id("other"))

    def test_file_remove_id(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({'dc/file': 'data'})
        tree.add(["file"], ["fooid"])
        tree.commit("msg")
        tree.remove(["file"])
        self.assertEqual(None, tree.path2id("file"))
        tree = WorkingTree.open("dc")
        self.assertEqual(None, tree.path2id("file"))

    def test_file_move_id(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({'dc/file': 'data', 'dc/dir': None})
        tree.add(["file", "dir"], ["fooid", "blaid"])
        tree.commit("msg")
        self.assertEqual("fooid", tree.path2id("file"))
        tree.move(["file"], "dir")
        self.assertEqual(None, tree.path2id("file"))
        self.assertEqual("fooid", tree.path2id("dir/file"))
        tree = WorkingTree.open("dc")
        self.assertEqual(None, tree.path2id("file"))
        self.assertEqual("fooid", tree.path2id("dir/file"))

    def test_escaped_char_filename(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({'dc/file with spaces': 'data'})
        tree.add(["file with spaces"], ["fooid"])
        tree.commit("msg")
        self.assertEqual("fooid", tree.path2id("file with spaces"))

    def test_get_branch_nick(self):
        tree = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({'dc/some strange file': 'data'})
        tree.add(["some strange file"])
        tree.commit("message")
        self.assertEqual("trunk", tree.branch.nick)

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
        self.requireFeature(SymlinkFeature)
        self.requireFeature(UnicodeFilenameFeature)
        wt = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({u"dc/\U00020001".encode(osutils._fs_enc): b""})
        os.symlink(u"\U00020001", "dc/a")
        self.build_tree({"dc/b": b""})
        os.symlink("b", u"dc/\U00020002")
        self.client_add("dc/a")
        self.client_add("dc/b")
        self.client_add(u"dc/\U00020001".encode("utf-8"))
        self.client_add(u"dc/\U00020002".encode("utf-8"))
        self.client_commit("dc", "Added files and links")
        list(wt.iter_entries_by_dir())

    def test_get_dir_properties(self):
        wt = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bla": None})
        self.client_add("dc/bla")
        self.client_set_prop("dc/bla", "bzrbla", "bloe")
        props = wt.get_file_properties('bla')
        self.assertEquals("bloe", props["bzrbla"])

    def test_get_file_properties(self):
        wt = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bla": b"data"})
        self.client_add("dc/bla")
        self.client_set_prop("dc/bla", "bzrbla", "bloe")
        props = wt.get_file_properties('bla')
        self.assertEquals("bloe", props["bzrbla"])

    def test_invdelta_remove(self):
        wt = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({"dc/bla": b"data"})
        self.client_add("dc/bla")
        wt.apply_inventory_delta([("bla", None, wt.path2id("bla"), None)])

    def test_keywords_commit(self):
        wt = self.make_svn_branch_and_tree('a', 'dc')
        # md5sum: a31b9ad28573e11e3e653a0c038c49b5
        self.build_tree({'dc/file.txt': 'This is a file with a $Id$'})
        self.client_add('dc/file.txt')
        self.client_set_prop('dc/file.txt', 'svn:keywords', 'Id')
        self.client_commit('dc', "Initial commit")
        self.assertCleanTree(wt)
        # md5sum: 2936900e291f6c12d1c2ffc7b83f4da1
        self.build_tree({'dc/file.txt': 'This is a file with a $Id$\n'
                                        'New line added\n'})
        wt.commit("Commit via bzr")
        self.assertCleanTree(wt)
        self.build_tree({'dc/file.txt': 'This is a file with a $Id$\n'
                                        'New line added\n'
                                        'Another change\n'})
        self.client_commit('dc', "Commit via svn")

    def test_revision_tree(self):
        wt = self.make_svn_branch_and_tree('a', 'dc')
        # md5sum: a31b9ad28573e11e3e653a0c038c49b5
        self.build_tree({'dc/file.txt': 'Foo'})
        self.client_add('dc/file.txt')
        revid = wt.commit("Initial commit")
        self.assertEquals(revid, wt.revision_tree(revid).get_revision_id())

    def test_kind_change_file_to_dir(self):
        wt = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({'dc/thing': 'Foo'})
        self.client_add('dc/thing')
        self.assertEquals('file', wt.kind('thing'))
        os.unlink('dc/thing')
        self.build_tree({'dc/thing': None})
        self.assertEquals('directory', wt.kind('thing'))
        entries = list(wt.iter_entries_by_dir())
        self.assertEquals(
            [("", "directory"), ("thing", "directory")],
            [(p, e.kind) for (p, e) in entries])

    def test_kind_change_dir_to_file(self):
        wt = self.make_svn_branch_and_tree('a', 'dc')
        self.build_tree({'dc/thing': None})
        self.client_add('dc/thing')
        self.assertEquals('directory', wt.kind('thing'))
        shutil.rmtree('dc/thing')
        self.build_tree({'dc/thing': 'Foo'})
        self.assertEquals('file', wt.kind('thing'))
        entries = list(wt.iter_entries_by_dir())
        self.assertEquals(
            [("", "directory"), ("thing", "directory")],
            [(p, e.kind) for (p, e) in entries])
