# -*- coding: utf-8 -*-

# Copyright (C) 2006-2007 Jelmer Vernooij <jelmer@samba.org>

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

"""Commit and push tests."""

from bzrlib.branch import Branch, PullResult
from bzrlib.bzrdir import BzrDir
from bzrlib.errors import DivergedBranches, BzrError
from bzrlib.repository import Repository
from bzrlib.tests import TestCase
from bzrlib.trace import mutter
from bzrlib.workingtree import WorkingTree

from copy import copy
import os

from subvertpy import ra
from bzrlib.plugins.svn.commit import set_svn_revprops, _revision_id_to_svk_feature
from bzrlib.plugins.svn.errors import RevpropChangeFailed
from subvertpy.properties import time_to_cstring
from bzrlib.plugins.svn.transport import SvnRaTransport
from bzrlib.plugins.svn.tests import SubversionTestCase

class TestNativeCommit(SubversionTestCase):
    def test_simple_commit(self):
        self.make_client('d', 'dc')
        self.build_tree({'dc/foo/bla': "data"})
        self.client_add("dc/foo")
        wt = WorkingTree.open("dc")
        revid = wt.commit(message="data")
        self.assertEqual(wt.branch.generate_revision_id(1), revid)
        self.client_update("dc")
        self.assertEqual(wt.branch.generate_revision_id(1), 
                wt.branch.last_revision())
        wt = WorkingTree.open("dc")
        new_inventory = wt.branch.repository.get_inventory(
                            wt.branch.last_revision())
        self.assertTrue(new_inventory.has_filename("foo"))
        self.assertTrue(new_inventory.has_filename("foo/bla"))

    def test_commit_message(self):
        self.make_client('d', 'dc')
        self.build_tree({'dc/foo/bla': "data"})
        self.client_add("dc/foo")
        wt = WorkingTree.open("dc")
        revid = wt.commit(message="data")
        self.assertEqual(wt.branch.generate_revision_id(1), revid)
        self.assertEqual(
                wt.branch.generate_revision_id(1), wt.branch.last_revision())
        new_revision = wt.branch.repository.get_revision(
                            wt.branch.last_revision())
        self.assertEqual(wt.branch.last_revision(), new_revision.revision_id)
        self.assertEqual("data", new_revision.message)

    def test_commit_rev_id(self):
        self.make_client('d', 'dc')
        self.build_tree({'dc/foo/bla': "data"})
        self.client_add("dc/foo")
        wt = WorkingTree.open("dc")
        revid = wt.commit(message="data", rev_id="some-revid-bla")
        self.assertEqual("some-revid-bla", revid)
        self.assertEqual(wt.branch.generate_revision_id(1), revid)
        self.assertEqual(
                wt.branch.generate_revision_id(1), wt.branch.last_revision())
        new_revision = wt.branch.repository.get_revision(
                            wt.branch.last_revision())
        self.assertEqual(wt.branch.last_revision(), new_revision.revision_id)

    def test_commit_unicode_filename(self):
        self.make_client('d', 'dc')
        self.build_tree({u'dc/I²C': "data"})
        self.client_add(u"dc/I²C".encode("utf-8"))
        wt = WorkingTree.open("dc")
        wt.commit(message="data")


    def test_commit_local(self):
        self.make_client('d', 'dc')
        self.build_tree({'dc/foo/bla': "data"})
        self.client_add("dc/foo")
        wt = WorkingTree.open("dc")
        self.assertRaises(BzrError, wt.commit, 
                message="data", local=True)

    def test_commit_committer(self):
        self.make_client('d', 'dc')
        self.build_tree({'dc/foo/bla': "data"})
        self.client_add("dc/foo")
        wt = WorkingTree.open("dc")
        revid = wt.commit(message="data", committer="john doe")
        rev = wt.branch.repository.get_revision(revid)
        self.assertEquals("john doe", rev.committer)

    def test_commit_message_nordic(self):
        self.make_client('d', 'dc')
        self.build_tree({'dc/foo/bla': "data"})
        self.client_add("dc/foo")
        wt = WorkingTree.open("dc")
        revid = wt.commit(message=u"føø")
        self.assertEqual(revid, wt.branch.generate_revision_id(1))
        self.assertEqual(
                wt.branch.generate_revision_id(1), wt.branch.last_revision())
        new_revision = wt.branch.repository.get_revision(
                            wt.branch.last_revision())
        self.assertEqual(wt.branch.last_revision(), new_revision.revision_id)
        self.assertEqual(u"føø", new_revision.message)

    def test_commit_update(self):
        self.make_client('d', 'dc')
        self.build_tree({'dc/foo/bla': "data"})
        self.client_add("dc/foo")
        wt = WorkingTree.open("dc")
        wt.set_pending_merges(["some-ghost-revision"])
        wt.commit(message="data")
        self.assertEqual(
                wt.branch.generate_revision_id(1),
                wt.branch.last_revision())

    def test_commit_rename_file(self):
        repos_url = self.make_client('d', 'dc')
        self.build_tree({'dc/foo': "data"})
        self.client_add("dc/foo")
        wt = WorkingTree.open("dc")
        wt.set_pending_merges(["some-ghost-revision"])
        oldid = wt.path2id("foo")
        wt.commit(message="data") # 1
        wt.rename_one("foo", "bar")
        wt.commit(message="doe") # 2
        paths = self.client_log(repos_url, 2, 0)[2][0]
        self.assertEquals('D', paths["/foo"][0])
        self.assertEquals('A', paths["/bar"][0])
        self.assertEquals('/foo', paths["/bar"][1])
        self.assertEquals(1, paths["/bar"][2])
        svnrepo = Repository.open(repos_url)
        self.assertEquals({u"bar": "oldid"}, 
                svnrepo._revmeta_provider.get_revision("", 2).get_fileid_map(svnrepo.get_mapping()))

    def test_commit_rename_file_from_directory(self):
        repos_url = self.make_client('d', 'dc')
        self.build_tree({'dc/adir/foo': "data"})
        self.client_add("dc/adir")
        wt = WorkingTree.open("dc")
        wt.commit(message="data")
        wt.rename_one("adir/foo", "bar")
        self.assertFalse(wt.has_filename("adir/foo"))
        self.assertTrue(wt.has_filename("bar"))
        wt.commit(message="doe")
        paths = self.client_log(repos_url, 2, 0)[2][0]
        self.assertEquals('D', paths["/adir/foo"][0])
        self.assertEquals('A', paths["/bar"][0])
        self.assertEquals('/adir/foo', paths["/bar"][1])
        self.assertEquals(1, paths["/bar"][2])

    def test_commit_sets_mergeinfo(self):
        repos_url = self.make_repository('d')

        dc = self.get_commit_editor(repos_url)
        foo = dc.add_dir("trunk")
        foo.add_file("trunk/bla").modify("bla")
        dc.add_dir("branches")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        tags = dc.add_dir("tags")
        foobranch = tags.add_dir("tags/foo")
        foobranch.add_file("tags/foo/afile").modify()
        dc.close()

        dc = self.get_commit_editor(repos_url)
        tags = dc.open_dir("tags")
        foobranch = tags.open_dir("tags/foo")
        foobranch.add_file("tags/foo/bfile").modify()
        dc.close()

        branch = Branch.open(repos_url+"/trunk")
        foobranch = Branch.open(repos_url+"/tags/foo")
        builder = branch.get_commit_builder([branch.last_revision(), foobranch.last_revision()], 
                revision_id="my-revision-id")
        tree = branch.repository.revision_tree(branch.last_revision())
        new_tree = copy(tree)
        ie = new_tree.inventory.root
        ie.revision = None
        builder.record_entry_contents(ie, [tree.inventory], '', new_tree, 
                                      None)
        builder.finish_inventory()
        builder.commit("foo")

        self.assertEqual("/tags/foo:2-3\n",
            self.client_get_prop("%s/trunk" % repos_url, 
                "svn:mergeinfo", 4))

        try:
            c = ra.RemoteAccess(repos_url)
            mi = c.mergeinfo(["trunk"], 4)
            self.assertEquals({"trunk": {"/tags/foo": [(1, 3, 1)]}}, mi)
        except NotImplementedError:
            pass # Svn 1.4

    def test_mwh(self):
        repo = self.make_client('d', 'sc')
        def mv(*mvs):
            for a, b in mvs:
                self.client_copy(a, b)
                self.client_delete(a)
            self.client_commit('sc', '.')
            self.client_update('sc')
        self.build_tree({'sc/de/foo':'data', 'sc/de/bar':'DATA'})
        self.client_add('sc/de')
        self.client_commit('sc', 'blah') #1
        self.client_update('sc')
        os.mkdir('sc/de/trunk')
        self.client_add('sc/de/trunk')
        mv(('sc/de/foo', 'sc/de/trunk'), ('sc/de/bar', 'sc/de/trunk')) #2
        mv(('sc/de', 'sc/pyd'))  #3
        self.client_delete('sc/pyd/trunk/foo')
        self.client_commit('sc', '.') #4
        self.client_update('sc')

        self.make_checkout(repo + '/pyd/trunk', 'pyd')
        self.assertEqual("DATA", open('pyd/bar').read())

        olddir = BzrDir.open("pyd")
        os.mkdir('bc')
        newdir = olddir.sprout("bc")
        newdir.open_branch().pull(olddir.open_branch())
        wt = newdir.open_workingtree()
        self.assertEqual("DATA", open('bc/bar').read())
        open('bc/bar', 'w').write('data')
        wt.commit(message="Commit from Bzr")
        olddir.open_branch().pull(newdir.open_branch())

        self.client_update('pyd')
        self.assertEqual("data", open('pyd/bar').read())
        

class TestPush(SubversionTestCase):
    def setUp(self):
        super(TestPush, self).setUp()
        self.repos_url = self.make_client('d', 'sc')

        self.build_tree({'sc/foo/bla': "data"})
        self.client_add("sc/foo")
        self.client_commit("sc", "foo")

        self.olddir = BzrDir.open("sc")
        os.mkdir("dc")
        self.newdir = self.olddir.sprout("dc")

    def test_empty(self):
        self.assertEqual(0, int(self.olddir.open_branch().pull(
                                self.newdir.open_branch())))

    def test_empty_result(self):
        result = self.olddir.open_branch().pull(self.newdir.open_branch())
        self.assertIsInstance(result, PullResult)
        self.assertEqual(result.old_revno, self.olddir.open_branch().revno())
        self.assertEqual(result.master_branch, None)
        self.assertEqual(result.source_branch.bzrdir.root_transport.base, 
                         self.newdir.root_transport.base)

    def test_child(self):
        self.build_tree({'sc/foo/bar': "data"})
        self.client_add("sc/foo/bar")
        self.client_commit("sc", "second message")

        self.assertEqual(0, int(self.olddir.open_branch().pull(
                                self.newdir.open_branch())))

    def test_diverged(self):
        self.build_tree({'sc/foo/bar': "data"})
        self.client_add("sc/foo/bar")
        self.client_commit("sc", "second message")

        olddir = BzrDir.open("sc")

        self.build_tree({'dc/file': 'data'})
        wt = self.newdir.open_workingtree()
        wt.add('file')
        wt.commit(message="Commit from Bzr")

        self.assertRaises(DivergedBranches, 
                          olddir.open_branch().pull,
                          self.newdir.open_branch())

    def test_unicode_filename(self):
        self.build_tree({u'dc/I²C': 'other data'})
        wt = self.newdir.open_workingtree()
        wt.add(u'I²C')
        wt.commit(message="Commit from Bzr")
        self.assertEqual(1, int(self.olddir.open_branch().pull(
                                self.newdir.open_branch())))

    def test_change(self):
        self.build_tree({'dc/foo/bla': 'other data'})
        wt = self.newdir.open_workingtree()
        wt.commit(message="Commit from Bzr") # Commit 2

        self.olddir.open_branch().pull(self.newdir.open_branch())

        repos = self.olddir._find_repository()
        mapping = repos.get_mapping()
        inv = repos.get_inventory(repos.generate_revision_id(2, "", mapping))
        self.assertEqual(repos.generate_revision_id(2, "", mapping),
                         inv[inv.path2id('foo/bla')].revision)
        self.assertEqual(wt.branch.last_revision(),
          repos.generate_revision_id(2, "", mapping))
        self.assertEqual(wt.branch.last_revision(),
                        self.olddir.open_branch().last_revision())
        self.assertEqual("other data", 
            repos.revision_tree(repos.generate_revision_id(2, "", mapping)).get_file_text( inv.path2id("foo/bla")))

    def test_simple(self):
        self.build_tree({'dc/file': 'data'})
        wt = self.newdir.open_workingtree()
        wt.add('file')
        wt.commit(message="Commit from Bzr")

        self.olddir.open_branch().pull(self.newdir.open_branch())

        repos = self.olddir._find_repository()
        mapping = repos.get_mapping()
        inv = repos.get_inventory(repos.generate_revision_id(2, "", mapping))
        self.assertTrue(inv.has_filename('file'))
        self.assertEqual(wt.branch.last_revision(), 
                repos.generate_revision_id(2, "", mapping))
        self.assertEqual(repos.generate_revision_id(2, "", mapping),
                        self.olddir.open_branch().last_revision())

    def test_pull_after_push(self):
        self.build_tree({'dc/file': 'data'})
        wt = self.newdir.open_workingtree()
        wt.add('file')
        wt.commit(message="Commit from Bzr")

        self.olddir.open_branch().pull(self.newdir.open_branch())

        repos = self.olddir._find_repository()
        mapping = repos.get_mapping()
        inv = repos.get_inventory(repos.generate_revision_id(2, "", mapping))
        self.assertTrue(inv.has_filename('file'))
        self.assertEquals(wt.branch.last_revision(), 
                         repos.generate_revision_id(2, "", mapping))

        self.assertEqual(repos.generate_revision_id(2, "", mapping),
                        self.olddir.open_branch().last_revision())

        self.newdir.open_branch().pull(self.olddir.open_branch())

        self.assertEqual(repos.generate_revision_id(2, "", mapping),
                        self.newdir.open_branch().last_revision())

    def test_message(self):
        self.build_tree({'dc/file': 'data'})
        wt = self.newdir.open_workingtree()
        wt.add('file')
        wt.commit(message="Commit from Bzr")

        self.olddir.open_branch().pull(self.newdir.open_branch())

        repos = self.olddir._find_repository()
        mapping = repos.get_mapping()
        self.assertEqual("Commit from Bzr",
            repos.get_revision(
                repos.generate_revision_id(2, "", mapping)).message)

    def test_message_nordic(self):
        self.build_tree({'dc/file': 'data'})
        wt = self.newdir.open_workingtree()
        wt.add('file')
        wt.commit(message=u"\xe6\xf8\xe5")

        self.olddir.open_branch().pull(self.newdir.open_branch())

        repos = self.olddir._find_repository()
        mapping = repos.get_mapping()
        self.assertEqual(u"\xe6\xf8\xe5", repos.get_revision(
            repos.generate_revision_id(2, "", mapping)).message)

    def test_commit_rename_file(self):
        self.build_tree({'dc/vla': "data"})
        wt = self.newdir.open_workingtree()
        wt.add("vla")
        wt.set_pending_merges(["some-ghost-revision"])
        wt.commit(message="data")
        wt.rename_one("vla", "bar")
        wt.commit(message="doe")
        self.olddir.open_branch().pull(self.newdir.open_branch())
        paths = self.client_log(self.repos_url, 3, 0)[3][0]
        self.assertEquals('D', paths["/vla"][0])
        self.assertEquals('A', paths["/bar"][0])
        self.assertEquals('/vla', paths["/bar"][1])
        self.assertEquals(2, paths["/bar"][2])

    def test_commit_rename_file_from_directory(self):
        wt = self.newdir.open_workingtree()
        self.build_tree({'dc/adir/foo': "data"})
        wt.add("adir")
        wt.add("adir/foo")
        wt.commit(message="data")
        wt.rename_one("adir/foo", "bar")
        self.assertTrue(wt.has_filename("bar"))
        self.assertFalse(wt.has_filename("adir/foo"))
        wt.commit(message="doe")
        self.olddir.open_branch().pull(self.newdir.open_branch())
        paths = self.client_log(self.repos_url, 3, 0)[3][0]
        mutter('paths %r' % paths)
        self.assertEquals('D', paths["/adir/foo"][0])
        self.assertEquals('A', paths["/bar"][0])
        self.assertEquals('/adir/foo', paths["/bar"][1])
        self.assertEquals(2, paths["/bar"][2])

    def test_commit_remove(self):
        wt = self.newdir.open_workingtree()
        self.build_tree({'dc/foob': "data"})
        wt.add("foob")
        wt.commit(message="data")
        wt.remove(["foob"])
        wt.commit(message="doe")
        self.olddir.open_branch().pull(self.newdir.open_branch())
        paths = self.client_log(self.repos_url, 3, 0)[3][0]
        mutter('paths %r' % paths)
        self.assertEquals('D', paths["/foob"][0])

    def test_commit_rename_remove_parent(self):
        wt = self.newdir.open_workingtree()
        self.build_tree({'dc/adir/foob': "data"})
        wt.add("adir")
        wt.add("adir/foob")
        wt.commit(message="data")
        wt.rename_one("adir/foob", "bar")
        wt.remove(["adir"])
        wt.commit(message="doe")
        self.olddir.open_branch().pull(self.newdir.open_branch())
        paths = self.client_log(self.repos_url, 3, 0)[3][0]
        mutter('paths %r' % paths)
        self.assertEquals('D', paths["/adir"][0])
        self.assertEquals('A', paths["/bar"][0])
        self.assertEquals('/adir/foob', paths["/bar"][1])
        self.assertEquals(2, paths["/bar"][2])

    def test_commit_remove_nested(self):
        wt = self.newdir.open_workingtree()
        self.build_tree({'dc/adir/foob': "data"})
        wt.add("adir")
        wt.add("adir/foob")
        wt.commit(message="data")
        wt.remove(["adir/foob"])
        wt.commit(message="doe")
        self.olddir.open_branch().pull(self.newdir.open_branch())
        paths = self.client_log(self.repos_url, 3, 0)[3][0]
        mutter('paths %r' % paths)
        self.assertEquals('D', paths["/adir/foob"][0])


class TestPushNested(SubversionTestCase):
    def setUp(self):
        super(TestPushNested, self).setUp()
        self.repos_url = self.make_client('d', 'sc')

        self.build_tree({'sc/foo/trunk/bla': "data"})
        self.client_add("sc/foo")
        self.client_commit("sc", "foo")

        self.olddir = BzrDir.open("sc/foo/trunk")
        os.mkdir("dc")
        self.newdir = self.olddir.sprout("dc")

    def test_simple(self):
        self.build_tree({'dc/file': 'data'})
        wt = self.newdir.open_workingtree()
        wt.add('file')
        wt.commit(message="Commit from Bzr")
        self.olddir.open_branch().pull(self.newdir.open_branch())
        repos = self.olddir._find_repository()
        self.client_update("sc")
        self.assertTrue(os.path.exists("sc/foo/trunk/file"))
        self.assertFalse(os.path.exists("sc/foo/trunk/filel"))


class HeavyWeightCheckoutTests(SubversionTestCase):
    def test_bind(self):
        repos_url = self.make_repository("d")
        master_branch = Branch.open(repos_url)
        os.mkdir("b")
        local_dir = master_branch.bzrdir.sprout("b")
        wt = local_dir.open_workingtree()
        local_dir.open_branch().bind(master_branch)
        local_dir.open_branch().unbind()

    def test_commit(self):
        repos_url = self.make_repository("d")
        master_branch = Branch.open(repos_url)
        os.mkdir("b")
        local_dir = master_branch.bzrdir.sprout("b")
        wt = local_dir.open_workingtree()
        local_dir.open_branch().bind(master_branch)
        self.build_tree({'b/file': 'data'})
        wt.add('file')
        revid = wt.commit(message="Commit from Bzr")
        master_branch = Branch.open(repos_url)
        self.assertEquals(revid, master_branch.last_revision())

    def test_fileid(self):
        repos_url = self.make_repository("d")
        master_branch = Branch.open(repos_url)
        os.mkdir("b")
        local_dir = master_branch.bzrdir.sprout("b")
        wt = local_dir.open_workingtree()
        local_dir.open_branch().bind(master_branch)
        self.build_tree({'b/file': 'data'})
        wt.add('file')
        oldid = wt.path2id("file")
        revid1 = wt.commit(message="Commit from Bzr")
        wt.rename_one('file', 'file2')
        revid2 = wt.commit(message="Commit from Bzr")
        master_branch = Branch.open(repos_url)
        rm_provider = master_branch.repository._revmeta_provider
        mapping = master_branch.repository.get_mapping()
        self.assertEquals({u"file": oldid}, 
                rm_provider.get_revision("", 1).get_fileid_map(mapping))
        self.assertEquals({u"file2": oldid}, 
                rm_provider.get_revision("", 2).get_fileid_map(mapping))
        tree1 = master_branch.repository.revision_tree(revid1)
        tree2 = master_branch.repository.revision_tree(revid2)
        delta = tree2.changes_from(tree1)
        mutter("changes %r" % list(rm_provider.iter_reverse_branch_changes("", 2, 0, master_branch.repository.get_mapping())))
        self.assertEquals(0, len(delta.added))
        self.assertEquals(0, len(delta.removed))
        self.assertEquals(1, len(delta.renamed))

    def test_nested_fileid(self):
        repos_url = self.make_repository("d")
        master_branch = Branch.open(repos_url)
        os.mkdir("b")
        local_dir = master_branch.bzrdir.sprout("b")
        wt = local_dir.open_workingtree()
        local_dir.open_branch().bind(master_branch)
        self.build_tree({'b/dir/file': 'data'})
        wt.add('dir')
        wt.add('dir/file')
        dirid = wt.path2id("dir")
        fileid = wt.path2id("dir/file")
        revid1 = wt.commit(message="Commit from Bzr")
        master_branch = Branch.open(repos_url)
        rm_provider = master_branch.repository._revmeta_provider
        mapping = master_branch.repository.get_mapping()
        self.assertEquals({"dir": dirid, 
                          "dir/file": fileid},
                          rm_provider.get_revision("", 1).get_fileid_map(mapping))


class RevpropTests(SubversionTestCase):
    def test_change_revprops(self):
        repos_url = self.make_repository("d", allow_revprop_changes=True)

        dc = self.get_commit_editor(repos_url, message="My commit")
        dc.add_file("foo.txt").modify()
        dc.close()

        transport = SvnRaTransport(repos_url)
        set_svn_revprops(transport, 1, {"svn:author": "Somebody", 
                                        "svn:date": time_to_cstring(1000000*473385600)})

        self.assertEquals(1, transport.get_latest_revnum())

        self.assertEquals(("Somebody", "1985-01-01T00:00:00.000000Z", "My commit"), 
                          self.client_log(repos_url, 1, 1)[1][1:])

    def test_change_revprops_disallowed(self):
        repos_url = self.make_repository("d", allow_revprop_changes=False)

        dc = self.get_commit_editor(repos_url)
        dc.add_file("foo.txt").modify()
        dc.close()

        transport = SvnRaTransport(repos_url)
        self.assertRaises(RevpropChangeFailed, 
                lambda: set_svn_revprops(transport, 1, {"svn:author": "Somebody", "svn:date": time_to_cstring(1000000*473385600)}))


class SvkTestCase(TestCase):
    def test_revid_svk_map(self):
        self.assertEqual("auuid:/:6", 
              _revision_id_to_svk_feature("svn-v3-undefined:auuid::6"))


