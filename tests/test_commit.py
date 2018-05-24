# -*- coding: utf-8 -*-

# Copyright (C) 2006-2009 Jelmer Vernooij <jelmer@samba.org>

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

"""Commit and push tests."""

import os
from subvertpy import (
    ERR_FS_TXN_OUT_OF_DATE,
    SubversionException,
    ra,
    )
from subvertpy.properties import time_to_cstring

from breezy.branch import (
    Branch,
    PullResult,
    )
from breezy.controldir import ControlDir
from breezy.errors import (
    BzrError,
    DivergedBranches,
    NoSuchId,
    )
from breezy.bzr.inventory import (
    InventoryFile,
    InventoryLink,
    )
from breezy.repository import Repository
from breezy.tests import (
    TestCase,
    TestSkipped,
    )
from breezy.tests.features import (
    UnicodeFilenameFeature
    )
from breezy.trace import mutter
from breezy.workingtree import WorkingTree

from breezy.plugins.svn.commit import (
    file_editor_send_prop_changes,
    set_svn_revprops,
    _revision_id_to_svk_feature,
    )
from breezy.plugins.svn.errors import (
    RevpropChangeFailed,
    )
from breezy.plugins.svn.mapping import mapping_registry
from breezy.plugins.svn.tests import SubversionTestCase


class TestNativeCommit(SubversionTestCase):

    def test_simple_commit(self):
        wt = self.make_svn_branch_and_tree('d', 'dc') #1
        self.build_tree({'dc/foo/bla': "data"})
        self.client_add("dc/foo")
        revid = wt.commit(message="data")
        self.assertEqual(wt.branch.generate_revision_id(2), revid)
        self.client_update("dc")
        self.assertEqual(wt.branch.generate_revision_id(2),
                wt.branch.last_revision())
        wt = WorkingTree.open("dc")
        new_tree = wt.branch.repository.revision_tree(
                            wt.branch.last_revision())
        self.assertTrue(new_tree.has_filename("foo"))
        self.assertTrue(new_tree.has_filename("foo/bla"))

    def test_commit_message(self):
        wt = self.make_svn_branch_and_tree('d', 'dc') #1
        self.build_tree({'dc/foo/bla': "data"})
        self.client_add("dc/foo")
        revid = wt.commit(message="data") #2
        self.assertEqual(wt.branch.generate_revision_id(2), revid)
        self.assertEqual(
                wt.branch.generate_revision_id(2), wt.branch.last_revision())
        new_revision = wt.branch.repository.get_revision(
                            wt.branch.last_revision())
        self.assertEqual(wt.branch.last_revision(), new_revision.revision_id)
        self.assertEqual("data", new_revision.message)

    def test_commit_rev_id(self):
        wt = self.make_svn_branch_and_tree('d', 'dc') #1
        self.build_tree({'dc/foo/bla': "data"})
        self.client_add("dc/foo")
        revid = wt.commit(message="data", rev_id="some-revid-bla") #2
        self.assertEqual("some-revid-bla", revid)
        self.assertEqual(wt.branch.generate_revision_id(2), revid)
        self.assertEqual(
                wt.branch.generate_revision_id(2), wt.branch.last_revision())
        new_revision = wt.branch.repository.get_revision(
                            wt.branch.last_revision())
        self.assertEqual(wt.branch.last_revision(), new_revision.revision_id)

    def test_commit_unicode_filename(self):
        self.requireFeature(UnicodeFilenameFeature)
        wt = self.make_svn_branch_and_tree('d', 'dc')
        self.build_tree({u'dc/I²C': "data"})
        self.client_add(u"dc/I²C".encode("utf-8"))
        wt.commit(message="data")

    def test_commit_local(self):
        wt = self.make_svn_branch_and_tree('d', 'dc')
        self.build_tree({'dc/foo/bla': "data"})
        self.client_add("dc/foo")
        self.assertRaises(BzrError, wt.commit,
                message="data", local=True)

    def test_commit_committer(self):
        wt = self.make_svn_branch_and_tree('d', 'dc')
        self.build_tree({'dc/foo/bla': "data"})
        self.client_add("dc/foo")
        revid = wt.commit(message="data", committer="john doe")
        rev = wt.branch.repository.get_revision(revid)
        self.assertEquals("john doe", rev.committer)

    def test_commit_message_nordic(self):
        wt = self.make_svn_branch_and_tree('d', 'dc') #1
        self.build_tree({'dc/foo/bla': "data"})
        self.client_add("dc/foo")
        revid = wt.commit(message=u"føø") #2
        self.assertEqual(revid, wt.branch.generate_revision_id(2))
        self.assertEqual(
                wt.branch.generate_revision_id(2), wt.branch.last_revision())
        new_revision = wt.branch.repository.get_revision(
                            wt.branch.last_revision())
        self.assertEqual(wt.branch.last_revision(), new_revision.revision_id)
        self.assertEqual(u"føø", new_revision.message)

    def test_commit_out_of_date(self):
        branch = self.make_svn_branch('d')

        dc = self.get_commit_editor(branch.base)
        somedir = dc.add_dir("somedir")
        somefile = somedir.add_file("somedir/somefile").modify()
        dc.close()

        dc = self.get_commit_editor(branch.repository.base)

        br = Branch.open(branch.base)
        br.lock_write()
        self.addCleanup(br.unlock)
        br.set_append_revisions_only(False)
        builder = br.get_commit_builder([br.last_revision()])
        builder.finish_inventory()

        trunk = dc.open_dir("trunk")
        somedir = trunk.open_dir("trunk/somedir")
        somefile = somedir.open_file("trunk/somedir/somefile").modify()
        dc.close()

        e = self.assertRaises(SubversionException, builder.commit, "foo")

        self.assertEquals(ERR_FS_TXN_OUT_OF_DATE, e.args[1])

    def test_lossy_commit_builder(self):
        branch = self.make_svn_branch('d', lossy=True) #1
        branch.lock_write()
        self.addCleanup(branch.unlock)
        cb = branch.repository.get_commit_builder(branch,
            [branch.last_revision()], branch.get_config_stack(),
            revision_id="foorevid", lossy=True)
        revid = cb.commit("msg")
        self.assertEquals(
            branch.repository.generate_revision_id(2, branch.get_branch_path(), branch.mapping),
            revid)

    def test_commit_update(self):
        self.make_svn_branch_and_tree('d', 'dc') #1
        self.build_tree({'dc/foo/bla': "data"})
        self.client_add("dc/foo")
        self.client_commit("dc", "msg") #2
        wt = WorkingTree.open("dc")
        self.build_tree({'dc/foo/bla': "data2"})
        wt.set_pending_merges(["some-ghost-revision"])
        wt.commit(message="data") #3
        self.assertEqual(
                wt.branch.generate_revision_id(3),
                wt.branch.last_revision())

    def test_commit_rename_file(self):
        wt = self.make_svn_branch_and_tree('d', 'dc') #1
        self.build_tree({'dc/.dummy': 'dummy'})
        self.client_add('dc/.dummy')
        self.client_commit("dc", "msg") #2
        self.build_tree({'dc/foo': "data"})
        self.client_add("dc/foo")
        wt.set_pending_merges(["some-ghost-revision"])
        oldid = wt.path2id("foo")
        wt.commit(message="data") # 3
        wt.rename_one("foo", "bar")
        self.assertEquals(oldid, wt.path2id("bar"))
        self.assertEquals(None, wt.path2id("foo"))
        wt.commit(message="doe") # 4
        paths = self.client_log(wt.branch.base, 4, 0)[4][0]
        self.assertEquals('D', paths["/trunk/foo"][0])
        self.assertEquals('A', paths["/trunk/bar"][0])
        self.assertEquals('/trunk/foo', paths["/trunk/bar"][1])
        self.assertEquals(3, paths["/trunk/bar"][2])
        svnrepo = Repository.open(wt.branch.repository.base)
        revmeta = svnrepo._revmeta_provider.get_revision(wt.branch.get_branch_path(), 4)
        self.assertEquals({u"bar": oldid},
                revmeta.get_fileid_overrides(svnrepo.get_mapping()))

    def test_commit_rename_file_from_directory(self):
        wt = self.make_svn_branch_and_tree('d', 'dc') #1
        self.build_tree({'dc/adir/foo': "data"})
        self.client_add("dc/adir")
        wt.commit(message="data") #2
        wt.rename_one("adir/foo", "bar")
        self.assertFalse(wt.has_filename("adir/foo"))
        self.assertTrue(wt.has_filename("bar"))
        wt.commit(message="doe") #3
        paths = self.client_log(wt.branch.base, 3, 0)[3][0]
        self.assertEquals('D', paths["/trunk/adir/foo"][0])
        self.assertEquals('A', paths["/trunk/bar"][0])
        self.assertEquals('/trunk/adir/foo', paths["/trunk/bar"][1])
        self.assertEquals(2, paths["/trunk/bar"][2])

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
        branch.lock_write()
        self.addCleanup(branch.unlock)
        builder = branch.get_commit_builder(
            [branch.last_revision(), foobranch.last_revision()],
            revision_id="my-revision-id")
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
            raise TestSkipped("mergeinfo not available with svn 1.4")

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
        mv(('sc/de/foo', 'sc/de/trunk/foo'), ('sc/de/bar', 'sc/de/trunk/bar')) #2
        mv(('sc/de', 'sc/pyd'))  #3
        self.client_delete('sc/pyd/trunk/foo')
        self.client_commit('sc', '.') #4
        self.client_update('sc')

        self.make_checkout(repo + '/pyd/trunk', 'pyd')
        self.assertEqual("DATA", open('pyd/bar').read())

        olddir = ControlDir.open("pyd")
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

    def test_parent_id_reused(self):
        wt = self.make_svn_branch_and_tree('d', 'dc')

        first_revid = wt.commit("msg", rev_id="firstrevid")
        second_revid = wt.commit("msg", rev_id="secondrevid")

        wt.branch.set_last_revision_info(1, first_revid)

        branch = wt.branch

        branch.lock_write()
        self.addCleanup(branch.unlock)
        cb = branch.get_commit_builder([branch.last_revision()],
            revision_id="foorevid")
        revid = cb.commit("msg")
        self.assertEquals("foorevid", revid)


class TestPush(SubversionTestCase):

    def setUp(self):
        super(TestPush, self).setUp()
        self.repos_url = self.make_client('d', 'sc')

        self.build_tree({'sc/foo/bla': "data"})
        self.client_add("sc/foo")
        self.client_commit("sc", "foo")

        self.olddir = ControlDir.open("sc")
        os.mkdir("dc")
        self.newdir = self.olddir.sprout("dc")

    def test_empty(self):
        result = self.olddir.open_branch().pull(self.newdir.open_branch())
        self.assertEquals(2, result.old_revno)
        self.assertEquals(2, result.new_revno)

    def test_empty_result(self):
        target_branch = self.olddir.open_branch()
        result = target_branch.pull(self.newdir.open_branch())
        self.assertIsInstance(result, PullResult)
        self.assertEqual(result.old_revno, self.olddir.open_branch().revno())
        self.assertEqual(result.master_branch, target_branch)
        self.assertEqual(result.source_branch.controldir.root_transport.base,
                         self.newdir.root_transport.base)

    def test_child(self):
        self.build_tree({'sc/foo/bar': "data"})
        self.client_add("sc/foo/bar")
        self.client_commit("sc", "second message")

        result = self.olddir.open_branch().pull(self.newdir.open_branch())
        self.assertEquals(3, result.old_revno)
        self.assertEquals(3, result.new_revno)

    def test_diverged(self):
        self.build_tree({'sc/foo/bar': "data"})
        self.client_add("sc/foo/bar")
        self.client_commit("sc", "second message")

        olddir = ControlDir.open("sc")

        self.build_tree({'dc/file': 'data'})
        wt = self.newdir.open_workingtree()
        wt.add('file')
        wt.commit(message="Commit from Bzr")

        self.assertRaises(DivergedBranches,
                          olddir.open_branch().pull,
                          self.newdir.open_branch())

    def test_unicode_filename(self):
        self.requireFeature(UnicodeFilenameFeature)
        self.build_tree({u'dc/I²C': 'other data'})
        wt = self.newdir.open_workingtree()
        wt.add(u'I²C')
        wt.commit(message="Commit from Bzr")
        result = self.olddir.open_branch().pull(self.newdir.open_branch())
        self.assertEqual(2, result.old_revno)
        self.assertEquals(3, result.new_revno)

    def test_add_file_to_unicode_folder(self):
        self.requireFeature(UnicodeFilenameFeature)
        self.build_tree({u'dc/I²C/': None})
        wt = self.newdir.open_workingtree()
        wt.add(u'I²C')
        wt.commit(message="Commit from Bzr")
        self.build_tree({u'dc/I²C/file': 'data'})
        wt.add(u'I²C/file')
        wt.commit(message="Commit from Bzr2")
        result = self.olddir.open_branch().pull(self.newdir.open_branch())
        self.assertEquals(2, result.old_revno)
        self.assertEquals(4, result.new_revno)
        owt = self.olddir.open_workingtree()
        owt.update()
        self.assertTrue(owt.has_filename(u'I²C/file'))

    def test_rename_from_unicode_filename(self):
        self.requireFeature(UnicodeFilenameFeature)
        self.build_tree({u'dc/I²C': 'other data'})
        wt = self.newdir.open_workingtree()
        wt.add(u'I²C')
        wt.commit(message="Commit from Bzr")
        wt.rename_one(u'I²C', u'I²C2')
        wt.commit(message="Commit from Bzr2")
        result = self.olddir.open_branch().pull(self.newdir.open_branch())
        self.assertEquals(2, result.old_revno)
        self.assertEquals(4, result.new_revno)

    def test_change(self):
        self.build_tree({'dc/foo/bla': 'other data'})
        wt = self.newdir.open_workingtree()
        wt.commit(message="Commit from Bzr") # Commit 2

        self.olddir.open_branch().pull(self.newdir.open_branch())

        repos = self.olddir._find_repository()
        mapping = repos.get_mapping()
        tree = repos.revision_tree(repos.generate_revision_id(2, u"", mapping))
        self.assertEqual(repos.generate_revision_id(2, u"", mapping),
                         tree.get_file_revision('foo/bla'))
        self.assertEqual(wt.branch.last_revision(),
          repos.generate_revision_id(2, u"", mapping))
        self.assertEqual(wt.branch.last_revision(),
                        self.olddir.open_branch().last_revision())
        self.assertEqual("other data",
            repos.revision_tree(repos.generate_revision_id(2, u"", mapping)).get_file_text("foo/bla"))

    def test_simple(self):
        self.build_tree({'dc/file': 'data'})
        wt = self.newdir.open_workingtree()
        wt.add('file')
        wt.commit(message="Commit from Bzr")

        self.olddir.open_branch().pull(self.newdir.open_branch())

        repos = self.olddir._find_repository()
        mapping = repos.get_mapping()
        tree = repos.revision_tree(repos.generate_revision_id(2, u"", mapping))
        self.assertTrue(tree.has_filename('file'))
        self.assertEqual(wt.branch.last_revision(),
                repos.generate_revision_id(2, u"", mapping))
        self.assertEqual(repos.generate_revision_id(2, u"", mapping),
                        self.olddir.open_branch().last_revision())

    def test_pull_after_push(self):
        self.build_tree({'dc/file': 'data'})
        wt = self.newdir.open_workingtree()
        wt.add('file')
        wt.commit(message="Commit from Bzr")

        self.olddir.open_branch().pull(self.newdir.open_branch())

        repos = self.olddir._find_repository()
        mapping = repos.get_mapping()
        tree = repos.revision_tree(repos.generate_revision_id(2, u"", mapping))
        self.assertTrue(tree.has_filename('file'))
        self.assertEquals(wt.branch.last_revision(),
                         repos.generate_revision_id(2, u"", mapping))

        self.assertEqual(repos.generate_revision_id(2, u"", mapping),
                        self.olddir.open_branch().last_revision())

        self.newdir.open_branch().pull(self.olddir.open_branch())

        self.assertEqual(repos.generate_revision_id(2, u"", mapping),
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
                repos.generate_revision_id(2, u"", mapping)).message)

    def test_message_nordic(self):
        self.build_tree({'dc/file': 'data'})
        wt = self.newdir.open_workingtree()
        wt.add('file')
        wt.commit(message=u"\xe6\xf8\xe5")

        self.olddir.open_branch().pull(self.newdir.open_branch())

        repos = self.olddir._find_repository()
        mapping = repos.get_mapping()
        self.assertEqual(u"\xe6\xf8\xe5", repos.get_revision(
            repos.generate_revision_id(2, u"", mapping)).message)

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


class JoinedCommitTests(SubversionTestCase):

    def test_commit_join(self):
        repos_url = self.make_client('d', 'sc')

        self.build_tree({'sc/trunk/foo/bla': "data"})
        self.client_add("sc/trunk")
        self.client_commit("sc", "foo")

        self.olddir = ControlDir.open("sc/trunk")
        os.mkdir("dc")
        self.newdir = self.olddir.sprout("dc")

        dc = self.get_commit_editor(repos_url)
        branches = dc.add_dir("branches")
        newdir = branches.add_dir("branches/newbranch")
        newdir.add_file("branches/newbranch/foob").modify()
        dc.close()

        wt = self.newdir.open_workingtree()
        self.build_tree({"dc/lala": "data"})
        wt.add(["lala"])
        wt.commit(message="init")
        joinedwt = ControlDir.create_standalone_workingtree("dc/newdir")
        joinedwt.pull(Branch.open(repos_url+"/branches/newbranch"))
        wt.subsume(joinedwt)
        wt.commit(message="doe")

        self.olddir.open_branch().pull(self.newdir.open_branch())
        paths = self.client_log(repos_url, 4, 0)[4][0]
        self.assertEquals(('A', "/branches/newbranch", 2), paths["/trunk/newdir"])


class TestPushNested(SubversionTestCase):

    def setUp(self):
        super(TestPushNested, self).setUp()
        self.repos_url = self.make_client('d', 'sc')

        self.build_tree({'sc/foo/trunk/bla': "data"})
        self.client_add("sc/foo")
        self.client_commit("sc", "foo")

        self.olddir = ControlDir.open("sc/foo/trunk")
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
        master_branch = self.make_svn_branch("d")
        os.mkdir("b")
        local_dir = master_branch.controldir.sprout("b")
        wt = local_dir.open_workingtree()
        local_dir.open_branch().bind(master_branch)
        local_dir.open_branch().unbind()

    def test_commit(self):
        master_branch = self.make_svn_branch("d")
        os.mkdir("b")
        local_dir = master_branch.controldir.sprout("b")
        local_dir.open_branch().bind(master_branch)
        wt = local_dir.open_workingtree()
        self.build_tree({'b/file': 'data'})
        wt.branch.nick = "somenick"
        wt.add('file')
        revid = wt.commit(message="Commit from Bzr")
        master_branch = Branch.open(master_branch.base)
        self.assertEquals(revid, master_branch.last_revision())

    def test_fileid(self):
        master_branch = self.make_svn_branch("d")
        os.mkdir("b")
        local_dir = master_branch.controldir.sprout("b")
        local_dir.open_branch().bind(master_branch)
        wt = local_dir.open_workingtree()
        self.build_tree({'b/file': 'data'})
        rootid = wt.get_root_id()
        wt.branch.nick = "some-nick"
        wt.add('file')
        oldid = wt.path2id("file")
        revid1 = wt.commit(message="Commit from Bzr")
        wt.rename_one('file', 'file2')
        revid2 = wt.commit(message="Commit from Bzr")
        master_branch = Branch.open(master_branch.base)
        rm_provider = master_branch.repository._revmeta_provider
        mapping = master_branch.repository.get_mapping()
        revmeta1 = rm_provider.get_revision(master_branch.get_branch_path(), 2)
        self.assertEquals({u'': rootid, u"file": oldid},
                revmeta1.get_fileid_overrides(mapping))
        revmeta2 = rm_provider.get_revision(master_branch.get_branch_path(), 3)
        self.assertEquals({u"file2": oldid},
                revmeta2.get_fileid_overrides(mapping))
        tree1 = master_branch.repository.revision_tree(revid1)
        tree2 = master_branch.repository.revision_tree(revid2)
        delta = tree2.changes_from(tree1)
        mutter("changes %r" % list(rm_provider.iter_reverse_branch_changes(master_branch.get_branch_path(), 3, 0)))
        self.assertEquals(0, len(delta.added))
        self.assertEquals(0, len(delta.removed))
        self.assertEquals(1, len(delta.renamed))

    def test_nested_fileid(self):
        master_branch = self.make_svn_branch("d") #1
        os.mkdir("b")
        local_dir = master_branch.controldir.sprout("b")
        local_dir.open_branch().bind(master_branch)
        wt = local_dir.open_workingtree()
        wt.branch.nick = "some-nick"
        self.build_tree({'b/dir/file': 'data'})
        wt.add('dir')
        wt.add('dir/file')
        rootid = wt.get_root_id()
        dirid = wt.path2id("dir")
        fileid = wt.path2id("dir/file")
        revid1 = wt.commit(message="Commit from Bzr") #2
        master_branch = Branch.open(master_branch.base)
        rm_provider = master_branch.repository._revmeta_provider
        mapping = master_branch.repository.get_mapping()
        revmeta = rm_provider.get_revision(master_branch.get_branch_path(), 2)
        self.assertEquals({
            "": rootid,
            "dir": dirid,
            "dir/file": fileid},
            revmeta.get_fileid_overrides(mapping))

    def test_push_merged_revision(self):
        repos_url = self.make_repository("d")

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("trunk")
        dc.add_dir("branches")
        dc.add_dir("tags")
        dc.close() # 1

        master_branch = Branch.open(repos_url+"/trunk")
        trunk = master_branch.controldir.sprout("trunk")
        trunk.open_branch().bind(master_branch)

        config = master_branch.repository.get_config()
        config.set_user_option("push_merged_revisions", "True")

        feature = trunk.sprout("feature")
        wt = feature.open_workingtree()
        self.build_tree_contents([("feature/test.txt", "test")])
        wt.add("test.txt")
        revid2 = wt.commit("added test.txt") # 2

        wt = trunk.open_workingtree()
        wt.merge_from_branch(feature.open_branch(), to_revision=revid2)
        wt.commit("merged feature") # 3

        log = self.client_log(repos_url, 3, 0)
        self.assertEquals({
            '/branches': ('A', None, -1),
            '/tags': ('A', None, -1),
            '/trunk': ('A', None, -1)}, log[1][0])
        self.assertEquals({
            '/branches/feature': ('A', '/trunk', 1),
            '/branches/feature/test.txt': ('A', None, -1)}, log[2][0])
        self.assertEquals({
            '/trunk': ('M', None, -1),
            '/trunk/test.txt': ('A', '/branches/feature/test.txt', 2)},
            log[3][0])


class RevpropTests(SubversionTestCase):

    def test_change_revprops(self):
        repos_url = self.make_repository("d", allow_revprop_changes=True)

        dc = self.get_commit_editor(repos_url, message="My commit")
        dc.add_file("foo.txt").modify()
        dc.close()

        repository = Repository.open(repos_url)
        set_svn_revprops(repository, 1, {"svn:author": "Somebody",
                                        "svn:date": time_to_cstring(1000000*473385600)})

        self.assertEquals(1, repository.get_latest_revnum())

        self.assertEquals(("Somebody", "1985-01-01T00:00:00.000000Z", "My commit"),
                          self.client_log(repos_url, 1, 1)[1][1:])

    def test_change_revprops_disallowed(self):
        repos_url = self.make_repository("d", allow_revprop_changes=False)

        dc = self.get_commit_editor(repos_url)
        dc.add_file("foo.txt").modify()
        dc.close()

        repository = Repository.open(repos_url)
        self.assertRaises(RevpropChangeFailed,
                lambda: set_svn_revprops(repository, 1, {"svn:author": "Somebody", "svn:date": time_to_cstring(1000000*473385600)}))


class SvkTestCase(TestCase):

    def test_revid_svk_map(self):
        self.assertEqual("auuid:/:6",
              _revision_id_to_svk_feature("svn-v3-undefined:auuid::6",
                  mapping_registry.parse_revision_id))


class SendPropChangesTests(TestCase):

    class MockTree(object):

        def __init__(self, file_id, kind, name, executable):
            self._file_id = file_id
            self._name = name
            self._executable = executable
            self._kind = kind

        def has_id(self, file_id):
            return (self._file_id == file_id)

        def is_executable(self, file_id):
            if self._file_id == file_id:
                return self._executable
            raise NoSuchId

        def kind(self, file_id):
            if self._file_id == file_id:
                return self._kind
            raise NoSuchId

    class RecordingFileEditor(object):

        def __init__(self):
            self._props = {}

        def change_prop(self, name, value):
            self._props[name] = value

    def test_become_symlink(self):
        ie2 = InventoryLink("myfileid", "name", "theroot")
        editor = SendPropChangesTests.RecordingFileEditor()
        file_editor_send_prop_changes(
            self.MockTree("myfileid", "file", "name", True), "myfileid",
            ie2, editor)
        self.assertEquals(editor._props,
            {"svn:special": "*", "svn:executable": None})

    def test_become_executable(self):
        tree = self.MockTree("myfileid", "file", "name", False)
        ie2 = InventoryFile("myfileid", "name", "theroot")
        ie2.executable = True
        editor = SendPropChangesTests.RecordingFileEditor()
        file_editor_send_prop_changes(tree, "myfileid",ie2, editor)
        self.assertEquals(editor._props, {"svn:executable": "*"})

    def test_unbecome_executable(self):
        tree = self.MockTree("myfileid", "file", "name", True)
        ie2 = InventoryFile("myfileid", "name", "theroot")
        ie2.executable = False
        editor = SendPropChangesTests.RecordingFileEditor()
        file_editor_send_prop_changes(tree, "myfileid", ie2, editor)
        self.assertEquals(editor._props, {"svn:executable": None})

    def test_nothing_changes(self):
        tree = self.MockTree("myfileid", "file", "name", False)
        ie2 = InventoryFile("myfileid", "name", "theroot")
        editor = SendPropChangesTests.RecordingFileEditor()
        file_editor_send_prop_changes(tree, "myfileid",ie2, editor)
        self.assertEquals(editor._props, {})

