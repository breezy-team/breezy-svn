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

"""Tests for push from a Bazaar to a Subversion repository."""

import os
from subvertpy import (
    NODE_DIR,
    NODE_FILE,
    ra,
    )

from breezy import osutils
from breezy.branch import Branch
from breezy.controldir import ControlDir
from breezy.errors import (
    AlreadyBranchError,
    AppendRevisionsOnlyViolation,
    BzrError,
    DivergedBranches,
    )
from breezy.bzr.inventory import InventoryDirectory
from breezy.merge import (
    Merger,
    Merge3Merger,
    )
from breezy.repository import (
    InterRepository,
    Repository,
    )
from breezy.revision import (
    NULL_REVISION,
    Revision,
    )
from breezy.trace import mutter
from breezy.tests import (
    TestCase,
    )
from breezy.tests.features import (
    SymlinkFeature,
    )

from breezy.plugins.svn import (
    transport,
    )
from breezy.plugins.svn.branch import (
    InterToSvnBranch,
    )
from breezy.plugins.svn.errors import (
    MissingPrefix,
    )
from breezy.plugins.svn.layout.standard import (
    RootLayout,
    TrunkLayout,
    TrunkLayout0,
    )
from breezy.plugins.svn.push import (
    create_branch_container,
    determine_branch_path,
    )
from breezy.plugins.svn.tests import (
    SubversionTestCase,
    )


def revision_history(branch):
    with branch.lock_read():
        graph = branch.repository.get_graph()
        history = list(graph.iter_lefthand_ancestry(branch.last_revision(),
            [NULL_REVISION]))
    history.reverse()
    return history


class TestDPush(SubversionTestCase):

    def setUp(self):
        super(TestDPush, self).setUp()
        transport.disabled_capabilities.update(["commit-revprops", "log-revprops"])
        self.repos_url = self.make_svn_repository('d')

        dc = self.commit_editor()
        foo = dc.add_dir("foo")
        foo.add_file("foo/bla").modify(b"data")
        dc.close()

        self.svndir = ControlDir.open(self.repos_url)
        os.mkdir("dc")
        self.controldir = self.svndir.sprout("dc")

    def tearDown(self):
        TestCase.tearDown(self)
        transport.disabled_capabilities = set()

    def commit_editor(self, message="Test commit"):
        return self.get_commit_editor(self.repos_url, message)

    def dpush(self, source, target):
        with target.lock_write():
            return InterToSvnBranch(source, target)._update_revisions(lossy=True)

    def test_change_single(self):
        self.build_tree({'dc/foo/bla': b'other data'})
        wt = self.controldir.open_workingtree()
        newid = wt.commit(message="Commit from Bzr")

        source_branch = self.controldir.open_branch()
        with source_branch.lock_write():
            old_revid, new_revid, revid_map = self.dpush(source_branch,
                    self.svndir.open_branch())

        self.assertEquals([newid], revid_map.keys())

        c = ra.RemoteAccess(self.repos_url)
        (entries, fetch_rev, props) = c.get_dir("", c.get_latest_revnum())
        self.assertEquals(set(['svn:entry:committed-rev',
            'svn:entry:last-author', 'svn:entry:uuid',
            'svn:entry:committed-date']), set(props.keys()))

        r = self.svndir.find_repository()
        self.assertEquals([
            r.generate_revision_id(
                c.get_latest_revnum(), u"", r.get_mapping())],
            revid_map.values())

    def test_change_multiple(self):
        self.build_tree({'dc/foo/bla': b'other data'})
        wt = self.controldir.open_workingtree()
        self.build_tree({'dc/foo/bla': b'other data'})
        newid1 = wt.commit(message="Commit from Bzr")
        self.build_tree({'dc/foo/bla': b'yet other data'})
        newid2 = wt.commit(message="Commit from Bzr")

        source_branch = self.controldir.open_branch()
        with source_branch.lock_write():
            old_revid, new_revid, revid_map = self.dpush(source_branch,
                    self.svndir.open_branch())

        self.assertEquals(set([newid1, newid2]), set(revid_map.keys()))

        c = ra.RemoteAccess(self.repos_url)
        (entries, fetch_rev, props) = c.get_dir("", c.get_latest_revnum())
        self.assertEquals(set(['svn:entry:committed-rev',
            'svn:entry:last-author', 'svn:entry:uuid',
            'svn:entry:committed-date']), set(props.keys()))

        r = self.svndir.find_repository()
        self.assertEquals(set([r.generate_revision_id(rev, u"",
                r.get_mapping()) for rev in (c.get_latest_revnum()-1, c.get_latest_revnum())]),
                set(revid_map.values()))

    def test_dpush_add(self):
        wt = self.controldir.open_workingtree()
        self.build_tree({'dc/foo/blal': b'other data'})
        wt.add(["foo/blal"])
        newid1 = wt.commit(message="Commit from Bzr")
        self.build_tree({'dc/foo/bliel': b'yet other data'})
        wt.add(["foo/bliel"])
        newid2 = wt.commit(message="Commit from Bzr")

        source_branch = self.controldir.open_branch()
        with source_branch.lock_write():
            old_revid, new_revid, revid_map = self.dpush(source_branch,
                self.svndir.open_branch())

        self.assertEquals(set([newid1, newid2]), set(revid_map.keys()))
        repos = self.svndir.find_repository()
        revmeta = repos._revmeta_provider.get_revision(u"", 3)
        self.assertChangedPathsEquals(
                {u'': ('M', None, -1, 2),
                 u"foo/bliel": ('A', None, -1, NODE_FILE)}, revmeta.metarev.paths)

    def test_diverged(self):
        dc = self.commit_editor()
        foo = dc.open_dir("foo")
        foo.add_file("foo/bar").modify(b"data")
        dc.close()

        svndir = ControlDir.open(self.repos_url)

        self.build_tree({'dc/file': b'data'})
        wt = self.controldir.open_workingtree()
        wt.add('file')
        wt.commit(message="Commit from Bzr")

        source_branch = self.controldir.open_branch()
        with source_branch.lock_write():
            self.assertRaises(DivergedBranches,
                              self.dpush, source_branch,
                              svndir.open_branch())


class TestPush(SubversionTestCase):

    def setUp(self):
        super(TestPush, self).setUp()
        self.repos_url = self.make_svn_repository('d')

        dc = self.commit_editor()
        foo = dc.add_dir("foo")
        foo.add_file("foo/bla").modify(b"data")
        dc.close() #1

        self.svndir = ControlDir.open(self.repos_url)
        os.mkdir("dc")
        self.controldir = self.svndir.sprout("dc")

        repo = self.svndir.open_repository()
        self.revid1 = repo.generate_revision_id(1, u"", repo.get_mapping())

    def commit_editor(self, message="Test commit"):
        return self.get_commit_editor(self.repos_url, message)

    def test_empty(self):
        svnbranch = self.svndir.open_branch()
        bzrbranch = self.controldir.open_branch()
        result = svnbranch.pull(bzrbranch)
        self.assertEqual(0, result.new_revno - result.old_revno)
        self.assertEqual(svnbranch.last_revision_info(),
                         bzrbranch.last_revision_info())

    def test_child(self):
        dc = self.commit_editor()
        foo = dc.open_dir("foo")
        foo.add_file("foo/bar").modify(b"data")
        dc.close()

        svnbranch = self.svndir.open_branch()
        bzrbranch = self.controldir.open_branch()
        result = svnbranch.pull(bzrbranch)
        self.assertEqual(0, result.new_revno - result.old_revno)

    def test_diverged(self):
        dc = self.commit_editor()
        foo = dc.open_dir("foo")
        foo.add_file("foo/bar").modify(b"data")
        dc.close()

        svndir = ControlDir.open(self.repos_url)

        self.build_tree({'dc/file': b'data'})
        wt = self.controldir.open_workingtree()
        wt.add('file')
        wt.commit(message="Commit from Bzr")

        self.assertRaises(DivergedBranches,
                          svndir.open_branch().pull,
                          self.controldir.open_branch())

    def test_change(self):
        self.build_tree({'dc/foo/bla': b'other data'})
        wt = self.controldir.open_workingtree()
        newid = wt.commit(message="Commit from Bzr")

        svnbranch = self.svndir.open_branch()
        svnbranch.pull(self.controldir.open_branch())

        repos = self.svndir.find_repository()
        mapping = repos.get_mapping()
        self.assertEquals(newid, svnbranch.last_revision())
        tree = repos.revision_tree(repos.generate_revision_id(2, u"", mapping))
        self.assertEqual(newid, tree.get_file_revision('foo/bla'))
        self.assertEqual(wt.branch.last_revision(),
          repos.generate_revision_id(2, u"", mapping))
        self.assertEqual(repos.generate_revision_id(2, u"", mapping),
                        self.svndir.open_branch().last_revision())
        self.assertEqual("other data", tree.get_file_text("foo/bla"))

    def test_simple(self):
        self.build_tree({'dc/file': b'data'})
        wt = self.controldir.open_workingtree()
        wt.add('file')
        wt.commit(message="Commit from Bzr")

        self.svndir.open_branch().pull(self.controldir.open_branch())

        repos = self.svndir.find_repository()
        mapping = repos.get_mapping()
        tree = repos.revision_tree(repos.generate_revision_id(2, u"", mapping))
        self.assertTrue(tree.has_filename('file'))
        self.assertEquals(wt.branch.last_revision(),
                repos.generate_revision_id(2, u"", mapping))
        self.assertEqual(repos.generate_revision_id(2, u"", mapping),
                        self.svndir.open_branch().last_revision())

    def test_override_revprops(self):
        self.svndir.find_repository().get_config().set_user_option("override-svn-revprops", "True")
        self.build_tree({'dc/file': b'data'})
        wt = self.controldir.open_workingtree()
        wt.add('file')
        wt.commit(message="Commit from Bzr", committer="Sombody famous", timestamp=1012604400, timezone=0)

        self.svndir.open_branch().pull(self.controldir.open_branch())

        self.assertEquals(("Sombody famous", "2002-02-01T23:00:00.000000Z", "Commit from Bzr"),
            self.client_log(self.repos_url, 0, 2)[2][1:])

    def test_empty_file(self):
        self.build_tree({'dc/file': b''})
        wt = self.controldir.open_workingtree()
        wt.add('file')
        wt.commit(message="Commit from Bzr")

        self.svndir.open_branch().pull(self.controldir.open_branch())

        repos = self.svndir.find_repository()
        mapping = repos.get_mapping()
        tree = repos.revision_tree(repos.generate_revision_id(2, u"", mapping))
        self.assertTrue(tree.has_filename('file'))
        self.assertEquals(wt.branch.last_revision(),
                repos.generate_revision_id(2, u"", mapping))
        self.assertEqual(repos.generate_revision_id(2, u"", mapping),
                        self.svndir.open_branch().last_revision())

    def test_symlink(self):
        self.requireFeature(SymlinkFeature)
        os.symlink("bla", "dc/south")
        self.assertTrue(os.path.islink("dc/south"))
        wt = self.controldir.open_workingtree()
        wt.add('south')
        self.assertEquals("bla", wt.get_symlink_target("south"))
        wt.commit(message="Commit from Bzr")

        self.svndir.open_branch().pull(self.controldir.open_branch())

        repos = self.svndir.find_repository()
        mapping = repos.get_mapping()
        tree = repos.revision_tree(repos.generate_revision_id(2, u"", mapping))
        self.assertTrue(tree.has_filename('south'))
        self.assertEquals('symlink', tree.kind('south'))
        self.assertEquals('bla', tree.get_symlink_target('south'))

    def test_pull_after_push(self):
        self.build_tree({'dc/file': b'data'})
        wt = self.controldir.open_workingtree()
        wt.add('file')
        wt.commit(message="Commit from Bzr")

        self.svndir.open_branch().pull(self.controldir.open_branch())

        repos = self.svndir.find_repository()
        mapping = repos.get_mapping()
        tree = repos.revision_tree(repos.generate_revision_id(2, u"", mapping))
        self.assertTrue(tree.has_filename('file'))
        self.assertEquals(wt.branch.last_revision(),
                         repos.generate_revision_id(2, u"", mapping))
        self.assertEqual(repos.generate_revision_id(2, u"", mapping),
                        self.svndir.open_branch().last_revision())

        self.controldir.open_branch().pull(self.svndir.open_branch())

        self.assertEqual(repos.generate_revision_id(2, u"", mapping),
                        self.controldir.open_branch().last_revision())

    def test_branch_after_push(self):
        self.build_tree({'dc/file': b'data'})
        wt = self.controldir.open_workingtree()
        wt.add('file')
        wt.commit(message="Commit from Bzr")

        self.svndir.open_branch().pull(self.controldir.open_branch())

        os.mkdir("b")
        repos = self.svndir.sprout("b")

        self.assertEqual(Branch.open("dc").last_revision_info(),
                         Branch.open("b").last_revision_info())

    def test_fetch_preserves_file_revid(self):
        self.build_tree({'dc/file': b'data'})
        wt = self.controldir.open_workingtree()
        wt.add('file')
        self.build_tree({'dc/foo/bla': b'data43243242'})
        revid = wt.commit(message="Commit from Bzr") #2

        self.svndir.open_branch().pull(self.controldir.open_branch())

        os.mkdir("b")
        repos = self.svndir.sprout("b")

        b = Branch.open("b")

        def check_tree_revids(rtree):
            self.assertEqual(rtree.get_file_revision("file"),
                             revid)
            self.assertEqual(rtree.get_file_revision("foo"),
                             self.revid1)
            self.assertEqual(rtree.get_file_revision("foo/bla"),
                             revid)
            self.assertEqual(rtree.get_revision_id(), b.last_revision())

        check_tree_revids(wt.branch.repository.revision_tree(b.last_revision()))

        check_tree_revids(b.repository.revision_tree(b.last_revision()))
        bc = self.svndir.open_branch()
        check_tree_revids(bc.repository.revision_tree(bc.last_revision()))

    def test_message(self):
        self.build_tree({'dc/file': b'data'})
        wt = self.controldir.open_workingtree()
        wt.add('file')
        wt.commit(message="Commit from Bzr")

        self.svndir.open_branch().pull(self.controldir.open_branch())

        repos = self.svndir.find_repository()
        mapping = repos.get_mapping()
        self.assertEqual("Commit from Bzr",
          repos.get_revision(repos.generate_revision_id(2, u"", mapping)).message)

    def test_commit_set_revid(self):
        self.build_tree({'dc/file': b'data'})
        wt = self.controldir.open_workingtree()
        wt.add('file')
        wt.commit(message="Commit from Bzr", rev_id=b"some-rid")

        self.svndir.open_branch().pull(self.controldir.open_branch())

        self.assertEquals((3, "some-rid"),
                self.svndir.open_branch().last_revision_info())

    def test_commit_check_rev_equal(self):
        self.build_tree({'dc/file': b'data'})
        wt = self.controldir.open_workingtree()
        wt.add('file')
        wt.commit(message="Commit from Bzr")

        self.svndir.open_branch().pull(self.controldir.open_branch())

        rev1 = self.svndir.find_repository().get_revision(wt.branch.last_revision())
        rev2 = self.controldir.find_repository().get_revision(wt.branch.last_revision())

        self.assertEqual(rev1.committer, rev2.committer)
        self.assertEqual(rev1.timestamp, rev2.timestamp)
        self.assertEqual(rev1.timezone, rev2.timezone)
        self.assertEqual(rev1.properties, rev2.properties)
        self.assertEqual(rev1.message, rev2.message)
        self.assertEqual(rev1.revision_id, rev2.revision_id)

    def test_multiple(self):
        self.build_tree({'dc/file': b'data'})
        wt = self.controldir.open_workingtree()
        wt.add('file')
        wt.commit(message="Commit from Bzr")

        self.build_tree({'dc/file': b'data2', 'dc/adir': None})
        wt.add('adir')
        wt.commit(message="Another commit from Bzr")

        self.svndir.open_branch().pull(self.controldir.open_branch())

        repos = self.svndir.find_repository()

        mapping = repos.get_mapping()

        self.assertEqual(repos.generate_revision_id(3, u"", mapping),
                        self.svndir.open_branch().last_revision())

        tree = repos.revision_tree(repos.generate_revision_id(2, u"", mapping))
        self.assertTrue(tree.has_filename('file'))
        self.assertFalse(tree.has_filename('adir'))

        tree = repos.revision_tree(repos.generate_revision_id(3, u"", mapping))
        self.assertTrue(tree.has_filename('file'))
        self.assertTrue(tree.has_filename('adir'))

        self.assertEqual(self.svndir.open_branch().last_revision_info(),
                         self.controldir.open_branch().last_revision_info())

        self.assertEqual(wt.branch.last_revision(),
                repos.generate_revision_id(3, u"", mapping))

    def test_multiple_diverged(self):
        oc_url = self.make_svn_repository("o")

        self.build_tree({'dc/file': b'data'})
        wt = self.controldir.open_workingtree()
        wt.add('file')
        wt.commit(message="Commit from Bzr")

        oc = self.get_commit_editor(oc_url)
        oc.add_file("file").modify(b"data2")
        oc.add_dir("adir")
        oc.close()

        self.assertRaises(DivergedBranches,
                lambda: Branch.open(oc_url).pull(self.controldir.open_branch()))

    def test_different_branch_path(self):
        # A       ,> C -> D
        # \ -> B /
        dc = self.commit_editor("Test commit 1")
        trunk = dc.add_dir("trunk")
        trunk.add_file('trunk/foo').modify(b"data")
        dc.add_dir("branches")
        dc.close()

        dc = self.commit_editor("Test commit 2")
        branches = dc.open_dir('branches')
        mybranch = branches.add_dir('branches/mybranch', 'trunk')
        mybranch.open_file("branches/mybranch/foo").modify(b'data2')
        dc.close()

        self.svndir = ControlDir.open("%s/branches/mybranch" % self.repos_url)
        os.mkdir("mybranch")
        self.controldir = self.svndir.sprout("mybranch")

        self.build_tree({'mybranch/foo': b'bladata'})
        wt = self.controldir.open_workingtree()
        revid = wt.commit(message="Commit from Bzr")

        b = Branch.open("%s/trunk" % self.repos_url)
        wt.branch.push(b, stop_revision=revision_history(wt.branch)[-2])
        mutter('log %r' % self.client_log("%s/trunk" % self.repos_url, 0, 4)[4][0])
        if not b.mapping.can_use_revprops and b.mapping.can_use_fileprops:
            self.assertEquals('M',
                self.client_log("%s/trunk" % self.repos_url, 0, 4)[4][0]['/trunk'][0])
        b = Branch.open("%s/trunk" % self.repos_url)
        wt.branch.push(b)
        mutter('log %r' % self.client_log("%s/trunk" % self.repos_url, 0, 5)[5][0])
        self.assertEquals({
            '/trunk': ('M', None, -1),
            '/trunk/foo': ('M', None, -1)},
            self.client_log("%s/trunk" % self.repos_url, 0, 5)[5][0])

    def test_comics(self):
        dc = self.commit_editor()
        trunk = dc.add_dir("trunk")
        comics = trunk.add_dir("trunk/comics")
        comics.add_dir("trunk/comics/bin")
        dc.close()

        self.svndir = ControlDir.open("%s/trunk" % self.repos_url)
        os.mkdir("mybranch")
        self.controldir = self.svndir.sprout("mybranch")
        wt = self.controldir.open_workingtree()

        wt.rename_one("comics", "old-comics")
        wt.mkdir("comics")
        wt.rename_one("old-comics", "comics/core")
        wt.rename_one("comics/core/bin", "comics/bin")

        wt.commit("Strange comics")

        self.svndir.open_branch().pull(wt.branch)

        paths = self.svndir.find_repository()._revmeta_provider.get_revision(u"", 3).metarev.paths
        self.assertChangedPathsEquals({
            u'trunk': ('M', None, -1, NODE_DIR),
            u'trunk/comics': ('R', None, -1, NODE_DIR),
            u'trunk/comics/bin': ('A', u'trunk/comics/bin', 2, NODE_DIR),
            u'trunk/comics/core': ('A', u'trunk/comics', 2, NODE_DIR),
            u'trunk/comics/core/bin': ('D', None, -1, NODE_DIR)}, paths)


class PushNewBranchTests(SubversionTestCase):

    def _create_single_rev_bzrwt(self):
        bzrwt = ControlDir.create_standalone_workingtree("c")
        self.build_tree({'c/test': b"Tour"})
        bzrwt.add("test")
        revid = bzrwt.commit("Do a commit")
        return bzrwt, revid

    def test_fetch_after_push(self):
        repos_url = self.make_svn_repository("a")
        newdir = ControlDir.open("%s/trunk" % repos_url)
        bzrwt, revid = self._create_single_rev_bzrwt()
        newbranch = newdir.import_branch(bzrwt.branch)

        oldrepos = Repository.open(repos_url)
        oldrepos.set_layout(TrunkLayout(0))
        dir = ControlDir.create("f")
        newrepos = dir.create_repository()
        oldrepos.copy_content_into(newrepos)
        newtree = newrepos.revision_tree(revid)

        with bzrwt.lock_read():
            self.assertEquals(bzrwt.path2id(''), newtree.path2id(''))

    def test_single_revision(self):
        repos_url = self.make_svn_repository("a")
        newdir = ControlDir.open("%s/trunk" % repos_url)
        bzrwt, revid = self._create_single_rev_bzrwt()
        newbranch = newdir.import_branch(bzrwt.branch)
        newtree = newbranch.repository.revision_tree(revid)
        with bzrwt.lock_read():
            self.assertEquals(bzrwt.path2id(''), newtree.path2id(''))
        self.assertEquals(revid, newbranch.last_revision())

    def test_single_revision_single_branch(self):
        repos_url = self.make_svn_repository("a")
        bzrwt = ControlDir.create_standalone_workingtree("c")
        self.build_tree({'c/test': b"Tour"})
        bzrwt.add("test")
        revid = bzrwt.commit("Do a commit")

        dc = self.get_commit_editor(repos_url)
        some = dc.add_dir("some")
        funny = some.add_dir("some/funny")
        funny.add_dir("some/funny/branch")
        dc.close()
        newdir = ControlDir.open("%s/some/funny/branch/name" % repos_url)
        newbranch = newdir.import_branch(bzrwt.branch)
        self.assertEquals(revid, newbranch.last_revision())

    # revision graph for the two tests below:
    # svn-1
    # |
    # base
    # |    \
    # diver svn2
    # |    /
    # merge

    def test_push_replace_existing_root(self):
        repos_url = self.make_client("test", "svnco")
        self.build_tree({'svnco/foo.txt': b'foo'})
        self.client_add("svnco/foo.txt")
        self.client_commit("svnco", "add file")  #1
        self.client_update("svnco")

        os.mkdir('bzrco')
        dir = ControlDir.open(repos_url).sprout("bzrco")
        wt = dir.open_workingtree()
        self.build_tree({'bzrco/bar.txt': b'bar'})
        wt.add("bar.txt")
        base_revid = wt.commit("add another file", rev_id=b"mybase")
        wt.branch.push(Branch.open(repos_url))

        self.build_tree({"svnco/baz.txt": b"baz"})
        self.client_add("svnco/baz.txt")
        self.assertEquals(
            3, self.client_commit("svnco", "add yet another file")[0])
        self.client_update("svnco")

        self.build_tree({"bzrco/qux.txt": b"qux"})
        wt.add("qux.txt")
        wt.commit("add still more files", rev_id=b"mydiver")

        repos = Repository.open(repos_url)
        repos.set_layout(RootLayout())
        wt.branch.repository.fetch(repos)
        mapping = repos.get_mapping()
        other_rev = repos.generate_revision_id(3, u"", mapping)
        with wt.lock_write():
            merge = Merger.from_revision_ids(wt, other=other_rev)
            merge.merge_type = Merge3Merger
            merge.do_merge()
            self.assertEquals(base_revid, merge.base_rev_id)
            merge.set_pending()
            self.assertEquals([wt.last_revision(), other_rev], wt.get_parent_ids())
            wt.commit("merge", rev_id=b"mymerge")
        self.assertTrue(os.path.exists("bzrco/baz.txt"))
        self.assertRaises(
            BzrError, lambda: wt.branch.push(Branch.open(repos_url)))

    def test_push_replace_existing_branch(self):
        repos_url = self.make_client("test", "svnco")
        self.build_tree({'svnco/trunk/foo.txt': b'foo'})
        self.client_add("svnco/trunk")
        self.client_commit("svnco", "add file") #1
        self.client_update("svnco")

        os.mkdir('bzrco')
        dir = ControlDir.open(repos_url+"/trunk").sprout("bzrco")
        wt = dir.open_workingtree()
        self.build_tree({'bzrco/bar.txt': b'bar'})
        wt.add("bar.txt")
        base_revid = wt.commit("add another file", rev_id=b"mybase")
        wt.branch.push(Branch.open(repos_url+"/trunk"))

        self.build_tree({"svnco/trunk/baz.txt": b"baz"})
        self.client_add("svnco/trunk/baz.txt")
        self.assertEquals(3,
                self.client_commit("svnco", "add yet another file")[0])
        self.client_update("svnco")

        self.build_tree({"bzrco/qux.txt": b"qux"})
        wt.add("qux.txt")
        wt.commit("add still more files", rev_id=b"mydiver")

        repos = Repository.open(repos_url)
        wt.branch.repository.fetch(repos)
        mapping = repos.get_mapping()
        other_rev = repos.generate_revision_id(3, u"trunk", mapping)
        with wt.lock_write():
            merge = Merger.from_revision_ids(wt, other=other_rev)
            merge.merge_type = Merge3Merger
            merge.do_merge()
            self.assertEquals(base_revid, merge.base_rev_id)
            merge.set_pending()
            self.assertEquals([wt.last_revision(), other_rev], wt.get_parent_ids())
            wt.commit("merge", rev_id=b"mymerge")
        self.assertTrue(os.path.exists("bzrco/baz.txt"))
        target_branch = Branch.open(repos_url+"/trunk")
        self.assertRaises(AppendRevisionsOnlyViolation, wt.branch.push, target_branch)
        target_branch.set_append_revisions_only(False)
        wt.branch.push(target_branch)

    def test_push_merge_unchanged_file(self):
        def check_tree(t):
            self.assertEquals(base_revid,
                t.get_file_revision("bar.txt"))
            self.assertEquals(other_revid,
                t.get_file_revision("bar2.txt"))
        repos_url = self.make_svn_repository("test")

        dc = self.get_commit_editor(repos_url)
        trunk = dc.add_dir("trunk")
        trunk.add_file("trunk/foo.txt").modify(b"add file")
        dc.add_dir("branches")
        dc.close()

        os.mkdir('bzrco1')
        dir1 = ControlDir.open(repos_url+"/trunk").sprout("bzrco1")

        os.mkdir('bzrco2')
        dir2 = ControlDir.open(repos_url+"/trunk").sprout("bzrco2")

        wt1 = dir1.open_workingtree()
        self.build_tree({'bzrco1/bar.txt': b'bar'})
        wt1.add("bar.txt")
        base_revid = wt1.commit("add another file", rev_id=b"mybase")
        wt1.branch.push(Branch.open(repos_url+"/trunk"))

        wt2 = dir2.open_workingtree()
        self.build_tree({'bzrco2/bar2.txt': b'bar'})
        wt2.add("bar2.txt")
        other_revid = wt2.commit("add yet another file", rev_id=b"side1")
        self.assertEquals("side1", other_revid)
        side1_dir = ControlDir.open(repos_url+"/branches/side1")
        side1_dir.push_branch(wt2.branch)

        with wt1.lock_write():
            wt1.merge_from_branch(wt2.branch)
            self.assertEquals(
                [wt1.last_revision(), other_revid], wt1.get_parent_ids())
            mergingrevid = wt1.commit("merge", rev_id=b"side2")
            check_tree(wt1.branch.repository.revision_tree(mergingrevid))
        self.assertTrue(os.path.exists("bzrco1/bar2.txt"))
        wt1.branch.push(Branch.open(repos_url+"/trunk"))
        r = Repository.open(repos_url)
        r._revmeta_provider.get_revision(u"trunk", 3)

        os.mkdir("cpy")
        cpy = ControlDir.create("cpy")
        cpyrepos = cpy.create_repository()

        t = r.revision_tree(mergingrevid)
        check_tree(t)
        r.copy_content_into(cpyrepos)
        check_tree(cpyrepos.revision_tree(mergingrevid))

    def test_missing_prefix_error(self):
        repos_url = self.make_svn_repository("a")
        bzrwt = ControlDir.create_standalone_workingtree("c")
        self.build_tree({'c/test': b"Tour"})
        bzrwt.add("test")
        bzrwt.commit("Do a commit")
        newdir = ControlDir.open(repos_url+"/foo/trunk")
        self.assertRaises(MissingPrefix,
                          lambda: newdir.import_branch(bzrwt.branch))

    def test_repeat(self):
        repos_url = self.make_svn_repository("a")
        bzrwt = ControlDir.create_standalone_workingtree("c")
        self.build_tree({'c/test': b"Tour"})
        bzrwt.add("test")
        revid = bzrwt.commit("Do a commit")
        newdir = ControlDir.open(repos_url+"/trunk")
        newbranch = newdir.import_branch(bzrwt.branch)
        self.assertEquals(revid, newbranch.last_revision())
        self.build_tree({'c/test': b"Tour de France"})
        bzrwt.commit("Do a commit")
        newdir = ControlDir.open(repos_url+"/trunk")
        self.assertRaises(AlreadyBranchError, newdir.import_branch,
                          bzrwt.branch)

    def test_multiple(self):
        repos_url = self.make_svn_repository("a")
        bzrwt = ControlDir.create_standalone_workingtree("c")
        self.build_tree({'c/test': b"Tour"})
        bzrwt.add("test")
        bzrwt.commit("Do a commit")
        self.build_tree({'c/test': b"Tour de France"})
        revid2 = bzrwt.commit("Do a commit")
        newdir = ControlDir.open(repos_url+"/trunk")
        newbranch = newdir.import_branch(bzrwt.branch)
        self.assertEquals(revid2, newbranch.last_revision())

    def test_dato(self):
        repos_url = self.make_svn_repository("a")
        bzrwt = ControlDir.create_standalone_workingtree("c")
        self.build_tree({'c/foo.txt': b"foo"})
        bzrwt.add("foo.txt")
        revid1 = bzrwt.commit("Do a commit",
                              committer=u"Adeodato Simó <dato@net.com.org.es>")
        newdir = ControlDir.open(repos_url+"/trunk")
        newdir.import_branch(bzrwt.branch)
        self.assertEquals(
            u"Adeodato Simó <dato@net.com.org.es>",
            Repository.open(repos_url).get_revision(revid1).committer)

    def test_utf8_commit_msg(self):
        repos_url = self.make_svn_repository("a")
        bzrwt = ControlDir.create_standalone_workingtree("c")
        self.build_tree({'c/foo.txt': b"foo"})
        bzrwt.add("foo.txt")
        revid1 = bzrwt.commit(u"Do á commït")
        newdir = ControlDir.open(repos_url+"/trunk")
        newdir.import_branch(bzrwt.branch)
        self.assertEquals(
            u"Do á commït",
            Repository.open(repos_url).get_revision(revid1).message)

    def test_kind_change_file_to_directory(self):
        repos_url = self.make_svn_repository("a")
        bzrwt = ControlDir.create_standalone_workingtree("c")
        self.build_tree({'c/foo.txt': b"foo"})
        bzrwt.add("foo.txt")
        bzrwt.commit(u"somecommit")
        os.remove("c/foo.txt")
        self.build_tree({"c/foo.txt/bar": b"contents"})
        bzrwt.add("foo.txt")
        bzrwt.commit(u"somecommit")
        newdir = ControlDir.open(repos_url+"/trunk")
        newdir.import_branch(bzrwt.branch)

    def test_kind_change_directory_to_file(self):
        repos_url = self.make_svn_repository("a")
        bzrwt = ControlDir.create_standalone_workingtree("c")
        self.build_tree({'c/foo.txt/bar': b"foo"})
        bzrwt.add("foo.txt")
        bzrwt.commit(u"somecommit")
        os.remove("c/foo.txt/bar")
        os.rmdir("c/foo.txt")
        self.build_tree({"c/foo.txt": b"contents"})
        bzrwt.add("foo.txt")
        bzrwt.commit(u"somecommit")
        newdir = ControlDir.open(repos_url+"/trunk")
        newdir.import_branch(bzrwt.branch)

    def test_multiple_part_exists(self):
        repos_url = self.make_svn_repository("a")

        dc = self.get_commit_editor(repos_url)
        trunk = dc.add_dir('trunk')
        trunk.add_file("trunk/myfile").modify(b"data")
        dc.add_dir("branches")
        dc.close()

        svnrepos = Repository.open(repos_url)
        os.mkdir("c")
        controldir = ControlDir.open(repos_url+"/trunk").sprout("c")
        bzrwt = controldir.open_workingtree()
        self.build_tree({'c/myfile': b"Tour"})
        revid1 = bzrwt.commit("Do a commit")
        self.build_tree({'c/myfile': b"Tour de France"})
        revid2 = bzrwt.commit("Do a commit")
        newdir = ControlDir.open(repos_url+"/branches/mybranch")
        newbranch = newdir.import_branch(bzrwt.branch)
        self.assertEquals(revid2, newbranch.last_revision())
        mapping = svnrepos.get_mapping()
        self.addCleanup(newbranch.lock_read().unlock)
        graph = newbranch.repository.get_graph()
        self.assertEquals(
            [revid2, revid1,
             svnrepos.generate_revision_id(1, u"trunk", mapping)],
            list(graph.iter_lefthand_ancestry(newbranch.last_revision())))

    def test_push_overwrite(self):
        repos_url = self.make_svn_repository("a")

        dc = self.get_commit_editor(repos_url)
        trunk = dc.add_dir("trunk")
        trunk.add_file("trunk/bloe").modify(b"text")
        dc.close()

        os.mkdir("d1")
        controldir = ControlDir.open(repos_url+"/trunk").sprout("d1")
        bzrwt1 = controldir.open_workingtree()

        os.mkdir("d2")
        controldir = ControlDir.open(repos_url+"/trunk").sprout("d2")
        bzrwt2 = controldir.open_workingtree()

        self.build_tree({'d1/myfile': b"Tour"})
        bzrwt1.add("myfile")
        bzrwt1.commit("Do a commit")

        self.build_tree({'d2/myfile': b"France"})
        bzrwt2.add("myfile")
        bzrwt2.commit("Do a commit")

        bzrwt1.branch.push(Branch.open(repos_url+"/trunk"))
        self.assertEquals(
            bzrwt1.branch.last_revision_info(),
            Branch.open(repos_url+"/trunk").last_revision_info())

        bzrwt2.branch.push(Branch.open(repos_url+"/trunk"), overwrite=True)

        self.assertEquals(
            bzrwt2.branch.last_revision_info(),
            Branch.open(repos_url+"/trunk").last_revision_info())

    def test_push_overwrite_unrelated(self):
        repos_url = self.make_svn_repository("a")

        dc = self.get_commit_editor(repos_url)
        trunk = dc.add_dir("trunk")
        trunk.add_file("trunk/bloe").modify(b"text")
        dc.close()

        os.mkdir("d1")
        controldir = ControlDir.open(repos_url+"/trunk").sprout("d1")
        bzrwt1 = controldir.open_workingtree()

        bzrwt2 = ControlDir.create_standalone_workingtree("d2")

        self.build_tree({'d1/myfile': b"Tour"})
        bzrwt1.add("myfile")
        bzrwt1.commit("Do a commit")

        self.build_tree({'d2/myfile': b"France"})
        bzrwt2.add("myfile")
        bzrwt2.commit("Do a commit")

        bzrwt1.branch.push(Branch.open(repos_url+"/trunk"))
        self.assertEquals(
            bzrwt1.branch.last_revision_info(),
            Branch.open(repos_url+"/trunk").last_revision_info())

        bzrwt2.branch.push(Branch.open(repos_url+"/trunk"), overwrite=True)

        self.assertEquals(
            bzrwt2.branch.last_revision_info(),
            Branch.open(repos_url+"/trunk").last_revision_info())

    def test_complex_rename(self):
        repos_url = self.make_svn_repository("a")
        bzrwt = ControlDir.create_standalone_workingtree("c")
        self.build_tree({'c/registry/generic.c': b"Tour"})
        bzrwt.add("registry")
        bzrwt.add("registry/generic.c")
        bzrwt.commit("Add initial directory + file")
        bzrwt.rename_one("registry", "registry.moved")
        os.unlink("c/registry.moved/generic.c")
        bzrwt.remove("registry.moved/generic.c")
        self.build_tree({'c/registry/generic.c': b"bla"})
        bzrwt.add("registry")
        bzrwt.add("registry/generic.c")
        revid2 = bzrwt.commit("Do some funky things")
        newdir = ControlDir.open(repos_url+"/trunk")
        newbranch = newdir.import_branch(bzrwt.branch)
        self.assertEquals(revid2, newbranch.last_revision())
        tree = newbranch.repository.revision_tree(revid2)
        delta = tree.changes_from(bzrwt)
        self.assertFalse(delta.has_changed())
        self.assertTrue(tree.has_filename("registry"))
        self.assertTrue(tree.has_filename("registry.moved"))
        self.assertTrue(tree.has_filename("registry/generic.c"))
        self.assertFalse(tree.has_filename("registry.moved/generic.c"))
        os.mkdir("n")
        ControlDir.open(repos_url+"/trunk").sprout("n")

    def test_rename_dir_changing_contents(self):
        repos_url = self.make_svn_repository("a")
        bzrwt = ControlDir.create_standalone_workingtree("c")
        self.build_tree({'c/registry/generic.c': b"Tour"})
        bzrwt.add("registry", "dirid")
        bzrwt.add("registry/generic.c", "origid")
        revid1 = bzrwt.commit("Add initial directory + file")
        bzrwt.rename_one("registry/generic.c", "registry/c.c")
        self.build_tree({'c/registry/generic.c': b"Tour2"})
        bzrwt.add("registry/generic.c", "newid")
        revid2 = bzrwt.commit("Other change")
        bzrwt.rename_one("registry", "registry.moved")
        revid3 = bzrwt.commit("Rename")
        newdir = ControlDir.open(repos_url+"/trunk")
        newbranch = newdir.import_branch(bzrwt.branch)

        def check(b):
            self.assertEquals([revid1, revid2, revid3], revision_history(b))
            tree = b.repository.revision_tree(revid3)
            self.assertEquals("origid", tree.path2id("registry.moved/c.c"))
            self.assertEquals(
                "newid", tree.path2id("registry.moved/generic.c"))
            self.assertEquals("dirid", tree.path2id("registry.moved"))
        check(newbranch)
        os.mkdir("n")
        ControlDir.open(repos_url+"/trunk").sprout("n")
        copybranch = Branch.open("n")
        check(copybranch)

    def _create_bzrwt_with_changed_root(self):
        bzrwt = ControlDir.create_standalone_workingtree("c")
        self.build_tree({"c/foo": b"bla"})
        bzrwt.add(["foo"])
        revid1 = bzrwt.commit("Initial")
        with bzrwt.lock_write():
            new_ie = bzrwt.root_inventory.root.copy()
            foo_ie = bzrwt.root_inventory.get_entry(
                bzrwt.path2id("foo")).copy()
            new_ie.file_id = "mynewroot"
            foo_ie.parent_id = new_ie.file_id
            bzrwt.apply_inventory_delta([
                ("", None, bzrwt.path2id(''), None),
                (None, "", new_ie.file_id, new_ie),
                ("foo", "foo", foo_ie.file_id, foo_ie)])
        revid2 = bzrwt.commit(message="Commit from Bzr")
        return bzrwt, new_ie, foo_ie, revid1, revid2

    def test_change_root(self):
        repos_url = self.make_svn_repository("a")
        bzrwt, new_ie, foo_ie, revid1, revid2 = self._create_bzrwt_with_changed_root()
        newdir = ControlDir.open(repos_url+"/trunk")
        newbranch = newdir.import_branch(bzrwt.branch)
        self.assertChangedPathsEquals({u"trunk": ("R", None, -1, NODE_DIR),
                           u"trunk/foo": ("A", u"trunk/foo", 1, NODE_FILE)},
            newbranch.repository._revmeta_provider.get_revision(u"trunk", 2).metarev.paths)
        tree1 = newbranch.repository.revision_tree(revid1)
        tree2 = newbranch.repository.revision_tree(revid2)
        self.assertEquals(tree2.path2id(""), new_ie.file_id)
        self.assertEquals(tree1.path2id("foo"), foo_ie.file_id)
        self.assertEquals(tree2.path2id("foo"), foo_ie.file_id)

        self.assertEquals(bzrwt.branch.last_revision_info(),
                          newbranch.last_revision_info())

    def test_change_root_fetch(self):
        repos_url = self.make_svn_repository("a")
        (bzrwt, new_ie, foo_ie, revid1, revid2) = \
            self._create_bzrwt_with_changed_root()
        newdir = ControlDir.open(repos_url+"/trunk")
        newbranch = newdir.import_branch(bzrwt.branch)

        oldrepos = Repository.open(repos_url)
        oldrepos.set_layout(TrunkLayout(0))
        dir = ControlDir.create("f")
        newrepos = dir.create_repository()
        oldrepos.copy_content_into(newrepos)

        log = self.client_log(repos_url, 2, 0)
        self.assertEquals({u'/trunk': ('A', None, -1),
                           u'/trunk/foo': ('A', None, -1)},
                           log[1][0])
        self.assertEquals({u'/trunk': ('R', None, -1),
                           u'/trunk/foo': ('A', u'/trunk/foo', 1)}, log[2][0])

        tree1 = newrepos.revision_tree(revid1)
        tree2 = newrepos.revision_tree(revid2)
        self.assertEquals(tree2.path2id(""), new_ie.file_id)
        self.assertEquals(tree1.path2id("foo"), foo_ie.file_id)
        self.assertEquals(tree2.path2id("foo"), foo_ie.file_id)

    def test_rename_dir(self):
        repos_url = self.make_svn_repository("a")
        bzrwt = ControlDir.create_standalone_workingtree("c")
        self.build_tree({'c/registry/generic.c': b"Tour"})
        bzrwt.add("registry", "dirid")
        bzrwt.add("registry/generic.c", "origid")
        revid1 = bzrwt.commit("Add initial directory + file")
        bzrwt.rename_one("registry", "registry.moved")
        revid2 = bzrwt.commit("Rename")
        newdir = ControlDir.open(repos_url+"/trunk")
        newbranch = newdir.import_branch(bzrwt.branch)
        def check(b):
            self.assertEquals([revid1, revid2], revision_history(b))
            tree = b.repository.revision_tree(revid2)
            self.assertEquals("origid", tree.path2id("registry.moved/generic.c"))
            self.assertEquals("dirid", tree.path2id("registry.moved"))
        check(newbranch)
        os.mkdir("n")
        ControlDir.open(repos_url+"/trunk").sprout("n")
        copybranch = Branch.open("n")
        check(copybranch)

    def test_push_non_lhs_parent(self):
        repos_url = self.make_svn_repository("a")
        bzrwt = ControlDir.create_standalone_workingtree("c")
        self.build_tree({'c/registry/generic.c': b"Tour"})
        bzrwt.add("registry")
        bzrwt.add("registry/generic.c")
        revid1 = bzrwt.commit("Add initial directory + file",
                              rev_id=b"initialrevid")

        # Push first branch into Subversion
        newdir = ControlDir.open(repos_url+"/trunk")
        mapping = newdir.find_repository().get_mapping()
        newbranch = newdir.import_branch(bzrwt.branch)

        # Should create dc/trunk
        with self.get_commit_editor(repos_url) as dc:
            with dc.add_dir("branches") as branches:
                branches.add_dir('branches/foo', 'trunk')

        dc = self.get_commit_editor(repos_url)
        branches = dc.open_dir("branches")
        foo = branches.open_dir("branches/foo")
        registry = foo.open_dir("branches/foo/registry")
        registry.open_file("branches/foo/registry/generic.c").modify(b"France")
        dc.close()

        r = ra.RemoteAccess(repos_url)
        merge_revno = r.get_latest_revnum()
        merge_revid = newdir.find_repository().generate_revision_id(merge_revno, u"branches/foo", mapping)

        self.build_tree({'c/registry/generic.c': b"de"})
        revid2 = bzrwt.commit("Change something", rev_id=b"changerevid")

        # Merge
        self.build_tree({'c/registry/generic.c': b"France"})
        bzrwt.add_pending_merge(merge_revid)
        revid3 = bzrwt.commit("Merge something", rev_id=b"mergerevid")

        trunk = Branch.open(repos_url + "/branches/foo")
        self.assertRaises(AppendRevisionsOnlyViolation, trunk.pull,
            bzrwt.branch)
        trunk.set_append_revisions_only(False)
        trunk.pull(bzrwt.branch)

        self.assertEquals([revid1, revid2, revid3], revision_history(trunk))

    def test_complex_replace_dir(self):
        repos_url = self.make_svn_repository("a")
        bzrwt = ControlDir.create_standalone_workingtree("c")
        self.build_tree({'c/registry/generic.c': b"Tour"})
        bzrwt.add(["registry"], ["origdir"])
        bzrwt.add(["registry/generic.c"], ["file"])
        revid1 = bzrwt.commit("Add initial directory + file")

        bzrwt.remove('registry/generic.c')
        bzrwt.remove('registry')
        bzrwt.add(["registry"], ["newdir"])
        bzrwt.add(["registry/generic.c"], ["file"])
        revid2 = bzrwt.commit("Do some funky things")

        newdir = ControlDir.open(repos_url+"/trunk")
        newbranch = newdir.import_branch(bzrwt.branch)
        self.assertEquals(revid2, newbranch.last_revision())
        self.assertEquals([revid1, revid2], revision_history(newbranch))

        os.mkdir("n")
        ControlDir.open(repos_url+"/trunk").sprout("n")

    def test_subdir_becomes_branch_root(self):
        repos_url = self.make_svn_repository("a")

        dc = self.get_commit_editor(repos_url)
        trunk = dc.add_dir("trunk")
        subdir = trunk.add_dir("trunk/mysubdir")
        subdir.add_file("trunk/mysubdir/myfile").modify(b"blabla")
        dc.close()  #1

        os.mkdir("dc")
        svndir = ControlDir.open(repos_url+"/trunk")
        controldir = svndir.sprout("dc")
        wt = controldir.open_workingtree()
        with wt.lock_write():
            wt.apply_inventory_delta([
                ("", None, wt.path2id(''), None),
                ("mysubdir", "", wt.path2id("mysubdir"),
                    InventoryDirectory(wt.path2id("mysubdir"), "",
                        None))])
            os.rename("dc/mysubdir/myfile", "dc/myfile")
            osutils.rmtree("dc/mysubdir")
        wt.commit("Change branch root") #2
        svnbranch = svndir.open_branch()
        svnbranch.pull(wt.branch)
        self.assertEquals(svnbranch.last_revision(), wt.branch.last_revision())
        self.assertEquals(2, svndir.svn_transport.get_latest_revnum())
        self.assertEquals(["myfile"], svndir.svn_transport.list_dir("."))
        paths = svnbranch.repository._revmeta_provider._log.get_revision_paths(2)
        self.assertChangedPathEquals(('R', "trunk/mysubdir", 1, NODE_DIR),
                paths['trunk'])

    def test_push_pointless(self):
        repos_url = self.make_svn_repository("a")

        dc = self.get_commit_editor(repos_url)
        trunk = dc.add_dir("trunk")
        dc.close()

        os.mkdir("dc")
        svndir = ControlDir.open(repos_url+"/trunk")
        controldir = svndir.sprout("dc")

        wt = controldir.open_workingtree()
        with wt.lock_write():
            wt.commit("This is pointless.")
            svnbranch = svndir.open_branch()
            svnbranch.pull(wt.branch)
            self.assertEquals(svnbranch.last_revision(),
                wt.branch.last_revision())


class TestPushTwice(SubversionTestCase):

    def test_push_twice(self):
        # bug 208566
        repos_url = self.make_svn_repository('d')

        dc = self.get_commit_editor(repos_url)
        trunk = dc.add_dir("trunk")
        foo = trunk.add_dir("trunk/foo")
        foo.add_file("trunk/foo/bla").modify(b"data")
        dc.add_dir("branches")
        dc.close()

        svndir = ControlDir.open(repos_url+"/trunk")
        os.mkdir("dc")
        controldir = svndir.sprout("dc")
        wt = controldir.open_workingtree()
        revid = wt.commit(message="Commit from Bzr")
        expected_history = revision_history(wt.branch)

        svndir1 = ControlDir.open(repos_url+"/branches/a")
        svndir1.import_branch(wt.branch)
        self.assertEquals(expected_history, revision_history(svndir1.open_branch()))

        svndir2 = ControlDir.open(repos_url+"/branches/b")
        svndir2.import_branch(wt.branch)
        self.assertEquals(expected_history, revision_history(svndir2.open_branch()))

        svndir1.open_branch().pull(wt.branch)
        self.assertEquals(expected_history, revision_history(svndir1.open_branch()))
        svndir2.open_branch().pull(wt.branch)
        self.assertEquals(expected_history, revision_history(svndir2.open_branch()))


class DetermineBranchPathTests(TestCase):

    def test_no_nick_no_proj(self):
        rev = Revision("fooid")
        self.assertEquals("branches/merged",
            determine_branch_path(rev, TrunkLayout(0)))

    def test_nick_no_proj(self):
        rev = Revision("fooid")
        rev.properties['branch-nick'] = 'bla'
        self.assertEquals("branches/bla",
            determine_branch_path(rev, TrunkLayout(0)))

    def test_nick_proj(self):
        rev = Revision("fooid")
        rev.properties['branch-nick'] = 'bla'
        self.assertEquals("someproj/branches/bla",
            determine_branch_path(rev, TrunkLayout(1), "someproj"))

    def test_nick_slashes(self):
        rev = Revision("fooid")
        rev.properties['branch-nick'] = 'bla/bloe'
        self.assertEquals("someproj/branches/bla_bloe",
            determine_branch_path(rev, TrunkLayout(1), "someproj"))


class InterToSvnRepositoryTestCase(SubversionTestCase):

    def setUp(self):
        super(InterToSvnRepositoryTestCase, self).setUp()
        self.from_controldir = ControlDir.create('bzrrepo')
        self.from_repo = self.from_controldir.create_repository(shared=True)
        self.svn_repo_url = self.make_svn_repository('svnrepo')
        self.to_repo = Repository.open(self.svn_repo_url)
        self.interrepo = InterRepository.get(self.from_repo, self.to_repo)


class ForeignRevisionInfoTests(InterToSvnRepositoryTestCase):

    def test__target_has_revision(self):
        self.assertTrue(self.interrepo._target_has_revision(NULL_REVISION))
        self.assertFalse(self.interrepo._target_has_revision("foo"))
        self.interrepo._add_path_info("trunk", "myrevid", ("myuuid", "trunk", 4))
        self.assertTrue(self.interrepo._target_has_revision("myrevid"))

    def test_foreign_revision_info_null(self):
        self.assertEquals((None, None),
            self.interrepo._get_foreign_revision_info(NULL_REVISION, "apath"))

    def test_foreign_revision_info_no_path(self):
        self.interrepo._add_path_info("trunk", "myrevid", ("myuuid", "trunk", 4))
        self.assertEquals(("myuuid", "trunk", 4),
            self.interrepo._get_foreign_revision_info("myrevid"))

    def test_foreign_revision_info_available(self):
        # Prefer path if it is available
        self.interrepo._add_path_info("trunk", "myrevid", ("myuuid", "trunk", 4))
        self.interrepo._add_path_info("otherpath", "myrevid", ("myuuid", "trunk", 4))
        self.assertEquals(("myuuid", "trunk", 4),
            self.interrepo._get_foreign_revision_info("myrevid", "trunk"))

    def test_foreign_revision_info_first(self):
        # Prefer path if it is available
        self.interrepo._add_path_info("anotherpath", "myrevid", ("myuuid", "trunk1", 4))
        self.interrepo._add_path_info("otherpath", "myrevid", ("myuuid", "trunk2", 4))
        foreign_info = self.interrepo._get_foreign_revision_info("myrevid", "trunk")
        self.assertEquals("myuuid", foreign_info[0])
        self.assertEquals(4, foreign_info[2])

    def test_lookup(self):
        revids_check = []
        # If the foreign revision info is not cached it should be retrieved
        def lookup(revid):
            revids_check.append(revid)
            return ("someuuid", "somebranch", 42)
        self.to_repo.lookup_bzr_revision_id = lookup
        self.assertEquals(("someuuid", u"somebranch", 42),
            self.interrepo._get_foreign_revision_info("auuid", u"trunk"))
        self.assertEquals(["auuid"], revids_check)


class PushRevisionTests(InterToSvnRepositoryTestCase):

    def setUp(self):
        super(PushRevisionTests, self).setUp()
        branch = ControlDir.create_branch_convenience('bzrrepo/tree1')
        tree = branch.controldir.open_workingtree()
        self.build_tree_contents([('bzrrepo/tree1/a', 'data')])
        tree.add(['a'])
        self.revid1 = tree.commit('msg')

    def test_push_first_revision_with_metadata(self):
        self.to_repo.lock_write()
        self.addCleanup(self.to_repo.unlock)
        self.interrepo.push_single_revision(
            u"trunk", self.interrepo._get_branch_config("trunk"),
            self.from_repo.get_revision(self.revid1),
            push_metadata=True, base_foreign_info=(None, None),
            root_action=("create",))
        paths = self.client_log(self.svn_repo_url, 1, 0)[1][0]
        self.assertEquals(
            paths, {'/trunk': ('A', None, -1), '/trunk/a': ('A', None, -1)})
        # FIXME: Check revision properties

    def test_push_first_revision_without_metadata(self):
        self.to_repo.lock_write()
        self.addCleanup(self.to_repo.unlock)
        self.interrepo.push_single_revision(
            u"trunk", self.interrepo._get_branch_config("trunk"),
            self.from_repo.get_revision(self.revid1),
            push_metadata=False, base_foreign_info=(None, None), root_action=("create",))
        paths = self.client_log(self.svn_repo_url, 1, 0)[1][0]
        self.assertEquals(
            paths, {'/trunk': ('A', None, -1), '/trunk/a': ('A', None, -1)})
        # FIXME: Check revision properties

    def test_push_first_revision_overwrite(self):
        ce = self.get_commit_editor(self.svn_repo_url, "msg")
        ce.add_dir("trunk")
        ce.close()

        paths = self.client_log(self.svn_repo_url, 1, 0)[1][0]
        self.assertEquals(paths, {'/trunk': ('A', None, -1)})

        config = self.interrepo._get_branch_config("trunk")
        rev1 = self.from_repo.get_revision(self.revid1)

        self.to_repo.lock_write()
        self.addCleanup(self.to_repo.unlock)

        # With append revisions only disabled but overwrite it should work
        self.interrepo.push_single_revision(u"trunk", config, rev1,
            push_metadata=False, base_foreign_info=(None, None),
            root_action=("replace", 2))

        paths = self.client_log(self.svn_repo_url, 2, 0)[2][0]
        self.assertEquals(paths,
            {u'/trunk': ('R', None, -1), u'/trunk/a': ('A', None, -1)})

    def test_push_first_revision_append_revisions_only(self):
        ce = self.get_commit_editor(self.svn_repo_url, "msg")
        ce.add_dir("trunk")
        ce.close()

        paths = self.client_log(self.svn_repo_url, 1, 0)[1][0]
        self.assertEquals(paths, {u'/trunk': ('A', None, -1)})

        config = self.interrepo._get_branch_config("trunk")
        rev1 = self.from_repo.get_revision(self.revid1)

        self.to_repo.lock_write()
        self.addCleanup(self.to_repo.unlock)

        self.interrepo.push_single_revision(
            u"trunk", config, rev1,
            push_metadata=False, base_foreign_info=(None, None),
            root_action=("replace", 1))

        paths = self.client_log(self.svn_repo_url, 2, 0)[2][0]
        self.assertEquals(
            paths, {'/trunk': ('R', None, -1), '/trunk/a': ('A', None, -1)})


class PushRevisionInclusiveTests(InterToSvnRepositoryTestCase):

    # revid1    revid2
    #   \       /
    #  revid_merge

    def setUp(self):
        super(PushRevisionInclusiveTests, self).setUp()
        branch = ControlDir.create_branch_convenience('bzrrepo/tree1')
        self.tree1 = branch.controldir.open_workingtree()
        self.build_tree_contents([('bzrrepo/tree1/a', 'data')])
        self.tree1.add(['a'])
        self.revid1 = self.tree1.commit('msg1')

        branch = ControlDir.create_branch_convenience('bzrrepo/tree2')
        self.tree2 = branch.controldir.open_workingtree()
        self.build_tree_contents([('bzrrepo/tree2/b', 'data')])
        self.tree2.add(['b'])
        self.revid2 = self.tree2.commit('msg2')

        self.tree1.merge_from_branch(self.tree2.branch, from_revision=NULL_REVISION,
                to_revision=self.revid2)
        self.revid_merge = self.tree1.commit("merge")

    def test_push_no_merged(self):
        config = self.interrepo._get_branch_config("trunk")
        self.addCleanup(self.from_repo.unlock)
        self.from_repo.lock_read()
        self.to_repo.lock_write()
        self.addCleanup(self.to_repo.unlock)
        rev1 = self.from_repo.get_revision(self.revid1)
        self.interrepo.push_single_revision(u"trunk",
            config, rev1, push_metadata=True, root_action=("create", ),
            base_foreign_info=(None, None))
        rev_merged = self.from_repo.get_revision(self.revid_merge)
        foreign_rev_info = self.interrepo._get_foreign_revision_info(
            rev_merged.parent_ids[0])
        self.interrepo.push_revision_inclusive(u"trunk", config,
            rev_merged, push_merged=False,
            layout=TrunkLayout0(), project="",
            push_metadata=True, root_action=("open", self.to_repo.get_latest_revnum()),
            base_foreign_info=foreign_rev_info)
        log = self.client_log(self.svn_repo_url, 2, 0)
        self.assertEquals(log[1][0],
            {u'/trunk': ('A', None, -1), u'/trunk/a': ('A', None, -1)})
        self.assertEquals(log[2][0],
            {u'/trunk': ('M', None, -1), u'/trunk/b': ('A', None, -1)})

    def test_push_merged(self):
        config = self.interrepo._get_branch_config("trunk")
        self.addCleanup(self.from_repo.unlock)
        self.from_repo.lock_read()
        self.to_repo.lock_write()
        self.addCleanup(self.to_repo.unlock)
        rev1 = self.from_repo.get_revision(self.revid1)
        self.interrepo.push_single_revision(u"trunk",
            config, rev1, push_metadata=True, root_action=("create", ),
            base_foreign_info=(None, None))
        rev_merged = self.from_repo.get_revision(self.revid_merge)
        self.interrepo.push_revision_inclusive(u"trunk", config,
            rev_merged, push_merged=True,
            layout=TrunkLayout0(), project="",
            push_metadata=True, root_action=("open",  self.to_repo.get_latest_revnum()),
            base_foreign_info=self.interrepo._get_foreign_revision_info(
                rev_merged.parent_ids[0]))
        log = self.client_log(self.svn_repo_url, 4, 0)
        self.assertEquals(log[1][0],
            {u'/trunk': ('A', None, -1), u'/trunk/a': ('A', None, -1)})
        self.assertEquals(log[2][0], {u'/branches': ('A', None, -1)})
        self.assertEquals(log[2][3], "Add branches directory.")
        self.assertEquals(log[3][3], 'msg2')
        self.assertEquals(log[3][0],
            {u'/branches/tree2': ('A', None, -1), u'/branches/tree2/b': ('A', None, -1)})
        self.assertEquals(log[4][0],
            {u'/trunk': ('M', None, -1),
             u'/trunk/b': ('A', u'/branches/tree2/b', 3)})
        self.assertEquals(log[4][3], 'merge')

    def test_push_merged_again(self):
        # Push a branch with a merge, then later push another branch with a
        # merge.
        config = self.interrepo._get_branch_config("trunk")
        self.addCleanup(self.from_repo.unlock)
        self.from_repo.lock_read()
        self.to_repo.lock_write()
        self.addCleanup(self.to_repo.unlock)
        rev1 = self.from_repo.get_revision(self.revid1)
        self.interrepo.push_single_revision(u"trunk",
            config, rev1, push_metadata=True, root_action=("create", ),
            base_foreign_info=(None, None))
        rev_merged = self.from_repo.get_revision(self.revid_merge)
        self.interrepo.push_revision_inclusive(u"trunk", config,
            rev_merged, push_merged=True,
            layout=TrunkLayout0(), project="",
            push_metadata=True, root_action=("open",  self.to_repo.get_latest_revnum()),
            base_foreign_info=self.interrepo._get_foreign_revision_info(rev_merged.parent_ids[0]))

        revid3 = self.tree2.commit('msg2 again')
        self.tree1.merge_from_branch(self.tree2.branch, to_revision=revid3)
        revid_merge = self.tree1.commit("merge again")
        self.from_repo.unlock()
        self.from_repo.lock_read()
        rev_merged = self.from_repo.get_revision(revid_merge)
        self.interrepo.push_revision_inclusive(u"trunk", config,
            rev_merged, push_merged=True,
            layout=TrunkLayout0(), project="",
            push_metadata=True, root_action=("open",  self.to_repo.get_latest_revnum()),
            base_foreign_info=self.interrepo._get_foreign_revision_info(rev_merged.parent_ids[0]))

        log = self.client_log(self.svn_repo_url, 6, 4)
        self.assertEquals(log[5][3], 'msg2 again')
        self.assertEquals(log[5][0], {u'/branches/tree2': ('M', None, -1)})
        self.assertEquals(log[6][0], {u'/trunk': ('M', None, -1)})
        self.assertEquals(log[6][3], 'merge again')


class CreateBranchContainerTests(SubversionTestCase):

    def setUp(self):
        super(CreateBranchContainerTests, self).setUp()
        self.repos_url = self.make_svn_repository("d")
        self.repo = Repository.open(self.repos_url)

    def test_create_single(self):
        create_branch_container(self.repo.svn_transport, "branches/abranch", "")
        log = self.client_log(self.repos_url, 1, 0)
        self.assertEquals(log[1][0], {u'/branches': ('A', None, -1)})
        self.assertEquals(log[1][3], "Add branches directory.")

    def test_create_double(self):
        create_branch_container(self.repo.svn_transport, u"project/branches/abranch", "")
        paths = self.client_log(self.repos_url, 1, 0)[1][0]
        self.assertEquals(paths,
            {u'/project': ('A', None, -1), u'/project/branches': ('A', None, -1)})

    def test_part_exists(self):
        dc = self.get_commit_editor(self.repos_url)
        dc.add_dir("project")
        dc.close()
        create_branch_container(self.repo.svn_transport, u"project/branches/abranch", u"project")
        paths = self.client_log(self.repos_url, 2, 0)[2][0]
        self.assertEquals(paths, {u'/project/branches': ('A', None, -1)})
