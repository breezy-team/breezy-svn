# Tests for repository

# Copyright (C) 2010 Jelmer Vernooij <jelmer@samba.org>

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from subvertpy import (
    NODE_DIR,
    NODE_FILE,
    )

from breezy.repository import Repository

from breezy.tests import TestCase

from breezy.plugins.svn.layout.standard import (
    CustomLayout,)
from breezy.plugins.svn.tests import SubversionTestCase
from breezy.plugins.svn.layout.standard import (
    RootLayout,
    TrunkLayout,
    )
from breezy.plugins.svn.logwalker import (
    DictBasedLogWalker,
    )
from breezy.plugins.svn.metagraph import (
    MetaRevision,
    filter_revisions,
    restrict_prefixes,
    )
from breezy.plugins.svn.metagraph import (
    RevisionMetadataBrowser,
    )



class TestMetaRevisionGraph(SubversionTestCase):
    """Mapping-dependent tests for Subversion repositories."""

    def test_iter_changes_parent_rename(self):
        repos_url = self.make_svn_repository("a")

        dc = self.get_commit_editor(repos_url)
        foo = dc.add_dir(u"foo")
        foo.add_dir(u"foo/bar")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.add_dir(u"bla", "foo", 1)
        dc.close()

        repos = Repository.open(repos_url)
        repos.set_layout(CustomLayout([u"bla/bar"]))
        ret = list(repos._revmeta_provider._graph.iter_changes(u'bla/bar', 2, 0))
        self.assertEquals(1, len(ret))
        self.assertEquals("foo/bar", ret[0][0])


class FilterRevisionsTests(TestCase):

    def test_simple(self):
        self.assertEquals([1, 5],
                list(filter_revisions(iter([("revision", 1), ("branch", None), ("revision", 5)]))))


class RestrictPrefixesTests(TestCase):

    def test_root(self):
        self.assertEquals(set(["a", "b/c"]), restrict_prefixes(["a", "b/c"], ""))

    def test_prefix_cuts_off(self):
        self.assertEquals(set(["a"]), restrict_prefixes(["a", "b/c"], "a"))

    def test_prefix_restricts(self):
        self.assertEquals(set(["a/d"]), restrict_prefixes(["a", "b/c"], "a/d"))


class FakeRevision(MetaRevision):

    def __init__(self, path, revnum):
        super(FakeRevision, self).__init__(None, "mock-uuid", path, revnum)


class MetadataBrowserTests(TestCase):

    def setUp(self):
        TestCase.setUp(self)

    def get_browser(self, prefixes, from_revnum, to_revnum, layout, paths):
        revprops = {}
        for revnum in paths:
            revprops[revnum] = { "svn:log": "Revision %d" % revnum }
        self._log = DictBasedLogWalker(paths, revprops)
        class MockTransport(object):
            def get_uuid(self):
                return "mock-uuid"
        self._log._transport = MockTransport()
        return RevisionMetadataBrowser(prefixes, from_revnum, to_revnum, layout, self)

    def test_root_layout_simple(self):
        browser = self.get_browser(None, 1, 0, RootLayout(),
                { 1: { u"bla": ('A', None, -1, NODE_DIR)}})
        rev1 = next(browser)
        self.assertEquals(('revision', FakeRevision(u'', 1)), rev1)
        rev2 = next(browser)
        self.assertEquals(('revision', FakeRevision(u'', 0)), rev2)
        self.assertTrue(rev1[1]._lhs_parent_known)
        self.assertTrue(rev2[1]._lhs_parent_known)
        self.assertRaises(StopIteration, next, browser)

    def test_trunk_layout_simple(self):
        browser = self.get_browser(None, 2, 0, TrunkLayout(),
                { 1: { u"trunk": ('A', None, -1, NODE_DIR)},
                  2: { u"trunk": ('M', None, -1, NODE_DIR)}})
        rev1 = next(browser)
        self.assertEquals(('revision', FakeRevision(u'trunk',2)), rev1)
        rev2 = next(browser)
        self.assertEquals(('revision', FakeRevision(u'trunk',1)), rev2)
        self.assertTrue(rev1[1]._lhs_parent_known)
        self.assertTrue(rev2[1]._lhs_parent_known)
        self.assertRaises(StopIteration, next, browser)

    def test_trunk_layout_movefrom_non_branch(self):
        browser = self.get_browser(None, 2, 0, TrunkLayout(),
                { 1: { u"old-trunk": ('A', None, -1, NODE_DIR)},
                  2: { u"trunk": ('A', u"old-trunk", 1, NODE_DIR)}})
        rev1 = next(browser)
        self.assertEquals(('revision', FakeRevision(u'trunk',2)), rev1)
        rev2 = next(browser)
        self.assertEquals(('revision', FakeRevision(u'old-trunk',1)), rev2)
        self.assertTrue(rev1[1]._lhs_parent_known)
        self.assertTrue(rev2[1]._lhs_parent_known)
        self.assertRaises(StopIteration, next, browser)

    def test_trunk_layout_movefrom_oldbranch(self):
        browser = self.get_browser(None, 3, 0, TrunkLayout(),
                { 1: { u"old-trunk": ('A', None, -1, NODE_DIR)},
                  2: { u"old-trunk": ('D', None, -1, NODE_DIR)},
                  3: { u"trunk": ('A', u"old-trunk", 1, NODE_DIR)}})
        rev1 = next(browser)
        self.assertEquals(('revision', FakeRevision(u'trunk', 3)), rev1)
        self.assertEquals(('delete', (u"old-trunk", 2)), next(browser))
        rev2 = next(browser)
        self.assertEquals(('revision', FakeRevision(u'old-trunk', 1)), rev2)
        self.assertTrue(rev1[1]._lhs_parent_known)
        self.assertTrue(rev2[1]._lhs_parent_known)
        self.assertRaises(StopIteration, next, browser)

    def test_trunk_layout_copiedbranch(self):
        browser = self.get_browser(None, 2, 0, TrunkLayout(),
                {1: {u"trunk": ('A', None, -1, NODE_DIR)},
                 2: {u"branches": ('A', None, -1, NODE_DIR),
                     u"branches/foo": ('A', u"trunk", 1, NODE_DIR)}})
        rev1 = next(browser)
        self.assertEquals(('revision', FakeRevision(u'branches/foo', 2)), rev1)
        rev2 = next(browser)
        self.assertEquals(('revision', FakeRevision(u'trunk', 1)), rev2)
        self.assertTrue(rev1[1]._lhs_parent_known)
        self.assertTrue(rev2[1]._lhs_parent_known)
        self.assertRaises(StopIteration, browser.next)

    def test_subdir_becomes_branch_root(self):
        browser = self.get_browser(None, 2, 0, TrunkLayout(), {
                 1: {u"trunk": ('A', None, -1, NODE_DIR),
                     u"trunk/mysubdir": ('A', None, -1, NODE_DIR),
                     u"trunk/mysubdir/myfile": ('A', None, -1, NODE_FILE)},
                 2: {u"trunk": ('R', u"trunk/mysubdir", 1, NODE_DIR) }})
        self.assertEquals(('delete', (u'trunk', 2)), next(browser))
        rev1 = next(browser)
        self.assertEquals(('revision', FakeRevision(u'trunk',2)), rev1)
        rev2 = next(browser)
        self.assertEquals(('revision', FakeRevision(u'trunk/mysubdir',1)), rev2)
        rev3 = next(browser)
        self.assertEquals(('revision', FakeRevision(u'trunk',1)), rev3)
        self.assertTrue(rev1[1]._lhs_parent_known)
        self.assertTrue(rev2[1]._lhs_parent_known)
        self.assertTrue(rev3[1]._lhs_parent_known)
        self.assertRaises(StopIteration, next, browser)

    def test_copyfrom_revnum_skipped(self):
        browser = self.get_browser([u"python"], 4, 0, TrunkLayout(1),
                { 1: { u"python": ('A', None, -1, NODE_DIR),
                       u"python/tags": ('A', None, -1, NODE_DIR),
                       u"python/trunk": ('A', None, -1, NODE_DIR)},
                  2: { u"foo": ('A', None, -1, NODE_DIR) },
                  3: { u"bar": ('A', None, -1, NODE_DIR) },
                  4: { u"python/tags/bla": ('A', u'python/trunk', 2, NODE_DIR)}})
        rev1 = next(browser)
        self.assertEquals(('revision', FakeRevision(u'python/tags/bla',4)), rev1)
        rev2 = next(browser)
        self.assertEquals(('revision', FakeRevision(u'python/trunk',1)), rev2)
        self.assertRaises(StopIteration, next, browser)
        self.assertTrue(rev1[1]._lhs_parent_known)

    def test_chaco(self):
        rev3 = { u"packages/chaco2/trunk/debian/rules": ("M", None, -1, NODE_FILE)}
        rev4 = { u"packages/chaco2": ("D", None, -1, NODE_DIR),
              u"packages/enthought-chaco2": ("A", u"packages/chaco2", 3, NODE_DIR),
              u"packages/enthought-chaco2/trunk": ("D", None, -1, NODE_DIR)}
        rev5 = { u"packages/enthought-chaco2/trunk": ("A", None, -1, NODE_DIR),
                u"packages/enthought-chaco2/trunk/debian": ("A", None, -1, NODE_DIR)}
        browser = self.get_browser([u"packages"], 5, 3, TrunkLayout(2),
                { 3: rev3, 4: rev4, 5: rev5 })
        rev1 = next(browser)
        self.assertEquals(('revision',
            FakeRevision(u'packages/enthought-chaco2/trunk',5)), rev1)
        rev2 = next(browser)
        self.assertEquals(('delete', (u'packages/enthought-chaco2/trunk', 4)), rev2)
        rev3 = next(browser)
        self.assertEquals(('delete', (u'packages/enthought-chaco2/trunk', 4)), rev3)
        rev4 = next(browser)
        self.assertEquals(('revision', FakeRevision(u'packages/chaco2/trunk',3)), rev4)
        self.assertRaises(StopIteration, next, browser)
        self.assertTrue(rev1[1]._lhs_parent_known)
        self.assertFalse(rev4[1]._lhs_parent_known)

    def test_follow_prefixes(self):
        rev1 = { u"foo": ('A', None, -1, NODE_DIR),
                 u"foo/trunk": ('A', None, -1, NODE_DIR) }
        rev2 = { u"bar": ('A', u'foo', 1, NODE_DIR) }
        rev3 = { u"bar/trunk": ('M', None, -1, NODE_DIR) }
        browser = self.get_browser([u"bar"], 4, 0, TrunkLayout(1),
                { 1: rev1, 2: rev2, 3: rev3 })
        rev1 = next(browser)
        self.assertEquals(('revision', FakeRevision(u'bar/trunk', 3)), rev1)
        rev2 = next(browser)
        self.assertEquals(('revision', FakeRevision(u'foo/trunk', 1)), rev2)
        self.assertRaises(StopIteration, next, browser)
        self.assertTrue(rev1[1]._lhs_parent_known)
        self.assertTrue(rev2[1]._lhs_parent_known)

    def test_pointless_root_commit(self):
        rev1 = { u"foo": ('A', None, -1, NODE_DIR) }
        rev2 = {}
        rev3 = { u"bar": ('A', None, -1, NODE_DIR) }
        browser = self.get_browser([u""], 3, 0, RootLayout(),
                { 1: rev1, 2: rev2, 3: rev3 })
        rev1 = next(browser)
        self.assertEquals(("revision", FakeRevision(u"", 3)), rev1)
        rev2 = next(browser)
        self.assertEquals(("revision", FakeRevision(u"", 2)), rev2)
        rev3 = next(browser)
        self.assertEquals(("revision", FakeRevision(u"", 1)), rev3)
        rev4 = next(browser)
        self.assertEquals(("revision", FakeRevision(u"", 0)), rev4)

    def test_ignore_sideeffects(self):
        browser = self.get_browser([u"python"], 8, 0, TrunkLayout(1),
                { 1: { u"python": ('A', None, -1, NODE_DIR),
                       u"python/tags": ('A', None, -1, NODE_DIR),
                       u"python/trunk": ('A', None, -1, NODE_DIR)},
                  5: { u"python/trunk/bar": ('A', None, -1, NODE_FILE),
                       u"something/trunk": ("M", None ,-1, NODE_DIR)},
                  6: { u"python/branches/bla": ("A", u"something/trunk", 5, NODE_DIR) }
                  })
        rev1 = next(browser)
        self.assertEquals(('revision', FakeRevision(u'python/branches/bla',6)), rev1)
        rev2 = next(browser)
        self.assertEquals(('revision', FakeRevision(u'python/trunk',5)), rev2)
        rev3 = next(browser)
        self.assertEquals(('revision', FakeRevision(u'python/trunk',1)), rev3)
        self.assertRaises(StopIteration, next, browser)
        self.assertFalse(rev1[1]._lhs_parent_known)
        self.assertTrue(rev2[1]._lhs_parent_known)
        self.assertTrue(rev3[1]._lhs_parent_known)
