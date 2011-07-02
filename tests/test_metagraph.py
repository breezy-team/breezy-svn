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

from bzrlib.repository import Repository

from bzrlib.tests import TestCase

from bzrlib.plugins.svn.layout.standard import (
    CustomLayout,)
from bzrlib.plugins.svn.tests import SubversionTestCase
from bzrlib.plugins.svn.layout.standard import (
    RootLayout,
    TrunkLayout,
    )
from bzrlib.plugins.svn.logwalker import (
    DictBasedLogWalker,
    )
from bzrlib.plugins.svn.metagraph import (
    filter_revisions,
    restrict_prefixes,
    )
from bzrlib.plugins.svn.metagraph import (
    RevisionMetadataBrowser,
    )



class TestMetaRevisionGraph(SubversionTestCase):
    """Mapping-dependent tests for Subversion repositories."""

    def test_iter_changes_parent_rename(self):
        repos_url = self.make_repository("a")

        dc = self.get_commit_editor(repos_url)
        foo = dc.add_dir("foo")
        foo.add_dir("foo/bar")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("bla", "foo", 1)
        dc.close()

        repos = Repository.open(repos_url)
        repos.set_layout(CustomLayout(["bla/bar"]))
        ret = list(repos._revmeta_provider._graph.iter_changes('bla/bar', 2, 0))
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


class FakeRevision(object):

    def __init__(self, path, revnum):
        self.branch_path = path
        self.revnum = revnum
        self._parent_revmeta_set = False
        self._direct_lhs_parent_incomplete = None
        self.parent_revmeta = None
        self.children = set()

    def _set_direct_lhs_parent_revmeta(self, revmeta):
        assert not self._parent_revmeta_set or revmeta == self.parent_revmeta, \
                "%r != %r for %r" % (self.parent_revmeta, revmeta, self)
        self._parent_revmeta_set = True
        self.parent_revmeta = revmeta
        if revmeta is not None:
            revmeta.children.add(self)

    def __repr__(self):
        return "FakeRevision(%r,%r)" % (self.branch_path, self.revnum)

    def __eq__(self, other):
        return (type(self) == type(other) and
                self.branch_path == other.branch_path and
                self.revnum == other.revnum)


class MetadataBrowserTests(TestCase):

    def setUp(self):
        TestCase.setUp(self)
        self._get_revs = []

    def get_revision(self, path, revnum, changes=None, revprops=None,
                     changed_fileprops=None, fileprops=None, metaiterator=None):
        self._get_revs.append((path, revnum))
        return FakeRevision(path, revnum)

    def get_browser(self, prefixes, from_revnum, to_revnum, layout, paths):
        revprops = {}
        for revnum in paths:
            revprops[revnum] = { "svn:log": "Revision %d" % revnum }
        self._log = DictBasedLogWalker(paths, revprops)
        return RevisionMetadataBrowser(prefixes, from_revnum, to_revnum, layout, self)

    def test_root_layout_simple(self):
        browser = self.get_browser(None, 1, 0, RootLayout(),
                { 1: { "bla": ('A', None, -1, NODE_DIR)}})
        rev1 = browser.next()
        self.assertEquals(('revision', FakeRevision('',1)), rev1)
        rev2 = browser.next()
        self.assertEquals(('revision', FakeRevision('',0)), rev2)
        self.assertTrue(rev1[1]._parent_revmeta_set)
        self.assertTrue(rev2[1]._parent_revmeta_set)
        self.assertRaises(StopIteration, browser.next)

    def test_trunk_layout_simple(self):
        browser = self.get_browser(None, 2, 0, TrunkLayout(),
                { 1: { "trunk": ('A', None, -1, NODE_DIR)},
                  2: { "trunk": ('M', None, -1, NODE_DIR)}})
        rev1 = browser.next()
        self.assertEquals(('revision', FakeRevision('trunk',2)), rev1)
        rev2 = browser.next()
        self.assertEquals(('revision', FakeRevision('trunk',1)), rev2)
        self.assertTrue(rev1[1]._parent_revmeta_set)
        self.assertTrue(rev2[1]._parent_revmeta_set)
        self.assertRaises(StopIteration, browser.next)

    def test_trunk_layout_movefrom_non_branch(self):
        browser = self.get_browser(None, 2, 0, TrunkLayout(),
                { 1: { "old-trunk": ('A', None, -1, NODE_DIR)},
                  2: { "trunk": ('A', "old-trunk", 1, NODE_DIR)}})
        rev1 = browser.next()
        self.assertEquals(('revision', FakeRevision('trunk',2)), rev1)
        rev2 = browser.next()
        self.assertEquals(('revision', FakeRevision('old-trunk',1)), rev2)
        self.assertTrue(rev1[1]._parent_revmeta_set)
        self.assertTrue(rev2[1]._parent_revmeta_set)
        self.assertRaises(StopIteration, browser.next)

    def test_trunk_layout_movefrom_oldbranch(self):
        browser = self.get_browser(None, 3, 0, TrunkLayout(),
                { 1: { "old-trunk": ('A', None, -1, NODE_DIR)},
                  2: { "old-trunk": ('D', None, -1, NODE_DIR)},
                  3: { "trunk": ('A', "old-trunk", 1, NODE_DIR)}})
        rev1 = browser.next()
        self.assertEquals(('revision', FakeRevision('trunk',3)), rev1)
        self.assertEquals(('delete', ("old-trunk", 2)), browser.next())
        rev2 = browser.next()
        self.assertEquals(('revision', FakeRevision('old-trunk',1)), rev2)
        self.assertTrue(rev1[1]._parent_revmeta_set)
        self.assertTrue(rev2[1]._parent_revmeta_set)
        self.assertRaises(StopIteration, browser.next)

    def test_trunk_layout_copiedbranch(self):
        browser = self.get_browser(None, 2, 0, TrunkLayout(),
                { 1: { "trunk": ('A', None, -1, NODE_DIR)},
                  2: { "branches": ('A', None, -1, NODE_DIR),
                       "branches/foo": ('A', "trunk", 1, NODE_DIR)}})
        rev1 = browser.next()
        self.assertEquals(('revision', FakeRevision('branches/foo',2)), rev1)
        rev2 = browser.next()
        self.assertEquals(('revision', FakeRevision('trunk',1)), rev2)
        self.assertTrue(rev1[1]._parent_revmeta_set)
        self.assertTrue(rev2[1]._parent_revmeta_set)
        self.assertRaises(StopIteration, browser.next)

    def test_subdir_becomes_branch_root(self):
        browser = self.get_browser(None, 2, 0, TrunkLayout(),
                { 1: { "trunk": ('A', None, -1, NODE_DIR),
                       "trunk/mysubdir": ('A', None, -1, NODE_DIR),
                       "trunk/mysubdir/myfile": ('A', None, -1, NODE_FILE)},
                  2: { "trunk": ('R', "trunk/mysubdir", 1, NODE_DIR) }})
        self.assertEquals(('delete', ('trunk', 2)), browser.next())
        rev1 = browser.next()
        self.assertEquals(('revision', FakeRevision('trunk',2)), rev1)
        rev2 = browser.next()
        self.assertEquals(('revision', FakeRevision('trunk/mysubdir',1)), rev2)
        rev3 = browser.next()
        self.assertEquals(('revision', FakeRevision('trunk',1)), rev3)
        self.assertTrue(rev1[1]._parent_revmeta_set)
        self.assertTrue(rev2[1]._parent_revmeta_set)
        self.assertTrue(rev3[1]._parent_revmeta_set)
        self.assertRaises(StopIteration, browser.next)

    def test_copyfrom_revnum_skipped(self):
        browser = self.get_browser(["python"], 4, 0, TrunkLayout(1),
                { 1: { "python": ('A', None, -1, NODE_DIR),
                       "python/tags": ('A', None, -1, NODE_DIR),
                       "python/trunk": ('A', None, -1, NODE_DIR)},
                  2: { "foo": ('A', None, -1, NODE_DIR) },
                  3: { "bar": ('A', None, -1, NODE_DIR) },
                  4: { "python/tags/bla": ('A', 'python/trunk', 2, NODE_DIR)}})
        rev1 = browser.next()
        self.assertEquals(('revision', FakeRevision('python/tags/bla',4)), rev1)
        rev2 = browser.next()
        self.assertEquals(('revision', FakeRevision('python/trunk',1)), rev2)
        self.assertRaises(StopIteration, browser.next)
        self.assertTrue(rev1[1]._parent_revmeta_set)

    def test_chaco(self):
        rev3 = { "packages/chaco2/trunk/debian/rules": ("M", None, -1, NODE_FILE)}
        rev4 = { "packages/chaco2": ("D", None, -1, NODE_DIR),
              "packages/enthought-chaco2": ("A", "packages/chaco2", 3, NODE_DIR),
              "packages/enthought-chaco2/trunk": ("D", None, -1, NODE_DIR)}
        rev5 = { "packages/enthought-chaco2/trunk": ("A", None, -1, NODE_DIR),
                "packages/enthought-chaco2/trunk/debian": ("A", None, -1, NODE_DIR)}
        browser = self.get_browser(["packages"], 5, 3, TrunkLayout(2),
                { 3: rev3, 4: rev4, 5: rev5 })
        rev1 = browser.next()
        self.assertEquals(('revision',
            FakeRevision('packages/enthought-chaco2/trunk',5)), rev1)
        rev2 = browser.next()
        self.assertEquals(('delete', ('packages/enthought-chaco2/trunk', 4)), rev2)
        rev3 = browser.next()
        self.assertEquals(('delete', ('packages/enthought-chaco2/trunk', 4)), rev3)
        rev4 = browser.next()
        self.assertEquals(('revision', FakeRevision('packages/chaco2/trunk',3)), rev4)
        self.assertRaises(StopIteration, browser.next)
        self.assertTrue(rev1[1]._parent_revmeta_set)
        self.assertFalse(rev4[1]._parent_revmeta_set)

    def test_follow_prefixes(self):
        rev1 = { "foo": ('A', None, -1, NODE_DIR),
                 "foo/trunk": ('A', None, -1, NODE_DIR) }
        rev2 = { "bar": ('A', 'foo', 1, NODE_DIR) }
        rev3 = { "bar/trunk": ('M', None, -1, NODE_DIR) }
        browser = self.get_browser(["bar"], 4, 0, TrunkLayout(1),
                { 1: rev1, 2: rev2, 3: rev3 })
        rev1 = browser.next()
        self.assertEquals(('revision', FakeRevision('bar/trunk', 3)), rev1)
        rev2 = browser.next()
        self.assertEquals(('revision', FakeRevision('foo/trunk', 1)), rev2)
        self.assertRaises(StopIteration, browser.next)
        self.assertTrue(rev1[1]._parent_revmeta_set)
        self.assertTrue(rev2[1]._parent_revmeta_set)

    def test_pointless_root_commit(self):
        rev1 = { "foo": ('A', None, -1, NODE_DIR) }
        rev2 = {}
        rev3 = { "bar": ('A', None, -1, NODE_DIR) }
        browser = self.get_browser([""], 3, 0, RootLayout(),
                { 1: rev1, 2: rev2, 3: rev3 })
        rev1 = browser.next()
        self.assertEquals(("revision", FakeRevision("", 3)), rev1)
        rev2 = browser.next()
        self.assertEquals(("revision", FakeRevision("", 2)), rev2)
        rev3 = browser.next()
        self.assertEquals(("revision", FakeRevision("", 1)), rev3)
        rev4 = browser.next()
        self.assertEquals(("revision", FakeRevision("", 0)), rev4)

    def test_ignore_sideeffects(self):
        browser = self.get_browser(["python"], 8, 0, TrunkLayout(1),
                { 1: { "python": ('A', None, -1, NODE_DIR),
                       "python/tags": ('A', None, -1, NODE_DIR),
                       "python/trunk": ('A', None, -1, NODE_DIR)},
                  5: { "python/trunk/bar": ('A', None, -1, NODE_FILE),
                       "something/trunk": ("M", None ,-1, NODE_DIR)},
                  6: { "python/branches/bla": ("A", "something/trunk", 5, NODE_DIR) }
                  })
        rev1 = browser.next()
        self.assertEquals(('revision', FakeRevision('python/branches/bla',6)), rev1)
        rev2 = browser.next()
        self.assertEquals(('revision', FakeRevision('python/trunk',5)), rev2)
        rev3 = browser.next()
        self.assertEquals(('revision', FakeRevision('python/trunk',1)), rev3)
        self.assertRaises(StopIteration, browser.next)
        self.assertFalse(rev1[1]._parent_revmeta_set)
        self.assertTrue(rev2[1]._parent_revmeta_set)
        self.assertTrue(rev3[1]._parent_revmeta_set)
        self.assertEquals([('python/branches/bla', 6), ('python/trunk', 5), ('python/trunk', 1)], self._get_revs)
