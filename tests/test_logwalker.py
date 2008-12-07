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

"""Log walker tests."""

from bzrlib.errors import NoSuchRevision
from bzrlib.tests import TestCase

import os
from bzrlib import debug

from bzrlib.plugins.svn import logwalker
from bzrlib.plugins.svn.tests import SubversionTestCase
from bzrlib.plugins.svn.transport import SvnRaTransport

class TestLogWalker(SubversionTestCase):
    def setUp(self):
        super(TestLogWalker, self).setUp()
        debug.debug_flags.add("transport")

    def get_log_walker(self, transport):
        return logwalker.LogWalker(transport)

    def test_create(self):
        repos_url = self.make_repository("a")
        self.get_log_walker(transport=SvnRaTransport(repos_url))

    def test_get_branch_log(self):
        repos_url = self.make_repository("a")
        cb = self.get_commit_editor(repos_url)
        cb.add_file("foo").modify()
        cb.close()

        walker = self.get_log_walker(transport=SvnRaTransport(repos_url))

        self.assertEqual(2, len(list(walker.iter_changes(None, 1))))

    def test_get_branch_follow_branch(self):
        repos_url = self.make_repository("a")
        cb = self.get_commit_editor(repos_url)
        cb.add_dir("trunk").close()
        cb.close()

        cb = self.get_commit_editor(repos_url)
        cb.add_dir("branches").close()
        cb.close()
        
        cb = self.get_commit_editor(repos_url)
        cb.add_dir("branches/foo", "trunk").close()
        cb.close()

        walker = self.get_log_walker(transport=SvnRaTransport(repos_url))

        self.assertEqual(2, len(list(walker.iter_changes(["branches/foo"], 3))))

    def test_get_branch_follow_branch_changing_parent(self):
        repos_url = self.make_repository("a")
        cb = self.get_commit_editor(repos_url)
        d = cb.add_dir("trunk")
        d.add_file("trunk/foo").modify()
        d.close()
        cb.close()

        cb = self.get_commit_editor(repos_url)
        cb.add_dir("branches").close()
        cb.close()

        cb = self.get_commit_editor(repos_url)
        cb.add_dir("branches/abranch", "trunk").close()
        cb.close()

        walker = self.get_log_walker(transport=SvnRaTransport(repos_url))

        self.assertEquals([
            ({"branches/abranch": ('A', 'trunk', 2)}, 3),
            ({"trunk/foo": ('A', None, -1), 
                           "trunk": ('A', None, -1)}, 1)
            ], [l[:2] for l in walker.iter_changes(["branches/abranch/foo"], 3)])

    def test_get_branch_invalid_revision(self):
        repos_url = self.make_repository("a")
        walker = self.get_log_walker(transport=SvnRaTransport(repos_url))
        self.assertRaises(NoSuchRevision, list, 
                          walker.iter_changes(["/"], 20))

    def test_branch_log_all(self):
        repos_url = self.make_repository("a")
        cb = self.get_commit_editor(repos_url)
        d = cb.add_dir("trunk")
        d.add_file("trunk/file").modify()
        d = cb.add_dir("foo")
        d.add_file("foo/file").modify()
        cb.close()

        walker = self.get_log_walker(transport=SvnRaTransport(repos_url))

        self.assertEqual(2, len(list(walker.iter_changes([""], 1))))

    def test_branch_log_specific(self):
        repos_url = self.make_repository("a")
        cb = self.get_commit_editor(repos_url)
        d = cb.add_dir("branches")
        ba = d.add_dir("branches/brancha")
        ba.add_file("branches/brancha/data").modify()
        d.add_dir("branches/branchb")
        bab = d.add_dir("branches/branchab")
        bab.add_file("branches/branchab/data").modify()
        cb.close()

        walker = self.get_log_walker(transport=SvnRaTransport(repos_url))

        self.assertEqual(1, len(list(walker.iter_changes(["branches/brancha"],
            1))))

    def test_iter_changes_ignore_unchanged(self):
        repos_url = self.make_repository("a")
        cb = self.get_commit_editor(repos_url)
        b = cb.add_dir("branches")
        ba = b.add_dir("branches/brancha")
        ba.add_file("branches/brancha/data").modify()
        b.add_dir("branches/branchab")
        cb.close()

        cb = self.get_commit_editor(repos_url)
        bs = cb.open_dir("branches")
        bab = bs.open_dir("branches/branchab")
        bab.add_file("branches/branchab/data").modify()
        cb.close()

        walker = self.get_log_walker(transport=SvnRaTransport(repos_url))

        self.assertEqual(1, len(list(walker.iter_changes(["branches/brancha"],
            2))))

    def test_find_latest_none(self):
        repos_url = self.make_repository("a")
        cb = self.get_commit_editor(repos_url)
        cb.add_dir("branches")
        cb.close()

        walker = self.get_log_walker(transport=SvnRaTransport(repos_url))

        self.assertEqual(1, walker.find_latest_change("", 1))
    
    def test_find_latest_children_root(self):
        repos_url = self.make_repository("a")
        cb = self.get_commit_editor(repos_url)
        cb.add_file("branches").modify()
        cb.close()

        walker = self.get_log_walker(transport=SvnRaTransport(repos_url))

        self.assertEqual(1, 
            walker.find_latest_change("", 1))

    def test_find_latest_case(self):
        repos_url = self.make_repository("a")
        cb = self.get_commit_editor(repos_url)
        b = cb.add_dir("branches")
        b.add_file("branches/child").modify()
        cb.close()

        cb = self.get_commit_editor(repos_url)
        b = cb.add_dir("BRANCHES")
        b.add_file("BRANCHES/child").modify()
        cb.close()

        walker = self.get_log_walker(transport=SvnRaTransport(repos_url))

        self.assertEqual(1, 
            walker.find_latest_change("branches", 2))

    def test_find_latest_parent(self):
        repos_url = self.make_repository("a")
        cb = self.get_commit_editor(repos_url)
        cb.add_dir("tags")
        b = cb.add_dir("branches")
        bt = b.add_dir("branches/tmp")
        bt.add_dir("branches/tmp/foo")
        cb.close()

        cb = self.get_commit_editor(repos_url)
        t = cb.open_dir("tags")
        t.add_dir("tags/tmp", "branches/tmp")
        cb.close()

        walker = self.get_log_walker(transport=SvnRaTransport(repos_url))

        self.assertEqual(2, walker.find_latest_change("tags/tmp/foo", 2))

    def test_find_latest_parent_just_modify(self):
        repos_url = self.make_repository("a")

        cb = self.get_commit_editor(repos_url)
        b = cb.add_dir("branches")
        bt = b.add_dir("branches/tmp")
        bt.add_dir("branches/tmp/foo")
        cb.add_dir("tags")
        cb.close()

        cb = self.get_commit_editor(repos_url)
        t = cb.open_dir("tags")
        t.add_dir("tags/tmp", "branches/tmp")
        cb.close()

        cb = self.get_commit_editor(repos_url)
        t = cb.open_dir("tags")
        t.change_prop("myprop", "mydata")
        cb.close()

        walker = self.get_log_walker(transport=SvnRaTransport(repos_url))
        self.assertEqual(2, walker.find_latest_change("tags/tmp/foo", 3))

    def test_find_latest_parentmoved(self):
        repos_url = self.make_repository("a")

        cb = self.get_commit_editor(repos_url)
        b = cb.add_dir("branches")
        b.add_dir("branches/tmp")
        cb.close()

        cb = self.get_commit_editor(repos_url)
        cb.add_dir("bla", "branches")
        cb.close()

        walker = self.get_log_walker(transport=SvnRaTransport(repos_url))

        self.assertIs(2, walker.find_latest_change("bla/tmp", 2))

    def test_find_latest_nonexistant(self):
        repos_url = self.make_repository("a")

        cb = self.get_commit_editor(repos_url)
        b = cb.add_dir("branches")
        b.add_dir("branches/tmp")
        cb.close()

        cb = self.get_commit_editor(repos_url)
        cb.add_dir("bla", "branches")
        cb.close()

        walker = self.get_log_walker(transport=SvnRaTransport(repos_url))

        self.assertIs(None, walker.find_latest_change("bloe", 2))
        self.assertIs(None, walker.find_latest_change("bloe/bla", 2))

    def test_find_latest_change(self):
        repos_url = self.make_repository("a")

        cb = self.get_commit_editor(repos_url)
        cb.add_dir("branches")
        cb.close()

        walker = self.get_log_walker(transport=SvnRaTransport(repos_url))

        self.assertEqual(1, walker.find_latest_change("branches", 1))

    def test_find_latest_change_children(self):
        repos_url = self.make_repository("a")

        cb = self.get_commit_editor(repos_url)
        cb.add_dir("branches")
        cb.close()

        cb = self.get_commit_editor(repos_url)
        b = cb.open_dir("branches")
        b.add_file("branches/foo")
        cb.close()

        walker = self.get_log_walker(transport=SvnRaTransport(repos_url))

        self.assertEqual(2, walker.find_latest_change("branches", 2))

    def test_find_latest_change_prop(self):
        repos_url = self.make_repository("a")

        cb = self.get_commit_editor(repos_url)
        cb.add_dir("branches")
        cb.close()

        cb = self.get_commit_editor(repos_url)
        cb.open_dir("branches").change_prop("myprop", "mydata")
        cb.close()

        cb = self.get_commit_editor(repos_url)
        b = cb.open_dir("branches")
        b.add_file("branches/foo")
        cb.close()

        walker = self.get_log_walker(transport=SvnRaTransport(repos_url))

        self.assertEqual(3, walker.find_latest_change("branches", 3))

    def test_find_latest_change_file(self):
        repos_url = self.make_repository("a")

        cb = self.get_commit_editor(repos_url)
        cb.add_dir("branches")
        cb.close()

        cb = self.get_commit_editor(repos_url)
        b = cb.open_dir("branches")
        b.add_file("branches/foo").modify()
        cb.close()

        cb = self.get_commit_editor(repos_url)
        b = cb.open_dir("branches")
        b.open_file("branches/foo").modify()
        cb.close()

        walker = self.get_log_walker(transport=SvnRaTransport(repos_url))

        self.assertEqual(3, walker.find_latest_change("branches/foo", 3))

    def test_find_latest_change_newer(self):
        repos_url = self.make_repository("a")

        cb = self.get_commit_editor(repos_url)
        cb.add_dir("branches")
        cb.close()

        cb = self.get_commit_editor(repos_url)
        b = cb.open_dir("branches")
        b.add_file("branches/foo")
        cb.close()

        cb = self.get_commit_editor(repos_url)
        b = cb.open_dir("branches")
        b.open_file("branches/foo").modify()
        cb.close()

        walker = self.get_log_walker(transport=SvnRaTransport(repos_url))

        self.assertEqual(2, walker.find_latest_change("branches/foo", 2))

    def test_follow_history_branch_replace(self):
        repos_url = self.make_repository("a")

        cb = self.get_commit_editor(repos_url)
        t = cb.add_dir("trunk")
        t.add_file("trunk/data").modify()
        cb.close()
        
        cb = self.get_commit_editor(repos_url)
        cb.delete("trunk")
        cb.close()

        cb = self.get_commit_editor(repos_url)
        t = cb.add_dir("trunk")
        t.add_file("trunk/data")
        cb.close()

        walker = self.get_log_walker(transport=SvnRaTransport(repos_url))
        self.assertEqual([({"trunk/data": ('A', None, -1),
                                     "trunk": ('A', None, -1)}, 3)], 
                                     [l[:2] for l in walker.iter_changes(["trunk"], 3)])

    def test_follow_history(self):
        repos_url = self.make_repository("a")
        walker = self.get_log_walker(transport=SvnRaTransport(repos_url))

        cb = self.get_commit_editor(repos_url)
        cb.add_file("foo")
        cb.close()

        for (paths, rev, revprops) in walker.iter_changes([""], 1):
            self.assertTrue(rev == 0 or paths.has_key("foo"))
            self.assertTrue(rev in (0, 1))

    def test_follow_history_nohist(self):
        repos_url = self.make_repository("a")
        walker = self.get_log_walker(transport=SvnRaTransport(repos_url))

        self.assertEqual([({'': ('A', None, -1)}, 0)], [l[:2] for l in walker.iter_changes([""], 0)])

    def test_later_update(self):
        repos_url = self.make_repository("a")

        walker = self.get_log_walker(transport=SvnRaTransport(repos_url))

        cb = self.get_commit_editor(repos_url)
        cb.add_file("foo")
        cb.close()

        for (paths, rev, revprops) in walker.iter_changes([""], 1):
            self.assertTrue(rev == 0 or paths.has_key("foo"))
            self.assertTrue(rev in (0, 1))

        iter = walker.iter_changes([""], 2)
        self.assertRaises(NoSuchRevision, list, iter)

    def test_get_branch_log_follow(self):
        repos_url = self.make_repository("a")
        cb = self.get_commit_editor(repos_url)
        t = cb.add_dir("trunk")
        t.add_file("trunk/afile")
        cb.add_dir("branches")
        cb.close()

        cb = self.get_commit_editor(repos_url)
        b = cb.open_dir("branches")
        b.add_dir("branches/abranch", "trunk")
        cb.close()

        walker = self.get_log_walker(transport=SvnRaTransport(repos_url))

        items = [l[:2] for l in walker.iter_changes(["branches/abranch"], 2)]
        self.assertEqual([({'branches/abranch': ('A', 'trunk', 1)}, 2), 
                          ({'branches': (u'A', None, -1),
                                     'trunk/afile': ('A', None, -1), 
                                     'trunk': (u'A', None, -1)}, 1)], items)

    def test_revprop_list(self):
        repos_url = self.make_repository("a")

        cb = self.get_commit_editor(repos_url)
        cb.add_dir("trunk")
        cb.close()

        walker = self.get_log_walker(transport=SvnRaTransport(repos_url))

        props = walker.revprop_list(1)
        self.assertEquals(set(["svn:date", "svn:author", "svn:log"]), set(props.keys()))

        props = walker.revprop_list(0)
        self.assertEquals(set(["svn:date"]), set(props.keys()))

    def test_set_revprop(self):
        repos_url = self.make_repository("a")

        cb = self.get_commit_editor(repos_url)
        cb.add_dir("trunk")
        cb.close()

        transport = SvnRaTransport(repos_url)

        transport.change_rev_prop(1, "foo", "blaaa")

        walker = self.get_log_walker(transport=transport)

        props = walker.revprop_list(1)
        self.assertEquals("blaaa", props["foo"])

    def test_iter_changes_property_change(self):
        repos_url = self.make_repository('d')

        cb = self.get_commit_editor(repos_url)
        t = cb.add_dir("trunk")
        t.add_file("trunk/bla").modify()
        cb.close()

        cb = self.get_commit_editor(repos_url)
        t = cb.open_dir("trunk")
        t.change_prop("some:property", "some data\n")
        cb.close()

        cb = self.get_commit_editor(repos_url)
        t = cb.open_dir("trunk")
        t.change_prop("some2:property", "some data\n")
        cb.close()

        cb = self.get_commit_editor(repos_url)
        t = cb.open_dir("trunk")
        t.change_prop("some:property", "some other data\n")
        cb.close()

        walker = self.get_log_walker(transport=SvnRaTransport(repos_url))
        self.assertEquals([({'trunk': (u'M', None, -1)}, 3), 
                           ({'trunk': (u'M', None, -1)}, 2), 
                           ({'trunk/bla': (u'A', None, -1), 'trunk': (u'A', None, -1)}, 1)], [l[:2] for l in walker.iter_changes(["trunk"], 3)])

    def test_iter_changes_pointless(self):
        repos_url = self.make_repository('d')

        cb = self.get_commit_editor(repos_url)
        cb.close()

        cb = self.get_commit_editor(repos_url)
        t = cb.add_dir("trunk")
        cb.close()

        walker = self.get_log_walker(transport=SvnRaTransport(repos_url))
        self.assertEquals([({'trunk': (u'A', None, -1)}, 2)],
                [l[:2] for l in walker.iter_changes(["trunk"], 2)])

        self.assertEquals([({'trunk': (u'A', None, -1)}, 2), 
                           ({}, 1),
                           ({'': ('A', None, -1)}, 0) ],
                [l[:2] for l in walker.iter_changes([""], 2)])


class TestCachingLogWalker(TestLogWalker):
    def setUp(self):
        super(TestCachingLogWalker, self).setUp()

        logwalker.cache_dir = os.path.join(self.test_dir, "cache-dir")

    def get_log_walker(self, transport):
        return logwalker.CachingLogWalker(super(TestCachingLogWalker, self).get_log_walker(transport))



class TestLogCache(TestCase):
    def setUp(self):
        super(TestLogCache, self).setUp()
        self.cache = logwalker.LogCache()

    def test_insert_path(self):
        self.cache.insert_path(42, "foo", "A", None, -1)
        self.assertEquals({"foo": ("A", None, -1)}, self.cache.get_revision_paths(42))

    def test_insert_revprop(self):
        self.cache.insert_revprop(100, "some", "data")
        self.assertEquals({"some": "data"}, self.cache.get_revprops(100))

    def test_insert_revinfo(self):
        self.cache.insert_revinfo(45, True)
        self.cache.insert_revinfo(42, False)
        self.assertTrue(self.cache.has_all_revprops(45))
        self.assertFalse(self.cache.has_all_revprops(42))

    def test_find_latest_change(self):
        self.cache.insert_path(42, "foo", "A")
        self.assertEquals(42, self.cache.find_latest_change("foo", 42))
        self.assertEquals(42, self.cache.find_latest_change("foo", 45))
