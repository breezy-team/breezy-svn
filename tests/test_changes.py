# Copyright (C) 2005-2009 Jelmer Vernooij <jelmer@samba.org>
 
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

from bzrlib.tests import TestCase

from bzrlib.plugins.svn.changes import (
    apply_reverse_changes,
    changes_root,
    find_prev_location,
    path_is_child,
    )

class PathIsChildTests(TestCase):
    def test_both_empty(self):
        self.assertTrue(path_is_child("", ""))

    def test_child_path(self):
        self.assertTrue(path_is_child("trunk", "trunk/bar"))

    def test_self(self):
        self.assertTrue(path_is_child("trunk", "trunk"))

    def test_child_empty_bp(self):
        self.assertTrue(path_is_child("", "bar"))

    def test_unrelated(self):
        self.assertFalse(path_is_child("bla", "bar"))


class FindPrevLocationTests(TestCase):
    def test_simple_prev_changed(self):
        self.assertEquals(("foo", 1), find_prev_location({"foo": ("M", None, -1)}, "foo", 2))

    def test_simple_prev_notchanged(self):
        self.assertEquals(("foo", 1), find_prev_location({}, "foo", 2))

    def test_simple_prev_copied(self):
        self.assertEquals(("bar", 1), find_prev_location({"foo": ("A", "bar", 1)}, "foo", 2))

    def test_simple_prev_replaced(self):
        self.assertEquals(("bar", 1), find_prev_location({"foo": ("R", "bar", 1)}, "foo", 2))

    def test_endofhistory(self):
        self.assertEquals(None, find_prev_location({}, "", 0))

    def test_rootchange(self):
        self.assertEquals(("", 4), find_prev_location({}, "", 5))

    def test_parentcopy(self):
        self.assertEquals(("foo/bla", 3), find_prev_location({"bar": ("A", "foo", 3)}, "bar/bla", 5))


class ChangesRootTests(TestCase):
    def test_empty(self):
        self.assertEquals(None, changes_root([]))

    def test_simple(self):
        self.assertEquals("bla", changes_root(["bla", "bla/blie"]))

    def test_siblings(self):
        self.assertEquals("bla", changes_root(["bla/blie", "bla/bloe", "bla"]))

    def test_simple_other(self):
        self.assertEquals("bla", changes_root(["bla/blie", "bla"]))

    def test_single(self):
        self.assertEquals("bla", changes_root(["bla"]))

    def test_multiple_roots(self):
        self.assertEquals(None, changes_root(["bla", "blie"]))


class ApplyReverseChangesTests(TestCase):

    def test_parent_rename(self):
        self.assertEquals([('tags/2.5-M2', 'geotools/tags/2.5-M2', 5617)],
            list(apply_reverse_changes(['trunk/geotools2', 'tags/2.5-M2', 'trunk'], 
                {'tags': (u'A', 'geotools/tags', 5617), 'geotools/tags': (u'D', None, -1)}))
            )

    def test_simple_rename(self):
        self.assertEquals([("newname", "oldname", 2)],
            list(apply_reverse_changes(["newname"], 
                {"newname": (u"A", "oldname", 2)})))

    def test_simple_rename_nonexistant(self):
        self.assertEquals([],
            list(apply_reverse_changes([], 
                {"newname": (u"A", "oldname", 2)})))

    def test_modify(self):
        self.assertEquals([],
            list(apply_reverse_changes(["somename"], 
                {"somename": (u"M", None, -1)})))

    def test_delete(self):
        self.assertEquals([("somename/bloe", None, -1)],
            list(apply_reverse_changes(["somename/bloe"], 
                {"somename": (u"D", None, -1)})))

    def test_add(self):
        self.assertEquals([],
            list(apply_reverse_changes(["somename"], 
                {"somename": (u"A", None, -1)})))

    def test_chaco(self):
        self.assertEquals([('packages/enthought-chaco2/trunk', 'packages/chaco2/trunk', 3)],
            list(apply_reverse_changes(['packages/enthought-chaco2/trunk'],
                { "packages/chaco2": ("D", None, -1),
                    "packages/enthought-chaco2": ("A", "packages/chaco2", 3),
                    "packages/enthought-chaco2/trunk": ("D", None, -1)})))
