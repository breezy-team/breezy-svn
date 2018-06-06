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

"""Tests for the changes module."""

from subvertpy import NODE_DIR

from breezy.tests import TestCase

from breezy.plugins.svn.changes import (
    apply_reverse_changes,
    changes_root,
    find_prev_location,
    path_is_child,
    under_prefixes,
    )

class PathIsChildTests(TestCase):

    def test_both_empty(self):
        self.assertTrue(path_is_child(u"", u""))

    def test_child_path(self):
        self.assertTrue(path_is_child(u"trunk", u"trunk/bar"))

    def test_self(self):
        self.assertTrue(path_is_child(u"trunk", u"trunk"))

    def test_child_empty_bp(self):
        self.assertTrue(path_is_child(u"", u"bar"))

    def test_unrelated(self):
        self.assertFalse(path_is_child(u"bla", u"bar"))


class FindPrevLocationTests(TestCase):

    def test_simple_prev_changed(self):
        self.assertEquals((u"foo", 1),
            find_prev_location({u"foo": ("M", None, -1)}, u"foo", 2))

    def test_simple_prev_notchanged(self):
        self.assertEquals(("foo", 1),
            find_prev_location({}, u"foo", 2))

    def test_simple_prev_copied(self):
        self.assertEquals(("bar", 1),
            find_prev_location({u"foo": ("A", u"bar", 1)}, u"foo", 2))

    def test_simple_prev_replaced(self):
        self.assertEquals(("bar", 1),
            find_prev_location({u"foo": ("R", u"bar", 1)}, u"foo", 2))

    def test_endofhistory(self):
        self.assertEquals(None,
            find_prev_location({}, u"", 0))

    def test_rootchange(self):
        self.assertEquals((u"", 4),
            find_prev_location({}, u"", 5))

    def test_parentcopy(self):
        self.assertEquals((u"foo/bla", 3),
            find_prev_location({u"bar": ("A", u"foo", 3)}, u"bar/bla", 5))

    def test_from_root(self):
        self.assertEquals((u"tools", 3),
            find_prev_location({u"trunk": ("A", u"", 3)}, u"trunk/tools", 5))


class ChangesRootTests(TestCase):

    def test_empty(self):
        self.assertEquals(None, changes_root([]))

    def test_simple(self):
        self.assertEquals(u"bla", changes_root([u"bla", u"bla/blie"]))

    def test_siblings(self):
        self.assertEquals(u"bla", changes_root([u"bla/blie", u"bla/bloe", u"bla"]))

    def test_simple_other(self):
        self.assertEquals(u"bla", changes_root([u"bla/blie", u"bla"]))

    def test_single(self):
        self.assertEquals(u"bla", changes_root([u"bla"]))

    def test_multiple_roots(self):
        self.assertEquals(None, changes_root([u"bla", u"blie"]))


class ApplyReverseChangesTests(TestCase):

    def test_parent_rename(self):
        self.assertEquals([(u'tags/2.5-M2', u'geotools/tags/2.5-M2', 5617, NODE_DIR)],
            list(apply_reverse_changes([u'trunk/geotools2', u'tags/2.5-M2', u'trunk'],
                {u'tags': (u'A', u'geotools/tags', 5617, NODE_DIR), u'geotools/tags': (u'D', None, -1, NODE_DIR)}))
            )

    def test_simple_rename(self):
        self.assertEquals([(u"newname", u"oldname", 2, NODE_DIR)],
            list(apply_reverse_changes([u"newname"],
                {u"newname": (u"A", u"oldname", 2, NODE_DIR)})))

    def test_simple_rename_nonexistant(self):
        self.assertEquals([],
            list(apply_reverse_changes([],
                {u"newname": (u"A", u"oldname", 2, NODE_DIR)})))

    def test_modify(self):
        self.assertEquals([],
            list(apply_reverse_changes([u"somename"],
                {u"somename": (u"M", None, -1, NODE_DIR)})))

    def test_delete(self):
        self.assertEquals([(u"somename/bloe", None, -1, NODE_DIR)],
            list(apply_reverse_changes([u"somename/bloe"],
                {u"somename": (u"D", None, -1, NODE_DIR)})))

    def test_add(self):
        self.assertEquals([],
            list(apply_reverse_changes([u"somename"],
                {u"somename": (u"A", None, -1, NODE_DIR)})))

    def test_chaco(self):
        self.assertEquals([(u'packages/enthought-chaco2/trunk', u'packages/chaco2/trunk', 3, NODE_DIR)],
            list(apply_reverse_changes([u'packages/enthought-chaco2/trunk'],
                { u"packages/chaco2": ("D", None, -1, NODE_DIR),
                    u"packages/enthought-chaco2": ("A", u"packages/chaco2", 3, NODE_DIR),
                    u"packages/enthought-chaco2/trunk": ("D", None, -1, NODE_DIR)})))


class UnderPrefixesTests(TestCase):

    def test_direct_match(self):
        self.assertTrue(under_prefixes(u"foo", [u"foo"]))
        self.assertTrue(under_prefixes(u"foo", [u"la", u"foo"]))
        self.assertFalse(under_prefixes(u"foob", [u"foo"]))
        self.assertFalse(under_prefixes(u"foob", []))

    def test_child(self):
        self.assertTrue(under_prefixes(u"foo", [u""]))
        self.assertTrue(under_prefixes(u"foo/bar", [u"foo"]))
        self.assertTrue(under_prefixes(u"foo/bar", [u""]))
        self.assertFalse(under_prefixes(u"foo/bar", [u"bar"]))
        self.assertTrue(under_prefixes(u"foo/bar", [u"bar", u"foo"]))

    def test_none(self):
        self.assertTrue(under_prefixes(u"foo", None))
