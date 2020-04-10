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

import subvertpy

from six import text_type
from breezy.tests import (
    TestCase,
    )

from breezy.plugins.svn.errors import (
    NoCustomBranchPaths,
    NotSvnBranchPath,
    NoLayoutTagSetSupport,
    )
from breezy.plugins.svn.layout.standard import (
    CustomLayout,
    RootLayout,
    TrunkLayout,
    WildcardLayout,
    )


class LayoutTests(object):

    def test_get_tag_path(self):
        try:
            path = self.layout.get_tag_path("foo", "")
        except NoLayoutTagSetSupport:
            return
        self.assertIsInstance(path, text_type)

    def test_tag_path_is_tag(self):
        try:
            path = self.layout.get_tag_path("foo", "")
        except NoLayoutTagSetSupport:
            return
        self.assertTrue(self.layout.is_tag(path))


class RootLayoutTests(TestCase,LayoutTests):

    def setUp(self):
        TestCase.setUp(self)
        self.layout = RootLayout()

    def test_get_branch_path_default(self):
        self.assertEquals("", self.layout.get_branch_path(""))
        self.assertRaises(NoCustomBranchPaths,
            self.layout.get_branch_path, None, "proh")

    def test_get_branch_path_custom(self):
        self.assertRaises(NoCustomBranchPaths,
            self.layout.get_branch_path, "la")
        self.assertRaises(NoCustomBranchPaths,
            self.layout.get_branch_path, "la", "proh")


class TrunkLayoutTests(TestCase,LayoutTests):

    def setUp(self):
        TestCase.setUp(self)
        self.layout = TrunkLayout()

    def test_get_branch_path_default(self):
        self.assertEquals(u"trunk", self.layout.get_branch_path(u""))
        self.assertEquals(u"myproj/trunk",
            self.layout.get_branch_path(u"", u"myproj"))

    def test_get_branch_path_somepath(self):
        self.assertEquals(u"branches/abranch",
            self.layout.get_branch_path(u"abranch"))
        self.assertEquals(u"proj/a/branches/abranch",
            self.layout.get_branch_path(u"abranch", u"proj/a"))

    def test_get_branch_name(self):
        self.assertEquals(u"abranch",
            self.layout.get_branch_name(u"branches/abranch"))
        self.assertEquals(u"",
            self.layout.get_branch_name(u"trunk"))

    def test_is_branch_parent(self):
        self.assertEquals(True, self.layout.is_branch_parent(u"foo/bar"))

    def test_is_tag_parent(self):
        self.assertEquals(True, self.layout.is_tag_parent(u"foo/bar"))

    def test_parse_trunk(self):
        self.assertEquals(("branch", u"", u"trunk", u""),
                          self.layout.parse(u"trunk"))

    def test_parse_trunk_tags(self):
        self.assertEquals(("branch", u"", u"trunk", u"tags"),
                          self.layout.parse(u"trunk/tags"))

    def test_parse_tag(self):
        self.assertEquals(("tag", u"", u"tags/foo", u""),
                          self.layout.parse(u"tags/foo"))

    def test_parse_branch(self):
        self.assertEquals(("branch", u"", u"branches/foo", u""),
                          self.layout.parse(u"branches/foo"))

    def test_parse_branch_project(self):
        self.assertEquals(("branch", u"bla", u"bla/branches/foo", u""),
                          self.layout.parse(u"bla/branches/foo"))

    def test_parse_branches(self):
        self.assertRaises(NotSvnBranchPath, self.layout.parse, u"branches")


class Trunk2LayoutTests(TestCase):

    def setUp(self):
        TestCase.setUp(self)
        self.layout = TrunkLayout(2)

    def test_is_branch_parent_trunk(self):
        self.assertEquals(False, self.layout.is_branch_parent(u"foo/bar/trunk"))

    def test_is_branch_parent_lev2(self):
        self.assertEquals(True, self.layout.is_branch_parent(u"foo/bar"))

    def test_is_branch_parent_lev1(self):
        self.assertEquals(True, self.layout.is_branch_parent(u"foo"))

    def test_is_branch_parent_lev3(self):
        self.assertEquals(False, self.layout.is_branch_parent(u"foo/bar/blie"))

    def test_is_branch_parent_branches(self):
        self.assertEquals(True, self.layout.is_branch_parent(u"foo/bar/branches"))

    def test_is_branch_parent_tags(self):
        self.assertEquals(False, self.layout.is_branch_parent(u"foo/bar/tags"))


class TrunkVariableLayoutTests(TestCase):

    def setUp(self):
        TestCase.setUp(self)
        self.layout = TrunkLayout(None)

    def test_is_trunk_branch(self):
        self.assertEquals(True, self.layout.is_branch(u"foo/bar/trunk"))
        self.assertEquals(True, self.layout.is_branch(u"trunk"))

    def test_is_foo_branch(self):
        self.assertEquals(False, self.layout.is_branch(u"foo"))

    def test_is_branch_parent_trunk(self):
        self.assertEquals(True, self.layout.is_branch_parent(u"foo/bar/trunk"))

    def test_is_branch_parent_lev2(self):
        self.assertEquals(True, self.layout.is_branch_parent(u"foo/bar"))

    def test_is_branch_parent_lev1(self):
        self.assertEquals(True, self.layout.is_branch_parent(u"foo"))

    def test_is_branch_parent_lev3(self):
        self.assertEquals(True, self.layout.is_branch_parent(u"foo/bar/blie"))

    def test_is_branch_parent_branches(self):
        self.assertEquals(True, self.layout.is_branch_parent(u"foo/bar/branches"))

    def test_is_branch_parent_tags(self):
        self.assertEquals(True, self.layout.is_branch_parent(u"bar/tags"))
        self.assertEquals(True, self.layout.is_branch_parent(u"foo/bar/tags"))


class WildcardLayoutTests(TestCase):

    def test_is_branch_simple(self):
        x = WildcardLayout([u"foo"])
        self.assertTrue(x.is_branch("foo"))
        self.assertFalse(x.is_branch("foo/bar"))
        self.assertFalse(x.is_branch(""))

    def test_wildcard(self):
        x = WildcardLayout([u"*"])
        self.assertTrue(x.is_branch("foo"))
        self.assertFalse(x.is_branch("foo/bar"))
        self.assertFalse(x.is_branch(""))

    def test_get_tag_name(self):
        x = WildcardLayout([u"trunk"], [u"tags/*"])
        self.assertEquals("bla", x.get_tag_name("tags/bla"))
        x = WildcardLayout([u"trunk"], [u"tags/bla"])
        self.assertEquals("bla", x.get_tag_name("tags/bla"))
        x = WildcardLayout([u"trunk"], [u"tags/bla"])
        self.assertEquals(None, x.get_tag_name("bla"))

    def test_get_branch_name(self):
        x = WildcardLayout([u"trunk"], [u"tags/*"])
        self.assertEquals("trunk", x.get_branch_name("trunk"))
        x = WildcardLayout([u"trunk"], [u"tags/bla"])
        self.assertEquals(None, x.get_branch_name("tags/bla"))
        x = WildcardLayout([u"trunk"], [u"tags/bla"])
        self.assertEquals(None, x.get_branch_name("bla"))

    def test_get_tag_path(self):
        x = WildcardLayout([u"trunk"], [u"tags/*"])
        self.assertEquals("tags/bla", x.get_tag_path("bla"))
        x = WildcardLayout([u"trunk"], [])
        self.assertRaises(NoLayoutTagSetSupport, x.get_tag_path, "bla")


class CustomLayoutTests(TestCase):

    def test_is_branch(self):
        x = CustomLayout([u"foo/bar"])
        self.assertTrue(x.is_branch(u"foo/bar"))
        self.assertFalse(x.is_branch(u"foo"))

    def test_is_branch_parent(self):
        x = CustomLayout([u"foo/bar"])
        self.assertFalse(x.is_branch_parent(u"foo/bar"))
        self.assertTrue(x.is_branch_parent(u"foo"))

    def test_get_branches(self):
        x = CustomLayout([u"foo/bar"])
        class MockTransport(object):

            def __init__(self, available_paths):
                self._paths = available_paths

            def get_dir(self, path, revnum):
                try:
                    return (None, self._paths[path], None)
                except KeyError:
                    raise subvertpy.SubversionException("foo",
                            subvertpy.ERR_FS_NOT_FOUND)

        class MockRepository(object):

            def __init__(self, available_paths):
                self.svn_transport = MockTransport(available_paths)

        self.assertEquals([(None, u"foo/bar", u"bar", None, 3)],
            x.get_branches(MockRepository({u"foo/bar": 3}), 3))
