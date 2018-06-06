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

from breezy.sixish import text_type
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
        self.assertEquals("trunk", self.layout.get_branch_path(""))
        self.assertEquals("myproj/trunk",
            self.layout.get_branch_path("", "myproj"))

    def test_get_branch_path_somepath(self):
        self.assertEquals("branches/abranch",
            self.layout.get_branch_path("abranch"))
        self.assertEquals("proj/a/branches/abranch",
            self.layout.get_branch_path("abranch", "proj/a"))

    def test_get_branch_name(self):
        self.assertEquals("abranch",
            self.layout.get_branch_name("branches/abranch"))
        self.assertEquals("",
            self.layout.get_branch_name("trunk"))

    def test_is_branch_parent(self):
        self.assertEquals(True, self.layout.is_branch_parent("foo/bar"))

    def test_is_tag_parent(self):
        self.assertEquals(True, self.layout.is_tag_parent("foo/bar"))

    def test_parse_trunk(self):
        self.assertEquals(("branch", "", "trunk", ""), 
                          self.layout.parse("trunk"))

    def test_parse_trunk_tags(self):
        self.assertEquals(("branch", "", "trunk", "tags"), 
                          self.layout.parse("trunk/tags"))

    def test_parse_tag(self):
        self.assertEquals(("tag", "", "tags/foo", ""), 
                          self.layout.parse("tags/foo"))

    def test_parse_branch(self):
        self.assertEquals(("branch", "", "branches/foo", ""), 
                          self.layout.parse("branches/foo"))

    def test_parse_branch_project(self):
        self.assertEquals(("branch", "bla", "bla/branches/foo", ""), 
                          self.layout.parse("bla/branches/foo"))

    def test_parse_branches(self):
        self.assertRaises(NotSvnBranchPath, self.layout.parse, "branches")


class Trunk2LayoutTests(TestCase):

    def setUp(self):
        TestCase.setUp(self)
        self.layout = TrunkLayout(2)

    def test_is_branch_parent_trunk(self):
        self.assertEquals(False, self.layout.is_branch_parent("foo/bar/trunk"))

    def test_is_branch_parent_lev2(self):
        self.assertEquals(True, self.layout.is_branch_parent("foo/bar"))

    def test_is_branch_parent_lev1(self):
        self.assertEquals(True, self.layout.is_branch_parent("foo"))

    def test_is_branch_parent_lev3(self):
        self.assertEquals(False, self.layout.is_branch_parent("foo/bar/blie"))

    def test_is_branch_parent_branches(self):
        self.assertEquals(True, self.layout.is_branch_parent("foo/bar/branches"))

    def test_is_branch_parent_tags(self):
        self.assertEquals(False, self.layout.is_branch_parent("foo/bar/tags"))


class TrunkVariableLayoutTests(TestCase):

    def setUp(self):
        TestCase.setUp(self)
        self.layout = TrunkLayout(None)

    def test_is_trunk_branch(self):
        self.assertEquals(True, self.layout.is_branch("foo/bar/trunk"))
        self.assertEquals(True, self.layout.is_branch("trunk"))

    def test_is_foo_branch(self):
        self.assertEquals(False, self.layout.is_branch("foo"))

    def test_is_branch_parent_trunk(self):
        self.assertEquals(True, self.layout.is_branch_parent("foo/bar/trunk"))

    def test_is_branch_parent_lev2(self):
        self.assertEquals(True, self.layout.is_branch_parent("foo/bar"))

    def test_is_branch_parent_lev1(self):
        self.assertEquals(True, self.layout.is_branch_parent("foo"))

    def test_is_branch_parent_lev3(self):
        self.assertEquals(True, self.layout.is_branch_parent("foo/bar/blie"))

    def test_is_branch_parent_branches(self):
        self.assertEquals(True, self.layout.is_branch_parent("foo/bar/branches"))

    def test_is_branch_parent_tags(self):
        self.assertEquals(True, self.layout.is_branch_parent("bar/tags"))
        self.assertEquals(True, self.layout.is_branch_parent("foo/bar/tags"))


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
        x = CustomLayout(["foo/bar"])
        self.assertTrue(x.is_branch("foo/bar"))
        self.assertFalse(x.is_branch("foo"))

    def test_is_branch_parent(self):
        x = CustomLayout(["foo/bar"])
        self.assertFalse(x.is_branch_parent("foo/bar"))
        self.assertTrue(x.is_branch_parent("foo"))

    def test_get_branches(self):
        x = CustomLayout(["foo/bar"])
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
                self.transport = MockTransport(available_paths)

        self.assertEquals([(None, "foo/bar", "bar", None, 3)],
            x.get_branches(MockRepository({"foo/bar": 3}), 3))
