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

from bzrlib.tests import (
    TestCase,
    )

from bzrlib.plugins.svn.errors import (
    NotSvnBranchPath,
    )
from bzrlib.plugins.svn.layout.standard import (
    RootLayout,
    TrunkLayout,
    WildcardLayout,
    )


class LayoutTests:

    def test_get_tag_path(self):
        path = self.layout.get_tag_path("foo", "")
        if path is None:
            return
        self.assertIsInstance(path, str)

    def test_tag_path_is_tag(self):
        path = self.layout.get_tag_path("foo", "")
        if path is None:
            return
        self.assertTrue(self.layout.is_tag(path))


class RootLayoutTests(TestCase,LayoutTests):

    def setUp(self):
        TestCase.setUp(self)
        self.layout = RootLayout()


class TrunkLayoutTests(TestCase,LayoutTests):

    def setUp(self):
        TestCase.setUp(self)
        self.layout = TrunkLayout()

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


class WildcardLayoutTests(TestCase):

    def test_is_branch_simple(self):
        x = WildcardLayout(["foo"])
        self.assertTrue(x.is_branch("foo"))
        self.assertFalse(x.is_branch("foo/bar"))
        self.assertFalse(x.is_branch(""))

    def test_wildcard(self):
        x = WildcardLayout(["*"])
        self.assertTrue(x.is_branch("foo"))
        self.assertFalse(x.is_branch("foo/bar"))
        self.assertFalse(x.is_branch(""))

    def test_get_tag_name(self):
        x = WildcardLayout(["trunk"], ["tags/*"])
        self.assertEquals("bla", x.get_tag_name("tags/bla"))
        x = WildcardLayout(["trunk"], ["tags/bla"])
        self.assertEquals("bla", x.get_tag_name("tags/bla"))
        x = WildcardLayout(["trunk"], ["tags/bla"])
        self.assertEquals(None, x.get_tag_name("bla"))
