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

from bzrlib.plugins.svn.layout.custom import (
    KDELayout,
    )


class KDELayoutTests(TestCase):

    def setUp(self):
        TestCase.setUp(self)
        self.layout = KDELayout()

    def test_repr(self):
        self.assertEquals("KDELayout()", repr(self.layout))

    def test_str(self):
        self.assertEquals("kde", str(self.layout))

    def test_get_project_prefixes(self):
        self.assertEquals([
            "trunk/KDE/kdebase",
            "branches/KDE",
            "tags/KDE"], self.layout.get_project_prefixes("KDE/kdebase"))

    def test_get_path(self):
        self.assertEquals("tags/KDE/1.0/kdebase", 
            self.layout.get_tag_path("1.0", "KDE/kdebase"))
        self.assertEquals("tags/k3b/3.0",
            self.layout.get_tag_path("3.0", "k3b"))

    def test_is_branch_parent(self):
        self.assertTrue(self.layout.is_branch_parent(""))
        self.assertTrue(self.layout.is_branch_parent("trunk"))
        self.assertTrue(self.layout.is_branch_parent("trunk/KDE"))
        self.assertTrue(self.layout.is_branch_parent("branches"))
        self.assertTrue(self.layout.is_branch_parent("branches/KDE/2.0"))
        self.assertFalse(self.layout.is_branch_parent("branches/something/2.0"))
        self.assertTrue(self.layout.is_branch_parent("branches/KDE"))
        self.assertFalse(self.layout.is_branch_parent("branches/some"))

