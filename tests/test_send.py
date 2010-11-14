# Copyright (C) 2010 Jelmer Vernooij <jelmer@samba.org>
# *-* coding: utf-8 *-*

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


"""bzr-svn send tests."""

from cStringIO import StringIO

from bzrlib import tests

from bzrlib.plugins.svn.send import SvnDiffTree


class TestDiffTreeNonSvn(tests.TestCaseWithTransport):
    """Test SvnDiffTree with non-Bazaar trees."""

    def setUp(self):
        super(TestDiffTreeNonSvn, self).setUp()
        self.old_tree = self.make_branch_and_tree('old-tree')
        self.old_tree.lock_write()
        self.addCleanup(self.old_tree.unlock)
        self.new_tree = self.make_branch_and_tree('new-tree')
        self.new_tree.lock_write()
        self.addCleanup(self.new_tree.unlock)
        self.differ = SvnDiffTree(self.old_tree, self.new_tree, StringIO())

    def test_show_properties(self):
        self.differ._write_properties_diff("my/path", {}, {})
        self.assertEquals("", self.differ.to_file.getvalue())

    def test_write_contents_diff(self):
        self.differ._write_contents_diff("file", "old-version", ["old\n"],
                "new-version", ["new\n"])
        self.assertEquals('Index: file\n' +
            "=" * 67 + "\n"
            "--- file\told-version\n"
            "+++ file\tnew-version\n"
            "@@ -1,1 +1,1 @@\n"
            "-old\n"
            "+new\n\n", self.differ.to_file.getvalue())

    def test_write_contents_diff_binary(self):
        self.differ._write_contents_diff("file", "old-version", ["old\0"],
                "new-version", ["n\0ew"])
        self.assertEquals('Index: file\n' +
            ("=" * 67) + "\n"
            "Cannot display: file contains binary data.\n",
            self.differ.to_file.getvalue())
