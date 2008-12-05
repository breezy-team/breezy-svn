# Copyright (C) 2005-2007 Jelmer Vernooij <jelmer@samba.org>
 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from bzrlib.repository import Repository
from bzrlib.tests import TestCase

from bzrlib.plugins.svn.revmeta import (
        filter_revisions,
        restrict_prefixes,
        )
from bzrlib.plugins.svn.tests import SubversionTestCase

class TestRevisionMetadata(SubversionTestCase):

    def make_provider(self, repos_url):
        r = Repository.open(repos_url)
        return r._revmeta_provider

    def test_get_changed_properties(self):
        repos_url = self.make_repository('d')

        dc = self.get_commit_editor(repos_url)
        dc.change_prop("myprop", "data\n")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.change_prop("myprop", "newdata\n")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.change_prop("myp2", "newdata\n")
        dc.close()

        repos = Repository.open(repos_url)

        revmeta1 = repos._revmeta_provider.get_revision("", 1)
        revmeta2 = repos._revmeta_provider.get_revision("", 2)
        revmeta3 = repos._revmeta_provider.get_revision("", 3)

        self.assertFalse(revmeta1.knows_changed_fileprops())

        self.assertEquals((None, "data\n"),
                          revmeta1.get_changed_fileprops()["myprop"])

        self.assertTrue(revmeta1.knows_changed_fileprops())
        self.assertTrue(revmeta1.knows_fileprops())

        self.assertEquals("data\n",
                          revmeta1.get_fileprops()["myprop"])

        self.assertEquals("newdata\n",
                          revmeta2.get_fileprops()["myprop"])

        self.assertEquals(("data\n","newdata\n"), 
                          revmeta2.get_changed_fileprops()["myprop"])

        self.assertEquals((None, "newdata\n"), 
                          revmeta3.get_changed_fileprops()["myp2"])

    def test_changes_branch_root(self):
        repos_url = self.make_repository('d')

        dc = self.get_commit_editor(repos_url)
        dc.change_prop("myprop", "data\n")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.add_file("bar").modify("bloe")
        dc.close()

        repos = Repository.open(repos_url)

        revmeta1 = repos._revmeta_provider.get_revision("", 1)
        revmeta2 = repos._revmeta_provider.get_revision("", 2)

        self.assertTrue(revmeta1.changes_branch_root())
        self.assertFalse(revmeta2.changes_branch_root())

    def test_get_paths(self):
        repos_url = self.make_repository('d')

        dc = self.get_commit_editor(repos_url)
        dc.change_prop("myprop", "data\n")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.add_file("bar").modify("bloe")
        dc.close()

        repos = Repository.open(repos_url)

        revmeta1 = repos._revmeta_provider.get_revision("", 1)
        revmeta2 = repos._revmeta_provider.get_revision("", 2)

        self.assertEquals({"": ("M", None, -1)}, revmeta1.get_paths())
        self.assertEquals({"bar": ("A", None, -1)}, 
                          revmeta2.get_paths())

    def test_foreign_revid(self):
        repos_url = self.make_repository('d')

        dc = self.get_commit_editor(repos_url)
        dc.change_prop("myprop", "data\n")
        dc.close()

        provider = self.make_provider(repos_url)

        revmeta1 = provider.get_revision("", 1)

        self.assertEquals((provider.repository.uuid, "", 1), 
                revmeta1.get_foreign_revid())

    def test_get_revprops(self):
        repos_url = self.make_repository('d')

        dc = self.get_commit_editor(repos_url)
        dc.change_prop("myprop", "data\n")
        dc.close()

        provider = self.make_provider(repos_url)
        revmeta1 = provider.get_revision("", 1)
        self.assertEquals(set(["svn:date", "svn:author", "svn:log"]),
                          set(revmeta1.get_revprops().keys()))

    def test_is_changes_root(self):
        repos_url = self.make_repository('d')

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("bloe")
        dc.close()

        provider = self.make_provider(repos_url)
        revmeta1 = provider.get_revision("", 1)
        self.assertFalse(revmeta1.is_changes_root())
        revmeta1 = provider.get_revision("bloe", 1)
        self.assertTrue(revmeta1.is_changes_root())


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
