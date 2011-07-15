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

from bzrlib.repository import Repository

from bzrlib.tests import TestCase

from bzrlib.plugins.svn.layout.standard import (
    CustomLayout,)
from bzrlib.plugins.svn.tests import SubversionTestCase
from bzrlib.plugins.svn.metagraph import (
    filter_revisions,
    restrict_prefixes,
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



