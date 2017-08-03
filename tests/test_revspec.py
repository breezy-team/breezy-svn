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

"""
Tests for revision specificiers.
"""

from breezy import version_info as bzrlib_version
from breezy.branch import Branch
from breezy.controldir import ControlDir
from breezy.errors import (
    BzrError,
    InvalidRevisionSpec,
    )
from breezy.revisionspec import (
    RevisionInfo,
    RevisionSpec,
    )
from breezy.tests import TestCase

from breezy.plugins.svn.tests import SubversionTestCase


class TestRevSpec(TestCase):

    def test_present(self):
        self.assertIsNot(None, RevisionSpec.from_string("svn:foo"))

    def test_needs_branch(self):
        self.assertTrue(RevisionSpec.from_string("svn:foo").needs_branch())

    def test_get_branch(self):
        self.assertIs(None, RevisionSpec.from_string("svn:foo").get_branch())


class TestRevSpecsBySubversion(SubversionTestCase):

    def test_by_single_revno(self):
        revspec = RevisionSpec.from_string("svn:2")
        repos_url = self.make_svn_repository("a")

        dc = self.get_commit_editor(repos_url)
        dc.add_file("foo").modify()
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.add_file("bar").modify()
        dc.close()

        branch = Branch.open(repos_url)
        revinfo = revspec._match_on(branch, None)

        if breezy_version < (2, 5):
            self.assertEquals(RevisionInfo.from_revision_id(branch,
                branch.last_revision(), branch.revision_history()), revinfo)
        else:
            self.assertEquals(RevisionInfo.from_revision_id(branch,
                branch.last_revision()), revinfo)

    def test_invalid_revnum(self):
        revspec = RevisionSpec.from_string("svn:foo")
        repos_url = self.make_svn_repository("a")

        dc = self.get_commit_editor(repos_url)
        dc.add_file("bar").modify()
        dc.close()

        branch = Branch.open(repos_url)

        self.assertRaises(InvalidRevisionSpec, revspec._match_on, branch, None)

    def test_oor_revnum(self):
        """Out-of-range revnum."""
        revspec = RevisionSpec.from_string("svn:24")
        repos_url = self.make_svn_repository("a")

        dc = self.get_commit_editor(repos_url)
        dc.add_file("bar").modify()
        dc.close()

        branch = Branch.open(repos_url)

        self.assertRaises(InvalidRevisionSpec, revspec._match_on, branch, None)

    def test_non_svn_branch(self):
        revspec = RevisionSpec.from_string("svn:0")
        branch = ControlDir.create_standalone_workingtree("a").branch
        self.assertRaises(BzrError, revspec._match_on, branch, None)
