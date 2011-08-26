# Copyright (C) 2011 Jelmer Vernooij <jelmer@samba.org>
 
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

from bzrlib.plugins.svn.tests import (
    SubversionTestCase,
    )

from bzrlib.plugins.svn.layout.guess import is_likely_branch_url


class URLIsLikelyBranch(SubversionTestCase):

    def test_root(self):
        repo_url = self.make_repository("d")
        self.assertIs(None, is_likely_branch_url(repo_url))
        self.assertFalse(is_likely_branch_url(repo_url+"/doesntexist"))

    def test_trunk(self):
        repo_url = self.make_repository("d")
        ce = self.get_commit_editor(repo_url)
        ce.add_dir("trunk")
        ce.close()
        self.assertFalse(is_likely_branch_url(repo_url))
        self.assertTrue(is_likely_branch_url(repo_url+"/trunk"))

    def test_file_in_trunk(self):
        repo_url = self.make_repository("d")
        ce = self.get_commit_editor(repo_url)
        trunk = ce.add_dir("trunk")
        trunk.add_dir("trunk/somedir")
        ce.close()
        self.assertFalse(is_likely_branch_url(repo_url))
        self.assertTrue(is_likely_branch_url(repo_url+"/trunk"))
        self.assertFalse(is_likely_branch_url(repo_url+"/trunk/somedir"))
