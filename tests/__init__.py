# Copyright (C) 2006-2009 Jelmer Vernooij <jelmer@samba.org>

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

"""Tests for the bzr-svn plugin."""

from subvertpy import NODE_UNKNOWN
import subvertpy.tests

from bzrlib import osutils
from bzrlib.bzrdir import (
    BzrDir,
    )
from bzrlib.tests import (
    TestCaseInTempDir,
    )

from bzrlib.plugins.svn.transport import (
    svn_to_bzr_url,
    )


class SubversionTestCase(subvertpy.tests.SubversionTestCase,TestCaseInTempDir):

    def make_repository(self, relpath, allow_revprop_changes=True):
        """Create an SVN repository.

        :param relpath: Relative path at which to create the repository.
        :param allow_revprop_changes: Allow changes to SVN revision
            properties.
        :return: A bzr-friendly URL for the created repository.
        """
        return svn_to_bzr_url(
            subvertpy.tests.SubversionTestCase.make_repository(self,
                relpath, allow_revprop_changes))

    def setUp(self):
        subvertpy.tests.SubversionTestCase.setUp(self)
        subvertpy.tests.SubversionTestCase.tearDown(self)
        TestCaseInTempDir.setUp(self)
        if type(self.test_dir) == unicode:
            self.test_dir = self.test_dir.encode(osutils._fs_enc)

    def tearDown(self):
        TestCaseInTempDir.tearDown(self)

    def make_local_bzrdir(self, repos_path, relpath):
        """Create a repository and checkout."""

        repos_url = self.make_repository(repos_path)
        self.make_checkout(repos_url, relpath)

        return BzrDir.open(relpath)

    def make_client_and_bzrdir(self, repospath, clientpath):
        repos_url = self.make_client(repospath, clientpath)

        return BzrDir.open(repos_url)

    def assertChangedPathEquals(self, expected, got, msg=None):
        if expected[:3] == got[:3] and got[3] in (expected[3], NODE_UNKNOWN):
            return
        self.assertEquals(expected, got, msg)

    def assertChangedPathsEquals(self, expected, got, msg=None):
        self.assertIsInstance(expected, dict)
        self.assertIsInstance(got, dict)
        if len(expected) != len(got):
            self.assertEquals(expected, got, msg)
        for p, v1 in expected.iteritems():
            try:
                v2 = got[p]
            except KeyError:
                self.assertEquals(expected, got, msg)
            self.assertChangedPathEquals(v1, v2, msg)

    def assertBranchLogEquals(self, expected, got, msg=None):
        if len(expected) != len(got):
            self.assertEquals(expected, got, msg)
        for (root1, changes1, revnum1), (root2, changes2, revnum2) in zip(expected, got):
            self.assertEquals(revnum1, revnum2)
            self.assertEquals(root1, root2)
            self.assertChangedPathsEquals(changes1, changes2, msg)


def test_suite():
    from unittest import TestSuite
    from bzrlib.tests import TestUtil

    loader = TestUtil.TestLoader()

    suite = TestSuite()

    testmod_names = [
            'test_branch',
            'test_branchprops',
            'test_cache',
            'test_changes',
            'test_checkout',
            'test_commit',
            'test_config',
            'test_convert',
            'test_errors',
            'test_fetch',
            'test_fileids',
            'test_keywords',
            'layout.test_custom',
            'layout.test_standard',
            'test_logwalker',
            'test_mapping',
            'test_parents',
            'test_push',
            'test_radir',
            'test_repository',
            'test_revmeta',
            'test_revspec',
            'test_send',
            'test_svk',
            'test_transport',
            'test_tree',
            'test_versionedfiles',
            'test_workingtree',
            'test_blackbox',
            'mapping_implementations',
            'mapping3',
            'mapping3.test_scheme']
    suite.addTest(loader.loadTestsFromModuleNames(["%s.%s" % (__name__, i) for i in testmod_names]))

    return suite
