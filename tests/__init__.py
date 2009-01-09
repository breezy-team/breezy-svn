# Copyright (C) 2006-2007 Jelmer Vernooij <jelmer@samba.org>

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

"""Tests for the bzr-svn plugin."""

import os
import sys
import bzrlib

from bzrlib.bzrdir import BzrDir
from bzrlib.tests import TestCaseInTempDir
from bzrlib.trace import mutter
from bzrlib.workingtree import WorkingTree

from bzrlib import osutils, urlutils
from bzrlib.plugins.svn import cache

import subvertpy.tests

class SubversionTestCase(subvertpy.tests.SubversionTestCase,TestCaseInTempDir):

    def make_repository(self, *args, **kwargs):
        return subvertpy.tests.SubversionTestCase.make_repository(self, *args, **kwargs)

    def setUp(self):
        subvertpy.tests.SubversionTestCase.setUp(self)
        subvertpy.tests.SubversionTestCase.tearDown(self)
        TestCaseInTempDir.setUp(self)
        self._old_connect_cachefile = cache.connect_cachefile
        cache.connect_cachefile = lambda path: cache.sqlite3.connect(":memory:")

    def tearDown(self):
        TestCaseInTempDir.tearDown(self)
        cache.connect_cachefile = self._old_connect_cachefile

    def make_local_bzrdir(self, repos_path, relpath):
        """Create a repository and checkout."""

        repos_url = self.make_repository(repos_path)
        self.make_checkout(repos_url, relpath)

        return BzrDir.open(relpath)

    def make_client_and_bzrdir(self, repospath, clientpath):
        repos_url = self.make_client(repospath, clientpath)

        return BzrDir.open("svn+%s" % repos_url)


def test_suite():
    from unittest import TestSuite
    
    from bzrlib.tests import TestUtil

    loader = TestUtil.TestLoader()

    suite = TestSuite()

    testmod_names = [
            'test_branch', 
            'test_branchprops', 
            'test_changes',
            'test_checkout',
            'test_commit',
            'test_config',
            'test_convert',
            'test_errors',
            'test_fetch',
            'test_fileids', 
            'test_layout',
            'test_logwalker',
            'test_mapping',
            'test_parents',
            'test_push',
            'test_radir',
            'test_repository', 
            'test_revids',
            'test_revmeta',
            'test_revspec',
            'test_svk',
            'test_transport',
            'test_tree',
            'test_upgrade',
            'test_versionedfiles',
            'test_workingtree',
            'test_blackbox',
            'mapping_implementations',
            'mapping3',
            'mapping3.test_scheme']
    suite.addTest(loader.loadTestsFromModuleNames(["%s.%s" % (__name__, i) for i in testmod_names]))
    suite.addTest(loader.loadTestsFromModuleNames(["bzrlib.plugins.svn.foreign.test_versionedfiles"]))
    suite.addTest(loader.loadTestsFromModuleNames(["subvertpy.tests.%s" % name for name in 'test_wc', 'test_client', 'test_ra', 'test_repos', 'test_core', 'test_delta', 'test_properties', 'test_marshall']))

    return suite
