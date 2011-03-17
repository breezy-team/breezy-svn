# -*- coding: utf-8 -*-

# Copyright (C) 2006-2011 Jelmer Vernooij <jelmer@samba.org>
# Copyright (C) 2011 Canonical Ltd.

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

"""Subversion repository tests."""

from bzrlib.branch import (
    Branch,
    )
from bzrlib.bzrdir import (
    BzrDir,
    format_registry,
    )
from bzrlib.config import GlobalConfig
from bzrlib.errors import (
    AppendRevisionsOnlyViolation,
    BadConversionTarget,
    UninitializableFormat,
    )
from bzrlib.repository import Repository
from bzrlib.tests import TestCase

from bzrlib.plugins.svn.tests import (
    SubversionTestCase,
    )
from bzrlib.plugins.svn.branch import (
    SvnBranch,
    )
from bzrlib.plugins.svn.layout.standard import TrunkLayout
from bzrlib.plugins.svn.repository import (
    SvnRepositoryFormat,
    )

import subvertpy.repos


class TestSubversionRepositoryWorks(SubversionTestCase):
    """Generic Subversion Repository tests."""

    def setUp(self):
        super(TestSubversionRepositoryWorks, self).setUp()
        self.repos_path = 'a'
        self.repos_url = self.make_repository(self.repos_path)

    def test_get_config_global_set(self):
        cfg = GlobalConfig()
        cfg.set_user_option("foo", "Still Life")

        repos = Repository.open(self.repos_url)
        self.assertEquals("Still Life",
                repos.get_config().get_user_option("foo"))

    def test_get_config(self):
        repos = Repository.open(self.repos_url)
        repos.get_config().set_user_option("foo", "Van Der Graaf Generator")

        repos = Repository.open(self.repos_url)
        self.assertEquals("Van Der Graaf Generator",
                repos.get_config().get_user_option("foo"))

    def test_repr(self):
        dc = self.get_commit_editor(self.repos_url)
        dc.add_file("foo").modify("data")
        dc.close()

        repos = Repository.open(self.repos_url)

        self.assertEqual("SvnRepository('%s')" % self.repos_url, repos.__repr__())

    def test_gather_stats(self):
        repos = Repository.open(self.repos_url)
        stats = repos.gather_stats()
        self.assertEquals(1, stats['revisions'])
        self.assertTrue(stats.has_key("firstrev"))
        self.assertTrue(stats.has_key("latestrev"))
        self.assertFalse(stats.has_key('committers'))

    def test_uuid(self):
        """ Test UUID is retrieved correctly """
        fs = self.open_fs(self.repos_path)
        repository = Repository.open(self.repos_url)
        self.assertEqual(fs.get_uuid(), repository.uuid)

    def test_is_shared(self):
        dc = self.get_commit_editor(self.repos_url)
        foo = dc.add_dir("foo")
        bla = foo.add_file("foo/bla").modify("data")
        dc.close()

        repository = Repository.open(self.repos_url)
        self.assertTrue(repository.is_shared())

    def test_format(self):
        """ Test repository format is correct """
        self.make_checkout(self.repos_url, 'ac')
        bzrdir = BzrDir.open("ac")
        self.assertRaises(NotImplementedError,
                bzrdir._format.get_format_string)
        self.assertEqual(bzrdir._format.get_format_description(),
                "Subversion Local Checkout")

    def test_make_working_trees(self):
        repos = Repository.open(self.repos_url)
        self.assertFalse(repos.make_working_trees())

    def test_get_physical_lock_status(self):
        repos = Repository.open(self.repos_url)
        self.assertFalse(repos.get_physical_lock_status())

    def test_seen_bzr_revprops(self):
        repos = Repository.open(self.repos_url)
        dc = self.get_commit_editor(self.repos_url)
        dc.add_dir("foo")
        dc.close()

        self.assertFalse(repos.seen_bzr_revprops())


class SvnRepositoryFormatTests(TestCase):

    def setUp(self):
        TestCase.setUp(self)
        self.format = SvnRepositoryFormat()

    def test_initialize(self):
        self.assertRaises(UninitializableFormat, self.format.initialize, None)

    def test_get_format_description(self):
        self.assertEqual("Subversion Repository",
                         self.format.get_format_description())

    def test_conversion_target_self(self):
        self.format.check_conversion_target(self.format)

    def test_conversion_target_incompatible(self):
        self.assertRaises(BadConversionTarget,
                          self.format.check_conversion_target,
                          format_registry.make_bzrdir('weave').repository_format)

    def test_conversion_target_compatible(self):
        self.format.check_conversion_target(
          format_registry.make_bzrdir('rich-root').repository_format)


class ForeignTestsRepositoryFactory(object):

    def make_repository(self, transport):
        subvertpy.repos.create(transport.local_abspath("."))
        return BzrDir.open_from_transport(transport).open_repository()


class GetCommitBuilderTests(SubversionTestCase):

    def setUp(self):
        super(GetCommitBuilderTests, self).setUp()
        self.repos_url = self.make_repository("d")
        dc = self.get_commit_editor(self.repos_url)
        dc.add_dir('trunk')
        dc.close()
        self.branch = Branch.open("d/trunk")

    def test_simple(self):
        cb = self.branch.get_commit_builder([self.branch.last_revision()])
        cb.commit("MSG")

        log = self.client_log(self.repos_url, 0, 2)
        self.assertEquals("MSG", log[2][3])
        self.assertEquals({"/trunk": ('M', None, -1)}, log[2][0])

    def test_diverged(self):
        cb = self.get_commit_editor(self.repos_url)
        branches = cb.add_dir("branches")
        branches.add_dir("branches/dir")
        cb.close()

        otherrevid = self.branch.repository.generate_revision_id(2, "branches/dir",
            self.branch.mapping)

        other_tree = self.branch.repository.revision_tree(otherrevid)

        self.branch.get_config().set_user_option("append_revisions_only", "False")

        cb = self.branch.get_commit_builder([otherrevid])
        list(cb.record_iter_changes(other_tree, otherrevid, []))
        cb.finish_inventory()
        cb.commit("MSG")

        log = self.client_log(self.repos_url, 0, 3)
        self.assertEquals("MSG", log[3][3])
        self.assertEquals({"/trunk": ('R', '/branches/dir', 2)}, log[3][0])

    def test_append_only(self):
        cb = self.get_commit_editor(self.repos_url)
        branches = cb.add_dir("branches")
        branches.add_dir("branches/dir")
        cb.close()

        otherrevid = self.branch.repository.generate_revision_id(2, "branches/dir",
            self.branch.mapping)

        self.assertRaises(AppendRevisionsOnlyViolation,
            self.branch.get_commit_builder,
            [otherrevid, self.branch.last_revision()])

    def test_create_new_branch(self):
        self.branch.repository.set_layout(TrunkLayout())

        cb = self.branch.get_commit_builder([])
        list(cb.record_iter_changes(self.branch.repository.revision_tree("null:"),
            "null:", [("rootid", (None, ""), (False, True), (False, True),
             (None, None), ("", ""), (None, "directory"), (None, False))]))
        cb.finish_inventory()
        cb.commit("FOO")

        log = self.client_log(self.repos_url, 0, 2)
        self.assertEquals("FOO", log[2][3])
        self.assertEquals({"/trunk": ('R', None, -1)}, log[2][0])

    def test_based_on_older(self):
        first_rev = self.branch.last_revision()

        self.branch = Branch.open("d/trunk")

        cb = self.get_commit_editor(self.repos_url + "/trunk")
        cb.add_dir("foo")
        cb.close()

        self.branch.repository.set_layout(TrunkLayout())

        self.branch.get_config().set_user_option("append_revisions_only", "False")

        cb = self.branch.get_commit_builder([first_rev])
        cb.finish_inventory()
        cb.commit("FOO")

        log = self.client_log(self.repos_url, 0, 3)
        self.assertEquals("FOO", log[3][3])
        self.assertEquals({"/trunk": ('R', "/trunk", 1)}, log[3][0])
