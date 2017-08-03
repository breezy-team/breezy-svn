# Copyright (C) 2007-2009 Jelmer Vernooij <jelmer@samba.org>

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

"""Config tests."""

from breezy import bzrdir
from breezy.branch import Branch
from breezy.repository import Repository
from breezy.tests import test_config
from breezy.plugins.svn.config import (
    NoSubversionBuildPackageConfig,
    PropertyConfig,
    SubversionBuildPackageConfig,
    SubversionStore,
    SvnBranchStack,
    UUIDMatcher,
    )
from breezy.plugins.svn.mapping3.scheme import TrunkBranchingScheme
from breezy.plugins.svn.tests import SubversionTestCase


def build_subversion_store(test):
    return SubversionStore()

def build_svn_branch_stack(test):
    """See bt.test_config.build_backing_branch."""
    if getattr(test, 'backing_branch', None) is None:
        # First call, let's build the branch on disk
        repo = test.make_repository('branch', format='subversion')
        test.backing_branch = bzrdir.BzrDir.open(repo.base
                                                 ).create_branch(lossy=False)
    b = Branch.open('branch')
    return SvnBranchStack(b.base, b.repository.uuid)


class TestReadonlySubversionStore(test_config.TestReadonlyStore):

    def setUp(self):
        super(TestReadonlySubversionStore, self).setUp()
        self.get_store = build_subversion_store


class TestMutableSubversionStore(test_config.TestMutableStore):

    def setUp(self):
        super(TestMutableSubversionStore, self).setUp()
        self.store_id = 'subversion'
        self.get_store = build_subversion_store


class TestUUIDMatcherStoreLoading(test_config.TestSectionMatcher):

    def setUp(self):
        super(TestUUIDMatcherStoreLoading, self).setUp()
        self.get_store = build_subversion_store
        self.matcher = UUIDMatcher


class TestUUIDMatching(test_config.TestNameMatcher):

    def setUp(self):
        super(TestUUIDMatching, self).setUp()
        self.get_store = build_subversion_store
        self.matcher = UUIDMatcher


class TestConcurrentStoreUpdates(test_config.TestConcurrentStoreUpdates):

    def setUp(self):
        self.get_stack = build_svn_branch_stack
        super(TestConcurrentStoreUpdates, self).setUp()


class TestConcreteStacks(test_config.TestConcreteStacks):

    def setUp(self):
        self.get_stack = build_svn_branch_stack
        super(TestConcreteStacks, self).setUp()


class TestStackGet(test_config.TestStackGet):

    def setUp(self):
        self.get_stack = build_svn_branch_stack
        super(TestStackGet, self).setUp()


class TestStackGetWithConverter(test_config.TestStackGetWithConverter):

    def setUp(self):
        self.get_stack = build_svn_branch_stack
        super(TestStackGetWithConverter, self).setUp()


class TestStackSet(test_config.TestStackSet):

    def setUp(self):
        self.get_stack = build_svn_branch_stack
        super(TestStackSet, self).setUp()


class TestStackRemove(test_config.TestStackRemove):

    def setUp(self):
        self.get_stack = build_svn_branch_stack
        super(TestStackRemove, self).setUp()


class ReposConfigTests(SubversionTestCase):

    def setUp(self):
        super(ReposConfigTests, self).setUp()
        self.repos_url = self.make_repository("d")
        self.repos = Repository.open(self.repos_url)
        self.config = self.repos.get_config()

    def test_get_default_locations(self):
        c = self.config
        self.assertEquals(set([self.repos.base.rstrip("/")]), c.get_locations())

    def test_get_location_one(self):
        c = self.config
        c.add_location("foobar")
        self.assertEquals(set(["foobar", self.repos.base.rstrip("/")]), c.get_locations())

    def test_get_location_two(self):
        c = self.config
        c.add_location("foobar")
        c.add_location("brainslug")
        self.assertEquals(set(["foobar", "brainslug", self.repos.base.rstrip("/")]), c.get_locations())

    def test_get_scheme_none(self):
        c = self.config
        self.assertEquals(None, c.get_branching_scheme())

    def test_get_scheme_set(self):
        c = self.config
        c.set_branching_scheme(TrunkBranchingScheme(), None)
        self.assertEquals("trunk0", str(c.get_branching_scheme()))

    def test_get_scheme_mandatory_none(self):
        c = self.config
        self.assertEquals(False, c.branching_scheme_is_mandatory())

    def test_get_scheme_mandatory_set(self):
        c = self.config
        c.set_branching_scheme(TrunkBranchingScheme(), None, mandatory=True)
        self.assertEquals(True, c.branching_scheme_is_mandatory())
        c.set_branching_scheme(TrunkBranchingScheme(), None, mandatory=False)
        self.assertEquals(False, c.branching_scheme_is_mandatory())

    def test_default_mapping(self):
        c = self.config
        self.assertEquals(None, c.get_default_mapping())
        c.set_user_option("default-mapping", "v8")
        self.assertEquals("v8", c.get_default_mapping())

    def test_use_cache(self):
        c = self.config
        self.assertEquals(None, c.get_use_cache())
        c.set_user_option("use-cache", "True")
        self.assertEquals(set(["log", "revids", "fileids", "revinfo"]), c.get_use_cache())
        c.set_user_option("use-cache", ["log", "revids"])
        self.assertEquals(set(["log", "revids"]), c.get_use_cache())
        c.set_user_option("use-cache", "False")
        self.assertEquals(set([]), c.get_use_cache())


class BranchConfigTests(SubversionTestCase):

    def setUp(self):
        super(BranchConfigTests, self).setUp()
        self.config = self.make_svn_branch("d").get_config()

    def test_has_explicit_nickname(self):
        self.assertEquals(False, self.config.has_explicit_nickname())

class BranchConfigStackTests(SubversionTestCase):

    def setUp(self):
        super(BranchConfigStackTests, self).setUp()
        self.config = self.make_svn_branch("d").get_config_stack()

    def test_set_option(self):
        self.config.set("append_revisions_only", "True")
        self.assertEquals(True, self.config.get('append_revisions_only'))

    def test_log_strip_trailing_newline(self):
        self.assertEquals(False, self.config.get('log-strip-trailing-newline'))
        self.config.set("log-strip-trailing-newline", "True")
        self.assertEquals(True, self.config.get('log-strip-trailing-newline'))
        self.config.set("log-strip-trailing-newline", "False")
        self.assertEquals(False, self.config.get('log-strip-trailing-newline'))

    def test_override_revprops(self):
        self.assertEquals(None, self.config.get('override-svn-revprops'))
        self.config.set("override-svn-revprops", "True")
        self.assertEquals(["svn:date", "svn:author"], self.config.get('override-svn-revprops'))
        self.config.set("override-svn-revprops", "False")
        self.assertEquals([], self.config.get('override-svn-revprops'))
        self.config.set("override-svn-revprops", "svn:author,svn:date")
        self.assertEquals(["svn:author", "svn:date"], self.config.get('override-svn-revprops'))
        self.config.set("override-svn-revprops", "svn:author")
        self.assertEquals(["svn:author"], self.config.get('override-svn-revprops'))
        self.config.set("override-svn-revprops", "svn:author=author")
        self.assertEquals(["svn:author=author"], self.config.get('override-svn-revprops'))
        self.config.set("override-svn-revprops", "svn:author=committer")
        self.assertEquals(["svn:author=committer"], self.config.get('override-svn-revprops'))

    def test_get_append_revisions_only(self):
        self.assertEquals(None, self.config.get('append_revisions_only'))
        self.config.set("append_revisions_only", "True")
        self.assertEquals(True, self.config.get('append_revisions_only'))
        self.config.set("append_revisions_only", "False")
        self.assertEquals(False, self.config.get('append_revisions_only'))

    def test_get_branches(self):
        c = self.config
        self.assertIs(None, c.get('branches'))
        c.set("branches", "bla;blie")
        self.assertEquals(["bla", "blie"], c.get('branches'))

    def test_get_tags(self):
        c = self.config
        c.set("tags", "bla;blie")
        self.assertEquals(["bla", "blie"], c.get('tags'))

    def test_tags(self):
        c = self.config
        c.set("tags", "bla;blie")
        self.assertEquals(["bla", "blie"], c.get('tags'))


class PropertyConfigTests(SubversionTestCase):

    def test_getitem(self):
        repos_url = self.make_repository("d")

        dc = self.get_commit_editor(repos_url)
        f = dc.add_file("foo")
        f.change_prop("bla", "bar")
        f.modify()
        dc.close()

        repos = Repository.open(repos_url)

        cfg = PropertyConfig(repos.revision_tree(repos.generate_revision_id(1, "", repos.get_mapping())), "foo")

        self.assertEquals("bar", cfg["bla"])


class SvnBpConfigTests(SubversionTestCase):

    def test_no_debian_dir(self):
        branch = self.make_svn_branch("d")
        self.assertRaises(NoSubversionBuildPackageConfig,
                SubversionBuildPackageConfig, branch.basis_tree())

    def test_mergeWithUpstream(self):
        branch = self.make_svn_branch("d")

        dc = self.get_commit_editor(branch.base)
        f = dc.add_dir("debian")
        f.change_prop("mergeWithUpstream", "1")
        dc.close()

        cfg = SubversionBuildPackageConfig(branch.basis_tree())

        self.assertEquals(True, cfg.get_merge_with_upstream())

    def test_get_property_val(self):
        branch = self.make_svn_branch("d")

        dc = self.get_commit_editor(branch.base)
        f = dc.add_dir("debian")
        f.change_prop("svn-bp:origDir", "myorigdir")
        dc.close()

        cfg = SubversionBuildPackageConfig(branch.basis_tree())

        self.assertEquals("myorigdir", cfg.get("origDir"))

    def test_get_intree_val(self):
        branch = self.make_svn_branch("d")

        dc = self.get_commit_editor(branch.base)
        d = dc.add_dir("debian")
        f = d.add_file("debian/svn-layout")
        f.modify("origDir = aorigdir\n")
        dc.close()

        cfg = SubversionBuildPackageConfig(branch.basis_tree())

        self.assertEquals("aorigdir", cfg.get("origDir"))

    def test_get_controldir_val(self):
        tree = self.make_svn_branch_and_tree("d", "dc")

        f = open('dc/.svn/svn-layout', 'w')
        f.write("buildArea = build-gebied\n")
        f.close()

        cfg = SubversionBuildPackageConfig(tree)

        self.assertEquals("build-gebied", cfg.get("buildArea"))
