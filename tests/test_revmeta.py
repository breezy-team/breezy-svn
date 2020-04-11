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

from subvertpy import (
    NODE_DIR,
    NODE_FILE,
    )
from subvertpy.ra import (
    Auth,
    RemoteAccess,
    get_username_provider,
    )
from subvertpy.tests import TestCommitEditor

from breezy.repository import Repository

from breezy.plugins.svn.mapping import (
    SVN_REVPROP_BZR_BASE_REVISION,
    SVN_REVPROP_BZR_REPOS_UUID,
    SVN_REVPROP_BZR_ROOT,
    SVN_REVPROP_BZR_MAPPING_VERSION,
    mapping_registry,
    )
from breezy.plugins.svn.mapping import (
    SVN_REVPROP_BZR_TESTAMENT,
    )
from breezy.plugins.svn.tests import SubversionTestCase


class TestWithRepository(SubversionTestCase):

    def make_provider(self, repos_url):
        r = Repository.open(repos_url)
        return r._revmeta_provider

    def test_checks_uuid(self):
        repos_url = self.make_svn_repository('d')

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("bp")
        dc.close()

        mapping = mapping_registry.get_default()()
        ra = RemoteAccess(repos_url.encode("utf-8"),
                          auth=Auth([get_username_provider()]))
        revnum = ra.get_latest_revnum()
        revprops = { SVN_REVPROP_BZR_REPOS_UUID: "otheruuid",
                    "svn:log": "bla",
                    SVN_REVPROP_BZR_ROOT: "bp",
                    SVN_REVPROP_BZR_MAPPING_VERSION: mapping.name,
                    SVN_REVPROP_BZR_BASE_REVISION: "therealbaserevid" }
        dc = TestCommitEditor(ra.get_commit_editor(revprops), ra.url, revnum)
        dc.open_dir("bp").add_file("bp/la").modify()
        dc.close()

        repos = Repository.open(repos_url)

        revmeta1 = repos._revmeta_provider.get_revision(u"bp", 1)
        revmeta2 = repos._revmeta_provider.get_revision(u"bp", 2)

        self.assertEquals(
            mapping.revision_id_foreign_to_bzr((repos.uuid, "bp", 1)),
            revmeta2.get_lhs_parent_revid(mapping, revmeta1))

    def test_get_testament(self):
        repos_url = self.make_svn_repository('d')

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("trunk")
        dc.close()

        self.client_set_revprop(repos_url, 1, SVN_REVPROP_BZR_TESTAMENT, "data\n")

        dc = self.get_commit_editor(repos_url)
        dc.close()

        repos = Repository.open(repos_url)

        revmeta1 = repos._revmeta_provider.get_revision(u"", 1)
        self.assertEquals("data\n", revmeta1.get_testament())

        revmeta2 = repos._revmeta_provider.get_revision(u"", 2)
        self.assertIs(None, revmeta2.get_testament())

    def test_get_changed_properties(self):
        repos_url = self.make_svn_repository('d')

        dc = self.get_commit_editor(repos_url)
        dc.change_prop("myprop", "data\n")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.change_prop("myprop", "newdata\n")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.change_prop("myp2", "newdata\n")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.change_prop("myp2", None)
        dc.close()

        repos = Repository.open(repos_url)

        revmeta1 = repos._revmeta_provider.get_revision(u"", 1)
        revmeta2 = repos._revmeta_provider.get_revision(u"", 2)
        revmeta3 = repos._revmeta_provider.get_revision(u"", 3)
        revmeta4 = repos._revmeta_provider.get_revision(u"", 4)

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

        self.assertEquals(("newdata\n", None),
                          revmeta4.get_changed_fileprops().get("myp2", ("newdata\n", None)))

    def test_changes_branch_root(self):
        repos_url = self.make_svn_repository('d')

        dc = self.get_commit_editor(repos_url)
        dc.change_prop("myprop", "data\n")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.add_file("bar").modify(b"bloe")
        dc.close()

        repos = Repository.open(repos_url)

        revmeta1 = repos._revmeta_provider.get_revision(u"", 1)
        revmeta2 = repos._revmeta_provider.get_revision(u"", 2)

        self.assertTrue(revmeta1.changes_branch_root())
        self.assertFalse(revmeta2.changes_branch_root())

    def test_paths(self):
        repos_url = self.make_svn_repository('d')

        dc = self.get_commit_editor(repos_url)
        dc.change_prop("myprop", "data\n")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.add_file("bar").modify(b"bloe")
        dc.close()

        repos = Repository.open(repos_url)

        revmeta1 = repos._revmeta_provider.get_revision(u"", 1)
        revmeta2 = repos._revmeta_provider.get_revision(u"", 2)

        self.assertChangedPathsEquals({u"": ("M", None, -1, NODE_DIR)}, 
                          revmeta1.metarev.paths)
        self.assertChangedPathsEquals({u"bar": ("A", None, -1, NODE_FILE)},
                          revmeta2.metarev.paths)

    def test_foreign_revid(self):
        repos_url = self.make_svn_repository('d')

        dc = self.get_commit_editor(repos_url)
        dc.change_prop("myprop", "data\n")
        dc.close()

        provider = self.make_provider(repos_url)

        revmeta1 = provider.get_revision(u"", 1)

        self.assertEquals((provider.repository.uuid, "", 1),
                revmeta1.metarev.get_foreign_revid())

    def test_revprops(self):
        repos_url = self.make_svn_repository('d')

        dc = self.get_commit_editor(repos_url)
        dc.change_prop("myprop", "data\n")
        dc.close()

        provider = self.make_provider(repos_url)
        revmeta1 = provider.get_revision(u"", 1)
        self.assertEquals(set(["svn:date", "svn:author", "svn:log"]),
                          set(revmeta1.metarev.revprops.keys()))

    def test_is_changes_root(self):
        repos_url = self.make_svn_repository('d')

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("bloe")
        dc.close()

        provider = self.make_provider(repos_url)
        revmeta1 = provider.get_revision(u"", 1)
        self.assertFalse(revmeta1.metarev.is_changes_root())
        revmeta1 = provider.get_revision(u"bloe", 1)
        self.assertTrue(revmeta1.metarev.is_changes_root())
