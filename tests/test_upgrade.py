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

"""Mapping upgrade tests."""

from bzrlib.bzrdir import (
    BzrDir,
    )
from bzrlib.errors import (
    IncompatibleAPI,
    )
from bzrlib.repository import (
    Repository,
    )
from bzrlib.tests import (
    TestCase,
    TestSkipped,
    )

from bzrlib.plugins.svn.format import (
    get_rich_root_format,
    )
from bzrlib.plugins.svn.layout.standard import (
    RootLayout,
    )
from bzrlib.plugins.svn.mapping import (
    foreign_vcs_svn,
    )
from bzrlib.plugins.svn.mapping3.base import (
    BzrSvnMappingv3,
    )
from bzrlib.plugins.svn.mapping3.scheme import (
    TrunkBranchingScheme,
    )
from bzrlib.plugins.svn.tests import (
    SubversionTestCase,
    )


def import_upgrade():
    try:
        from bzrlib.plugins.rebase import upgrade
    except IncompatibleAPI, e:
        raise TestSkipped(e)
    except ImportError, e:
        raise TestSkipped(e)
    return upgrade


class UpgradeTests(SubversionTestCase):

    def test_no_custom(self):
        upgrade = import_upgrade()
        repos_url = self.make_repository("a")

        dc = self.get_commit_editor(repos_url)
        dc.add_file("a").modify("b")
        dc.close()

        oldrepos = Repository.open(repos_url)
        oldrepos.set_layout(RootLayout())
        dir = BzrDir.create("f", format=get_rich_root_format())
        newrepos = dir.create_repository()
        oldrepos.copy_content_into(newrepos)
        dir.create_branch()
        wt = dir.create_workingtree()
        file("f/a","w").write("b")
        wt.add("a")
        wt.commit(message="data", rev_id="svn-v1:1@%s-" % oldrepos.uuid)

        self.assertTrue(newrepos.has_revision("svn-v1:1@%s-" % oldrepos.uuid))

        mapping = oldrepos.get_mapping()
        upgrade.upgrade_repository(newrepos, oldrepos, new_mapping=mapping, allow_changes=True)

        self.assertTrue(newrepos.has_revision(oldrepos.generate_revision_id(1, "", mapping)))

    def test_single_custom(self):
        upgrade = import_upgrade()
        repos_url = self.make_repository("a")

        dc = self.get_commit_editor(repos_url)
        dc.add_file("a").modify("b")
        dc.close()

        oldrepos = Repository.open(repos_url)
        oldrepos.set_layout(RootLayout())
        dir = BzrDir.create("f", format=get_rich_root_format())
        newrepos = dir.create_repository()
        oldrepos.copy_content_into(newrepos)
        dir.create_branch()
        wt = dir.create_workingtree()
        file("f/a", "w").write("b")
        wt.add("a")
        wt.commit(message="data", rev_id="svn-v1:1@%s-" % oldrepos.uuid)
        file("f/a", 'w').write("moredata")
        wt.commit(message='fix moredata', rev_id="customrev")

        mapping = oldrepos.get_mapping()
        upgrade.upgrade_repository(newrepos, oldrepos, new_mapping=mapping, allow_changes=True)

        self.assertTrue(newrepos.has_revision(oldrepos.generate_revision_id(1, "", mapping)))
        self.assertTrue(newrepos.has_revision("customrev%s-upgrade" % mapping.upgrade_suffix))
        newrepos.lock_read()
        self.assertTrue((oldrepos.generate_revision_id(1, "", mapping),),
                        tuple(newrepos.get_revision("customrev%s-upgrade" % mapping.upgrade_suffix).parent_ids))
        newrepos.unlock()

    def test_single_keep_parent_fileid(self):
        upgrade = import_upgrade()
        repos_url = self.make_repository("a")

        dc = self.get_commit_editor(repos_url)
        dc.add_file("a").modify("b")
        dc.close()

        oldrepos = Repository.open(repos_url)
        oldrepos.set_layout(RootLayout())
        dir = BzrDir.create("f", format=get_rich_root_format())
        newrepos = dir.create_repository()
        oldrepos.copy_content_into(newrepos)
        dir.create_branch()
        wt = dir.create_workingtree()
        file("f/a", "w").write("b")
        wt.add(["a"], ["someid"])
        wt.commit(message="data", rev_id="svn-v1:1@%s-" % oldrepos.uuid)
        wt.rename_one("a", "b")
        file("f/a", 'w').write("moredata")
        wt.add(["a"], ["specificid"])
        wt.commit(message='fix moredata', rev_id="customrev")

        mapping = oldrepos.get_mapping()
        upgrade.upgrade_repository(newrepos, oldrepos, new_mapping=mapping, allow_changes=True)
        tree = newrepos.revision_tree("customrev%s-upgrade" % mapping.upgrade_suffix)
        self.assertEqual("specificid", tree.inventory.path2id("a"))
        self.assertEqual(mapping.generate_file_id((oldrepos.uuid, "", 1), u"a"), 
                         tree.inventory.path2id("b"))

    def test_single_custom_continue(self):
        upgrade = import_upgrade()
        repos_url = self.make_repository("a")

        dc = self.get_commit_editor(repos_url)
        dc.add_file("a").modify("b")
        dc.add_file("b").modify("c")
        dc.close()

        oldrepos = Repository.open(repos_url)
        oldrepos.set_layout(RootLayout())
        dir = BzrDir.create("f", format=get_rich_root_format())
        newrepos = dir.create_repository()
        oldrepos.copy_content_into(newrepos)
        dir.create_branch()
        wt = dir.create_workingtree()
        file("f/a", "w").write("b")
        file("f/b", "w").write("c")
        wt.add("a")
        wt.add("b")
        wt.commit(message="data", rev_id="svn-v1:1@%s-" % oldrepos.uuid)
        file("f/a", 'w').write("moredata")
        file("f/b", 'w').write("moredata")
        wt.commit(message='fix moredata', rev_id="customrev")

        tree = newrepos.revision_tree("svn-v1:1@%s-" % oldrepos.uuid)

        newrepos.lock_write()
        newrepos.start_write_group()

        mapping = oldrepos.get_mapping()
        fileid = tree.inventory.path2id("a")
        revid = "customrev%s-upgrade" % mapping.upgrade_suffix
        newrepos.texts.add_lines((fileid, revid), 
                [(fileid, "svn-v1:1@%s-" % oldrepos.uuid)],
                tree.get_file(fileid).readlines())

        newrepos.commit_write_group()
        newrepos.unlock()

        upgrade.upgrade_repository(newrepos, oldrepos, new_mapping=mapping, 
                           allow_changes=True)

        self.assertTrue(newrepos.has_revision(oldrepos.generate_revision_id(1, "", mapping)))
        self.assertTrue(newrepos.has_revision("customrev%s-upgrade" % mapping.upgrade_suffix))
        newrepos.lock_read()
        self.assertTrue((oldrepos.generate_revision_id(1, "", mapping),),
                        tuple(newrepos.get_revision("customrev%s-upgrade" % mapping.upgrade_suffix).parent_ids))
        newrepos.unlock()

    def test_more_custom(self):
        upgrade = import_upgrade()
        repos_url = self.make_repository("a")

        dc = self.get_commit_editor(repos_url)
        dc.add_file("a").modify("b")
        dc.close()

        oldrepos = Repository.open(repos_url)
        oldrepos.set_layout(RootLayout())
        dir = BzrDir.create("f", format=get_rich_root_format())
        newrepos = dir.create_repository()
        dir.create_branch()
        wt = dir.create_workingtree()
        file("f/a", "w").write("b")
        wt.add("a")
        wt.commit(message="data", rev_id="svn-v1:1@%s-" % oldrepos.uuid)
        file("f/a", 'w').write("moredata")
        wt.commit(message='fix moredata', rev_id="customrev")
        file("f/a", 'w').write("blackfield")
        wt.commit(message='fix it again', rev_id="anotherrev")

        mapping = oldrepos.get_mapping()
        renames = upgrade.upgrade_repository(newrepos, oldrepos,
            new_mapping=mapping, allow_changes=True)
        self.assertEqual({
            'svn-v1:1@%s-' % oldrepos.uuid: mapping.revision_id_foreign_to_bzr((oldrepos.uuid, "", 1)),
            "customrev": "customrev%s-upgrade" % mapping.upgrade_suffix,
            "anotherrev": "anotherrev%s-upgrade" % mapping.upgrade_suffix},
            renames)

        self.assertTrue(newrepos.has_revision(oldrepos.generate_revision_id(1, "", mapping)))
        self.assertTrue(newrepos.has_revision("customrev%s-upgrade" % mapping.upgrade_suffix))
        self.assertTrue(newrepos.has_revision("anotherrev%s-upgrade" % mapping.upgrade_suffix))
        newrepos.lock_read()
        self.assertTrue((oldrepos.generate_revision_id(1, "", mapping),),
                        tuple(newrepos.get_revision("customrev%s-upgrade" % mapping.upgrade_suffix).parent_ids))
        self.assertTrue(("customrev-%s-upgrade" % mapping.upgrade_suffix,),
                        tuple(newrepos.get_revision("anotherrev%s-upgrade" % mapping.upgrade_suffix).parent_ids))
        newrepos.unlock()

    def test_more_custom_branch(self):
        upgrade = import_upgrade()
        repos_url = self.make_repository("a")

        dc = self.get_commit_editor(repos_url)
        dc.add_file("a").modify("b")
        dc.close()

        oldrepos = Repository.open(repos_url)
        oldrepos.set_layout(RootLayout())
        dir = BzrDir.create("f", format=get_rich_root_format())
        newrepos = dir.create_repository()
        b = dir.create_branch()
        wt = dir.create_workingtree()
        file("f/a", "w").write("b")
        wt.add("a")
        wt.commit(message="data", rev_id="svn-v1:1@%s-" % oldrepos.uuid)
        file("f/a", 'w').write("moredata")
        wt.commit(message='fix moredata', rev_id="customrev")
        file("f/a", 'w').write("blackfield")
        wt.commit(message='fix it again', rev_id="anotherrev")

        upgrade.upgrade_branch(b, oldrepos, new_mapping=oldrepos.get_mapping(), allow_changes=True)
        mapping = oldrepos.get_mapping()
        self.assertEqual([oldrepos.generate_revision_id(0, "", mapping),
                          oldrepos.generate_revision_id(1, "", mapping),
                          "customrev%s-upgrade" % mapping.upgrade_suffix,
                          "anotherrev%s-upgrade" % mapping.upgrade_suffix
                          ], b.revision_history())

    def test_workingtree(self):
        upgrade = import_upgrade()
        repos_url = self.make_repository("a")

        dc = self.get_commit_editor(repos_url)
        dc.add_file("a").modify("b")
        dc.close()

        oldrepos = Repository.open(repos_url)
        oldrepos.set_layout(RootLayout())
        dir = BzrDir.create("f", format=get_rich_root_format())
        newrepos = dir.create_repository()
        b = dir.create_branch()
        wt = dir.create_workingtree()
        file("f/a", "w").write("b")
        wt.add("a")
        wt.commit(message="data", rev_id="svn-v1:1@%s-" % oldrepos.uuid)
        file("f/a", 'w').write("moredata")
        wt.commit(message='fix moredata', rev_id="customrev")
        file("f/a", 'w').write("blackfield")
        wt.commit(message='fix it again', rev_id="anotherrev")

        mapping = oldrepos.get_mapping()
        upgrade.upgrade_workingtree(wt, oldrepos, new_mapping=mapping, 
                allow_changes=True)
        self.assertEquals(wt.last_revision(), b.last_revision())
        self.assertEqual([oldrepos.generate_revision_id(0, "", mapping),
                          oldrepos.generate_revision_id(1, "", mapping),
                          "customrev%s-upgrade" % mapping.upgrade_suffix,
                          "anotherrev%s-upgrade" % mapping.upgrade_suffix
                          ], b.revision_history())

    def test_branch_none(self):
        upgrade = import_upgrade()
        repos_url = self.make_repository("a")

        dc = self.get_commit_editor(repos_url)
        dc.add_file("a").modify("b")
        dc.close()

        oldrepos = Repository.open(repos_url)
        dir = BzrDir.create("f", format=get_rich_root_format())
        dir.create_repository()
        b = dir.create_branch()
        wt = dir.create_workingtree()
        file("f/a", "w").write("b")
        wt.add("a")
        wt.commit(message="data", rev_id="blarev")
        file("f/a", 'w').write("moredata")
        wt.commit(message='fix moredata', rev_id="customrev")
        file("f/a", 'w').write("blackfield")
        wt.commit(message='fix it again', rev_id="anotherrev")

        upgrade.upgrade_branch(b, oldrepos, new_mapping=oldrepos.get_mapping())
        self.assertEqual(["blarev", "customrev", "anotherrev"],
                b.revision_history())

    def test_raise_incompat(self):
        upgrade = import_upgrade()
        repos_url = self.make_repository("a")

        dc = self.get_commit_editor(repos_url)
        dc.add_file("d").modify("e")
        dc.close()

        oldrepos = Repository.open(repos_url)
        oldrepos.set_layout(RootLayout())
        dir = BzrDir.create("f", format=get_rich_root_format())
        dir.create_repository()
        b = dir.create_branch()
        wt = dir.create_workingtree()
        file("f/a", "w").write("c")
        wt.add("a")
        wt.commit(message="data", rev_id="svn-v1:1@%s-" % oldrepos.uuid)

        self.assertRaises(upgrade.UpgradeChangesContent, lambda: upgrade.upgrade_branch(b, oldrepos, new_mapping=oldrepos.get_mapping()))


class TestGenerateUpdateMapTests(TestCase):

    def test_nothing(self):
        upgrade = import_upgrade()
        self.assertEquals({}, upgrade.generate_upgrade_map(["bla", "bloe"], foreign_vcs_svn, BzrSvnMappingv3(TrunkBranchingScheme()).revision_id_foreign_to_bzr))

    def test_v2_to_v3(self):
        upgrade = import_upgrade()
        self.assertEquals({"svn-v2:12@65390229-12b7-0310-b90b-f21a5aa7ec8e-trunk": "svn-v3-trunk0:65390229-12b7-0310-b90b-f21a5aa7ec8e:trunk:12"}, upgrade.generate_upgrade_map(["svn-v2:12@65390229-12b7-0310-b90b-f21a5aa7ec8e-trunk", "bloe", "blaaa"], foreign_vcs_svn, BzrSvnMappingv3(TrunkBranchingScheme()).revision_id_foreign_to_bzr))
