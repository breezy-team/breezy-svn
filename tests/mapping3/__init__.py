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

import subvertpy
from subvertpy import ra

from bzrlib.branch import Branch
from bzrlib.bzrdir import BzrDir
from bzrlib.errors import (
    AppendRevisionsOnlyViolation,
    NoSuchRevision,
    )
from bzrlib.repository import Repository
from bzrlib.tests import TestCase
from bzrlib.workingtree import WorkingTree

from bzrlib.plugins.svn.layout.standard import (
    RootLayout,
    TrunkLayout,
    )
from bzrlib.plugins.svn.mapping import (
    SVN_PROP_BZR_REVISION_ID,
    mapping_registry,
    )
from bzrlib.plugins.svn.mapping3.base import (
    BzrSvnMappingv3,
    SVN_PROP_BZR_BRANCHING_SCHEME,
    set_property_scheme,
    )
from bzrlib.plugins.svn.mapping3.scheme import (
    InvalidSvnBranchPath,
    ListBranchingScheme,
    NoBranchingScheme,
    )
from bzrlib.plugins.svn.tests import (
    SubversionTestCase,
    )
from bzrlib.plugins.svn.tests.test_mapping import (
    sha1,
    )

class Mappingv3FilePropTests(TestCase):

    def setUp(self):
        TestCase.setUp(self)
        self.mapping = BzrSvnMappingv3(NoBranchingScheme())

    def test_generate_revid(self):
        self.assertEqual("svn-v3-undefined:myuuid:branch:5", 
                         BzrSvnMappingv3._generate_revision_id("myuuid", 5, "branch", "undefined"))

    def test_generate_revid_nested(self):
        self.assertEqual("svn-v3-undefined:myuuid:branch%2Fpath:5", 
                  BzrSvnMappingv3._generate_revision_id("myuuid", 5, "branch/path", "undefined"))

    def test_generate_revid_special_char(self):
        self.assertEqual("svn-v3-undefined:myuuid:branch%2C:5", 
             BzrSvnMappingv3._generate_revision_id("myuuid", 5, "branch\x2c", "undefined"))

    def test_generate_revid_nordic(self):
        self.assertEqual("svn-v3-undefined:myuuid:branch%C3%A6:5", 
             BzrSvnMappingv3._generate_revision_id("myuuid", 5, u"branch\xe6".encode("utf-8"), "undefined"))

    def test_parse_revid_simple(self):
        self.assertEqual(("uuid", "", 4, "undefined"),
                         BzrSvnMappingv3._parse_revision_id(
                             "svn-v3-undefined:uuid::4"))

    def test_parse_revid_nested(self):
        self.assertEqual(("uuid", "bp/data", 4, "undefined"),
                         BzrSvnMappingv3._parse_revision_id(
                     "svn-v3-undefined:uuid:bp%2Fdata:4"))

    def test_generate_file_id_root(self):
        self.assertEqual("2@uuid:bp:", self.mapping.generate_file_id(("uuid", "bp", 2), u""))

    def test_generate_file_id_path(self):
        self.assertEqual("2@uuid:bp:mypath", 
                self.mapping.generate_file_id(("uuid", "bp", 2), u"mypath"))

    def test_generate_file_id_long(self):
        dir = "this/is/a" + ("/very"*40) + "/long/path/"
        self.assertEqual("2@uuid:bp;" + sha1(dir+"filename"), 
                self.mapping.generate_file_id(("uuid", "bp", 2), dir+u"filename"))

    def test_generate_file_id_long_nordic(self):
        dir = "this/is/a" + ("/very"*40) + "/long/path/"
        self.assertEqual("2@uuid:bp;" + sha1((dir+u"filename\x2c\x8a").encode('utf-8')), 
                self.mapping.generate_file_id(("uuid", "bp", 2), dir+u"filename\x2c\x8a"))

    def test_generate_file_id_special_char(self):
        self.assertEqual("2@uuid:bp:mypath%2C%C2%8A",
                         self.mapping.generate_file_id(("uuid", "bp", 2), u"mypath\x2c\x8a"))

    def test_generate_file_id_spaces(self):
        self.assertFalse(" " in self.mapping.generate_file_id(("uuid", "b p", 1), u"my path"))

    def test_generate_svn_file_id(self):
        self.assertEqual("2@uuid:bp:path", 
                self.mapping.generate_file_id(("uuid", "bp", 2), u"path"))

    def test_generate_svn_file_id_nordic(self):
        self.assertEqual("2@uuid:bp:%C3%A6%C3%B8%C3%A5", 
                self.mapping.generate_file_id(("uuid", "bp", 2), u"\xe6\xf8\xe5"))

    def test_generate_svn_file_id_nordic_branch(self):
        self.assertEqual("2@uuid:%C3%A6:%C3%A6%C3%B8%C3%A5", 
                self.mapping.generate_file_id(("uuid", u"\xe6".encode('utf-8'), 2), u"\xe6\xf8\xe5"))


class RepositoryTests(SubversionTestCase):

    def setUp(self):
        super(RepositoryTests, self).setUp()
        self.repos_url = self.make_repository("d")
        self._old_mapping = mapping_registry._get_default_key()
        mapping_registry.set_default("v3")

    def tearDown(self):
        super(RepositoryTests, self).tearDown()
        mapping_registry.set_default("v3")

    def test_revision_id_to_revno_simple(self):
        repos_url = self.make_repository('a')

        dc = self.get_commit_editor(repos_url)
        dc.add_file("foo").modify()
        dc.change_prop("bzr:revision-id:v3-none", 
                            "2 myrevid\n")
        dc.close()

        branch = Branch.open(repos_url)
        self.assertEquals(2, branch.revision_id_to_revno("myrevid"))

    def test_revision_id_to_revno_older(self):
        repos_url = self.make_repository('a')

        dc = self.get_commit_editor(repos_url)
        dc.add_file("foo").modify()
        dc.change_prop("bzr:revision-id:v3-none", 
                            "2 myrevid\n")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.open_file("foo").modify()
        dc.change_prop("bzr:revision-id:v3-none", 
                            "2 myrevid\n3 mysecondrevid\n")
        dc.close()

        branch = Branch.open(repos_url)
        self.assertEquals(3, branch.revision_id_to_revno("mysecondrevid"))
        self.assertEquals(2, branch.revision_id_to_revno("myrevid"))


    def test_generate_revision_id_forced_revid(self):
        dc = self.get_commit_editor(self.repos_url)
        dc.change_prop(SVN_PROP_BZR_REVISION_ID+"v3-none", 
                             "2 someid\n")
        dc.close()

        repos = Repository.open(self.repos_url)
        mapping = repos.get_mapping()
        if not mapping.roundtripping:
            raise TestNotApplicable()
        revid = repos.generate_revision_id(1, "", mapping)
        self.assertEquals("someid", revid)

    def test_generate_revision_id_forced_revid_invalid(self):

        dc = self.get_commit_editor(self.repos_url)
        dc.change_prop(SVN_PROP_BZR_REVISION_ID+"v3-none", "corrupt-id\n")
        dc.close()

        repos = Repository.open(self.repos_url)
        mapping = repos.get_mapping()
        revid = repos.generate_revision_id(1, "", mapping)
        self.assertEquals(
                mapping.revision_id_foreign_to_bzr((repos.uuid, "", 1)),
                revid)

    def test_revision_ghost_parents(self):
        dc = self.get_commit_editor(self.repos_url)
        dc.add_file("foo").modify("data")
        dc.close()

        dc = self.get_commit_editor(self.repos_url)
        dc.open_file("foo").modify("data2")
        dc.change_prop("bzr:ancestry:v3-none", "ghostparent\n")
        dc.close()

        repository = Repository.open(self.repos_url)
        mapping = repository.get_mapping()
        self.assertEqual((),
                repository.get_revision(
                    repository.generate_revision_id(0, "", mapping)).parent_ids)
        self.assertEqual((repository.generate_revision_id(0, "", mapping),),
                repository.get_revision(
                    repository.generate_revision_id(1, "", mapping)).parent_ids)
        self.assertEqual((repository.generate_revision_id(1, "", mapping),
            "ghostparent"), 
                repository.get_revision(
                    repository.generate_revision_id(2, "", mapping)).parent_ids)
 
    def test_get_revision_id_overriden(self):
        self.make_checkout(self.repos_url, 'dc')
        repository = Repository.open(self.repos_url)
        self.assertRaises(NoSuchRevision, repository.get_revision, "nonexisting")
        self.build_tree({'dc/foo': "data"})
        self.client_add("dc/foo")
        self.client_commit("dc", "My Message")
        self.build_tree({'dc/foo': "data2"})
        self.client_set_prop("dc", "bzr:revision-id:v3-none", 
                            "3 myrevid\n")
        self.client_update("dc")
        (num, date, author) = self.client_commit("dc", "Second Message")
        repository = Repository.open(self.repos_url)
        mapping = repository.get_mapping()
        if not mapping.roundtripping:
            raise TestNotApplicable
        revid = mapping.revision_id_foreign_to_bzr((repository.uuid, "", 2))
        rev = repository.get_revision("myrevid")
        self.assertEqual((repository.generate_revision_id(1, "", mapping),),
                rev.parent_ids)
        self.assertEqual(rev.revision_id, 
                         repository.generate_revision_id(2, "", mapping))
        self.assertEqual(author, rev.committer)
        self.assertIsInstance(rev.properties, dict)

    def test_get_ancestry_merged(self):
        self.make_checkout(self.repos_url, 'dc')
        self.build_tree({'dc/foo': "data"})
        self.client_add("dc/foo")
        self.client_commit("dc", "My Message")
        self.client_update("dc")
        self.client_set_prop("dc", "bzr:ancestry:v3-none", "a-parent\n")
        self.build_tree({'dc/foo': "data2"})
        self.client_commit("dc", "Second Message")
        repository = Repository.open(self.repos_url)
        mapping = repository.get_mapping()
        self.assertEqual([None, repository.generate_revision_id(0, "", mapping)],
                repository.get_ancestry(
                    repository.generate_revision_id(0, "", mapping)))
        self.assertEqual([None, repository.generate_revision_id(0, "", mapping),
            repository.generate_revision_id(1, "", mapping)],
                repository.get_ancestry(
                    repository.generate_revision_id(1, "", mapping)))
        self.assertEqual([None, 
            repository.generate_revision_id(0, "", mapping), "a-parent", 
            repository.generate_revision_id(1, "", mapping), 
                  repository.generate_revision_id(2, "", mapping)], 
                repository.get_ancestry(
                    repository.generate_revision_id(2, "", mapping)))

    def test_lookup_revision_id_overridden(self):
        dc = self.get_commit_editor(self.repos_url)
        dc.add_dir("bloe")
        dc.change_prop(SVN_PROP_BZR_REVISION_ID+"v3-none", "2 myid\n")
        dc.close()
        repository = Repository.open(self.repos_url)
        mapping = repository.get_mapping()
        self.assertEqual(((repository.uuid, "", 1), mapping), repository.lookup_bzr_revision_id( 
            mapping.revision_id_foreign_to_bzr((repository.uuid, "", 1)))[:2])
        self.assertEqual(((repository.uuid, "", 1), mapping),
                repository.lookup_bzr_revision_id("myid")[:2])

    def test_lookup_revision_id_overridden_invalid(self):
        dc = self.get_commit_editor(self.repos_url)
        dc.add_dir("bloe")
        dc.change_prop(SVN_PROP_BZR_REVISION_ID+"v3-none", "corrupt-entry\n")
        dc.close()

        repository = Repository.open(self.repos_url)
        mapping = repository.get_mapping()
        self.assertEqual(((repository.uuid, "", 1), mapping), repository.lookup_bzr_revision_id( 
            mapping.revision_id_foreign_to_bzr((repository.uuid, "", 1)))[:2])
        self.assertRaises(NoSuchRevision, repository.lookup_bzr_revision_id, 
            "corrupt-entry")

    def test_lookup_revision_id_overridden_invalid_dup(self):
        self.make_checkout(self.repos_url, 'dc')
        self.build_tree({'dc/bloe': None})
        self.client_add("dc/bloe")
        self.client_set_prop("dc", SVN_PROP_BZR_REVISION_ID+"v3-none", 
                             "corrupt-entry\n")
        self.client_commit("dc", "foobar")
        self.build_tree({'dc/bla': None})
        self.client_add("dc/bla")
        self.client_set_prop("dc", SVN_PROP_BZR_REVISION_ID+"v3-none", 
                "corrupt-entry\n2 corrupt-entry\n")
        self.client_commit("dc", "foobar")
        repository = Repository.open(self.repos_url)
        mapping = repository.get_mapping()
        self.assertEqual(((repository.uuid, "", 2), mapping), repository.lookup_bzr_revision_id( 
            mapping.revision_id_foreign_to_bzr((repository.uuid, "", 2)))[:2])
        self.assertEqual(((repository.uuid, "", 1), mapping), repository.lookup_bzr_revision_id( 
            mapping.revision_id_foreign_to_bzr((repository.uuid, "", 1)))[:2])
        self.assertEqual(((repository.uuid, "", 2), mapping), repository.lookup_bzr_revision_id( 
            "corrupt-entry")[:2])

    def test_lookup_revision_id_overridden_not_found(self):
        """Make sure a revision id that is looked up but doesn't exist 
        doesn't accidently end up in the revid cache."""
        self.make_checkout(self.repos_url, 'dc')
        self.build_tree({'dc/bloe': None})
        self.client_add("dc/bloe")
        self.client_set_prop("dc", SVN_PROP_BZR_REVISION_ID+"v3-none", "2 myid\n")
        self.client_commit("dc", "foobar")
        repository = Repository.open(self.repos_url)
        self.assertRaises(NoSuchRevision, 
                repository.lookup_bzr_revision_id, "foobar")

    def test_set_branching_scheme_property(self):
        self.make_checkout(self.repos_url, 'dc')
        self.client_set_prop("dc", SVN_PROP_BZR_BRANCHING_SCHEME, 
            "trunk\nbranches/*\nbranches/tmp/*")
        self.client_commit("dc", "set scheme")
        repository = Repository.open(self.repos_url)
        self.assertEquals(ListBranchingScheme(["trunk", "branches/*", "branches/tmp/*"]).branch_list,
                          repository.get_mapping().scheme.branch_list)

    def test_set_property_scheme(self):
        self.make_checkout(self.repos_url, 'dc')
        repos = Repository.open(self.repos_url)
        set_property_scheme(repos, ListBranchingScheme(["bla/*"]))
        self.client_update("dc")
        self.assertEquals("bla/*\n", 
                   self.client_get_prop("dc", SVN_PROP_BZR_BRANCHING_SCHEME))
        self.assertEquals("Updating branching scheme for Bazaar.", 
                self.client_log(self.repos_url, 1, 1)[1][3])

    def test_fetch_fileid_renames(self):
        dc = self.get_commit_editor(self.repos_url)
        dc.add_file("test").modify("data")
        dc.change_prop("bzr:file-ids", "test\tbla\n")
        dc.change_prop("bzr:revision-info", "")
        dc.close()

        oldrepos = Repository.open(self.repos_url)
        dir = BzrDir.create("f")
        newrepos = dir.create_repository()
        oldrepos.copy_content_into(newrepos)
        mapping = oldrepos.get_mapping()
        self.assertEqual("bla", newrepos.get_inventory(
            oldrepos.generate_revision_id(1, "", mapping)).path2id("test"))

    def test_fetch_ghosts(self):
        dc = self.get_commit_editor(self.repos_url)
        dc.add_file("bla").modify("data")
        dc.change_prop("bzr:ancestry:v3-none", "aghost\n")
        dc.close()

        oldrepos = Repository.open(self.repos_url)
        oldrepos.set_layout(RootLayout())
        dir = BzrDir.create("f")
        newrepos = dir.create_repository()
        oldrepos.copy_content_into(newrepos)
        mapping = oldrepos.get_mapping()

        rev = newrepos.get_revision(oldrepos.generate_revision_id(1, "", mapping))
        self.assertTrue("aghost" in rev.parent_ids)

    def test_fetch_invalid_ghosts(self):
        dc = self.get_commit_editor(self.repos_url)
        dc.add_file("bla").modify("data")
        dc.change_prop("bzr:ancestry:v3-none", "a ghost\n")
        dc.close()

        oldrepos = Repository.open(self.repos_url)
        dir = BzrDir.create("f")
        newrepos = dir.create_repository()
        oldrepos.copy_content_into(newrepos)
        
        mapping = oldrepos.get_mapping()

        rev = newrepos.get_revision(oldrepos.generate_revision_id(1, "", mapping))
        self.assertEqual([oldrepos.generate_revision_id(0, "", mapping)], rev.parent_ids)

    def test_fetch_complex_ids_dirs(self):
        dc = self.get_commit_editor(self.repos_url)
        dir = dc.add_dir("dir")
        dir.add_dir("dir/adir")
        dc.change_prop("bzr:revision-info", "")
        dc.change_prop("bzr:file-ids", "dir\tbloe\ndir/adir\tbla\n")
        dc.close()

        dc = self.get_commit_editor(self.repos_url)
        dc.add_dir("bdir", "dir/adir")
        dir = dc.open_dir("dir")
        dir.delete("dir/adir")
        dc.change_prop("bzr:revision-info", "properties: \n")
        dc.change_prop("bzr:file-ids", "bdir\tbla\n")
        dc.close()

        oldrepos = Repository.open(self.repos_url)
        dir = BzrDir.create("f")
        newrepos = dir.create_repository()
        oldrepos.copy_content_into(newrepos)
        mapping = oldrepos.get_mapping()
        tree = newrepos.revision_tree(oldrepos.generate_revision_id(2, "", mapping))
        self.assertEquals("bloe", tree.path2id("dir"))
        self.assertIs(None, tree.path2id("dir/adir"))
        self.assertEquals("bla", tree.path2id("bdir"))

    def test_fetch_complex_ids_files(self):
        dc = self.get_commit_editor(self.repos_url)
        dir = dc.add_dir("dir")
        dir.add_file("dir/adir").modify("contents")
        dc.change_prop("bzr:revision-info", "")
        dc.change_prop("bzr:file-ids", "dir\tbloe\ndir/adir\tbla\n")
        dc.close()

        dc = self.get_commit_editor(self.repos_url)
        dc.add_file("bdir", "dir/adir")
        dir = dc.open_dir("dir")
        dir.delete("dir/adir")
        dc.change_prop("bzr:revision-info", "properties: \n")
        dc.change_prop("bzr:file-ids", "bdir\tbla\n")
        dc.close()

        oldrepos = Repository.open(self.repos_url)
        dir = BzrDir.create("f")
        newrepos = dir.create_repository()
        oldrepos.copy_content_into(newrepos)
        mapping = oldrepos.get_mapping()
        tree = newrepos.revision_tree(oldrepos.generate_revision_id(2, "", mapping))
        self.assertEquals("bloe", tree.path2id("dir"))
        self.assertIs(None, tree.path2id("dir/adir"))
        self.assertEquals("bla", tree.path2id("bdir"))

    def test_store_branching_scheme(self):
        self.make_checkout(self.repos_url, 'dc')
        repository = Repository.open(self.repos_url)
        repository.set_layout(TrunkLayout(42))
        repository = Repository.open(self.repos_url)
        self.assertEquals("trunk42", str(repository.get_mapping().scheme))

    def test_revision_fileidmap(self):
        dc = self.get_commit_editor(self.repos_url)
        dc.add_file("foo").modify("data")
        dc.change_prop("bzr:revision-info", "")
        dc.change_prop("bzr:file-ids", "foo\tsomeid\n")
        dc.close()

        repository = Repository.open(self.repos_url)
        repository.set_layout(RootLayout())
        tree = repository.revision_tree(Branch.open(self.repos_url).last_revision())
        self.assertEqual("someid", tree.inventory.path2id("foo"))
        self.assertFalse("1@%s::foo" % repository.uuid in tree.inventory)

    def test_commit_revision_id(self):
        self.make_checkout(self.repos_url, "dc")
        wt = WorkingTree.open("dc")
        self.build_tree({'dc/foo/bla': "data", 'dc/bla': "otherdata"})
        wt.add('bla')
        wt.commit(message="data")
        branch = Branch.open(self.repos_url)
        builder = branch.get_commit_builder([branch.last_revision()], 
                revision_id="my-revision-id")
        tree = branch.repository.revision_tree(branch.last_revision())
        list(builder.record_iter_changes(tree, branch.last_revision(), []))
        builder.finish_inventory()
        builder.commit("foo")

        self.assertEqual("3 my-revision-id\n", 
            self.client_get_prop("dc", 
                "bzr:revision-id:v3-none", 2))

    def test_commit_metadata(self):
        self.make_checkout(self.repos_url, "dc")

        wt = WorkingTree.open("dc")
        self.build_tree({'dc/foo/bla': "data", 'dc/bla': "otherdata"})
        wt.add('bla')
        wt.commit(message="data")
        branch = Branch.open(self.repos_url)
        builder = branch.get_commit_builder([branch.last_revision()], 
                timestamp=4534.0, timezone=2, committer="fry",
                revision_id="my-revision-id")
        tree = branch.repository.revision_tree(branch.last_revision())
        list(builder.record_iter_changes(tree, branch.last_revision(), []))
        builder.finish_inventory()
        builder.commit("foo")

        self.assertEqual("3 my-revision-id\n", 
                self.client_get_prop("dc", "bzr:revision-id:v3-none", 2))

        self.assertEqual(
                "timestamp: 1970-01-01 01:15:36.000000000 +0000\ncommitter: fry\n",
                self.client_get_prop("dc", "bzr:revision-info", 2))

    def test_commit_parents(self):
        self.make_checkout(self.repos_url, "dc")
        self.build_tree({'dc/foo/bla': "data"})
        self.client_add("dc/foo")
        wt = WorkingTree.open("dc")
        wt.set_pending_merges(["some-ghost-revision"])
        self.assertEqual(["some-ghost-revision"], wt.get_parent_ids()[1:])
        wt.commit(message="data")
        self.assertEqual("some-ghost-revision\n", 
                self.client_get_prop(self.repos_url, "bzr:ancestry:v3-none", 1))
        self.assertEqual((wt.branch.generate_revision_id(0), "some-ghost-revision"),
                         wt.branch.repository.get_revision(
                             wt.branch.last_revision()).parent_ids)

    def test_push_unnecessary_merge(self):
        from bzrlib.debug import debug_flags
        debug_flags.add("commit")
        debug_flags.add("fetch")
        repos_url = self.make_repository("a")
        bzrwt = BzrDir.create_standalone_workingtree("c")
        self.build_tree({'c/registry/generic.c': "Tour"})
        bzrwt.add("registry")
        bzrwt.add("registry/generic.c")
        revid1 = bzrwt.commit("Add initial directory + file", 
                              rev_id="initialrevid")

        # Push first branch into Subversion
        newdir = BzrDir.open(repos_url+"/trunk")
        newbranch = newdir.import_branch(bzrwt.branch)

        c = ra.RemoteAccess(repos_url)
        self.assertTrue(c.check_path("trunk/registry/generic.c", c.get_latest_revnum()) == subvertpy.NODE_FILE)

        dc = self.get_commit_editor(repos_url)
        trunk = dc.open_dir("trunk")
        registry = trunk.open_dir("trunk/registry")
        registry.open_file("trunk/registry/generic.c").modify("BLA")
        dc.close()
        mapping = newdir.find_repository().get_mapping()
        merge_revid = newdir.find_repository().generate_revision_id(2, "trunk", mapping)

        # Merge 
        self.build_tree({'c/registry/generic.c': "DE"})
        bzrwt.add_pending_merge(merge_revid)
        self.assertEquals(bzrwt.get_parent_ids()[1], merge_revid)
        revid2 = bzrwt.commit("Merge something", rev_id="mergerevid")
        bzr_parents = bzrwt.branch.repository.get_revision(revid2).parent_ids
        trunk = Branch.open(repos_url + "/trunk")
        self.assertRaises(AppendRevisionsOnlyViolation,
            trunk.pull, bzrwt.branch)
        trunk.get_config().set_user_option('append_revisions_only', 'False')
        trunk.pull(bzrwt.branch)

        self.assertEquals(tuple(bzr_parents),
                trunk.repository.get_revision(revid2).parent_ids)

        self.assertEquals([revid1, revid2], trunk.revision_history())
        self.assertEquals(
                '1 initialrevid\n2 mergerevid\n',
                self.client_get_prop(repos_url+"/trunk", SVN_PROP_BZR_REVISION_ID+"v3-trunk0",
                                     c.get_latest_revnum()))

    def test_revision_history(self):
        repos_url = self.make_repository('a')

        branch = Branch.open(repos_url)
        self.assertEqual([branch.generate_revision_id(0)], 
                branch.revision_history())

        dc = self.get_commit_editor(repos_url)
        dc.add_file("foo").modify()
        dc.change_prop(SVN_PROP_BZR_REVISION_ID+"v3-none", 
                "42 mycommit\n")
        dc.close()

        branch = Branch.open(repos_url)
        repos = Repository.open(repos_url)

        mapping = repos.get_mapping()

        self.assertEqual([repos.generate_revision_id(0, "", mapping), 
                    repos.generate_revision_id(1, "", mapping)], 
                branch.revision_history())

        dc = self.get_commit_editor(repos_url)
        dc.open_file("foo").modify()
        dc.close()

        branch = Branch.open(repos_url)
        repos = Repository.open(repos_url)

        mapping = repos.get_mapping()

        self.assertEqual([
            repos.generate_revision_id(0, "", mapping),
            "mycommit",
            repos.generate_revision_id(2, "", mapping)],
            branch.revision_history())


class ErrorTests(TestCase):

    def test_invalidsvnbranchpath_nonascii(self):
        InvalidSvnBranchPath('\xc3\xb6', None)

