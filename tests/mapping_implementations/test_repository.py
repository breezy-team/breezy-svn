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

from breezy import urlutils
from breezy.branch import Branch
from breezy.controldir import ControlDir
from breezy.errors import (
    NoSuchRevision,
    UnsupportedOperation,
    )
from breezy.repository import Repository
from breezy.revision import NULL_REVISION
from breezy.tests import TestSkipped, TestNotApplicable

from breezy.plugins.svn import errors as svn_errors
from subvertpy import ra
from breezy.plugins.svn.repository import find_branches_between
from breezy.plugins.svn.layout.standard import (
    TrunkLayout, RootLayout, CustomLayout,)
from breezy.plugins.svn.mapping import mapping_registry
from breezy.plugins.svn.tests import SubversionTestCase

from subvertpy import NODE_DIR, NODE_FILE


class TestSubversionMappingRepositoryWorks(SubversionTestCase):
    """Mapping-dependent tests for Subversion repositories."""

    def setUp(self):
        super(TestSubversionMappingRepositoryWorks, self).setUp()
        self._old_mapping = mapping_registry._get_default_key()
        self.addCleanup(mapping_registry.set_default, self._old_mapping)
        mapping_registry.set_default(self.mapping_name)

    def test_get_branch_log(self):
        repos_url = self.make_svn_repository("a")
        cb = self.get_commit_editor(repos_url)
        cb.add_file("foo").modify()
        cb.close()

        repos = Repository.open(repos_url)
        repos.set_layout(RootLayout())

        self.assertRevmetaLogEquals([
            ('', {'foo': ('A', None, -1, NODE_FILE)}, 1),
            ('', {'': ('A', None, -1, NODE_DIR)}, 0)],
            repos._revmeta_provider.iter_reverse_branch_changes("", 1, 0))

    def test_set_make_working_trees(self):
        repos_url = self.make_svn_repository("a")
        repos = Repository.open(repos_url)
        self.assertFalse(repos.make_working_trees())
        self.assertRaises(UnsupportedOperation, repos.set_make_working_trees, True)

    def test_get_fileid_map(self):
        repos_url = self.make_svn_repository("a")
        repos = Repository.open(repos_url)
        mapping = repos.get_mapping()
        rm_provider = repos._revmeta_provider
        ret = repos.get_fileid_map(rm_provider.get_revision("", 0), mapping).as_dict()
        if mapping.is_branch(""):
            self.assertEqual(
                {u"": (mapping.generate_file_id((repos.uuid, "", 0), u""),
                       mapping.revision_id_foreign_to_bzr((repos.uuid, "", 0)),
                       None)},
                ret)
        else:
            self.assertEquals({}, ret)

    def test_add_revision(self):
        repos_url = self.make_svn_repository("a")
        repos = Repository.open(repos_url)
        self.assertRaises(NotImplementedError, repos.add_revision, "revid",
                None)

    def test_has_signature_for_revision_id_no(self):
        repos_url = self.make_svn_repository("a")
        repos = Repository.open(repos_url)
        self.assertFalse(repos.has_signature_for_revision_id("foo"))

    def test_set_signature(self):
        repos_url = self.make_svn_repository("a")
        repos = Repository.open(repos_url)
        cb = self.get_commit_editor(repos_url)
        cb.add_file("foo").modify("bar")
        cb.close()
        revid = repos.get_mapping().revision_id_foreign_to_bzr((repos.uuid, "", 1))
        repos.add_signature_text(revid, "TEXT")
        self.assertTrue(repos.has_signature_for_revision_id(revid))
        self.assertEquals(repos.get_signature_text(revid), "TEXT")

    def test_get_branch_invalid_revision(self):
        repos_url = self.make_svn_repository("a")
        repos = Repository.open(repos_url)
        repos.set_layout(RootLayout())
        self.assertRaises(NoSuchRevision, list,
               repos._revmeta_provider.iter_reverse_branch_changes("/", 20, 0))

    def test_follow_branch_switched_parents(self):
        repos_url = self.make_svn_repository('a')

        cb = self.get_commit_editor(repos_url)
        pykleur = cb.add_dir("pykleur")
        trunk = pykleur.add_dir("pykleur/trunk")
        nested = trunk.add_dir("pykleur/trunk/pykleur")
        cb.close() #1

        cb = self.get_commit_editor(repos_url)
        pykleur = cb.open_dir("pykleur")
        trunk = pykleur.open_dir("pykleur/trunk")
        nested = trunk.open_dir("pykleur/trunk/pykleur")
        afile = nested.add_file("pykleur/trunk/pykleur/afile")
        afile.modify(b"contents")
        afile.close()
        cb.close() #2

        cb = self.get_commit_editor(repos_url)
        cb.add_dir("pygments", "pykleur", 1)
        cb.delete("pykleur")
        cb.close() #3

        repos = Repository.open(repos_url)
        repos.set_layout(TrunkLayout(1))
        results = repos._revmeta_provider.iter_reverse_branch_changes(
                "pygments/trunk", 3, 0)

        # Results differ per Subversion version, yay
        # For <= 1.4:
        if ra.version()[1] <= 4:
            self.assertRevmetaLogEquals([
            ('pygments/trunk', {
                'pygments': (u'A', 'pykleur', 1, NODE_DIR),
                'pygments/trunk': (u'R', 'pykleur/trunk', 2, NODE_DIR),
                'pykleur': (u'D', None, -1, NODE_DIR)}, 3),
            ('pykleur/trunk', {'pykleur/trunk/pykleur/afile': (u'A', None, -1, NODE_DIR)}, 2),
            ('pykleur/trunk',
                    {'pykleur': (u'A', None, -1, NODE_DIR),
                     'pykleur/trunk': (u'A', None, -1, NODE_DIR),
                     'pykleur/trunk/pykleur': (u'A', None, -1, NODE_DIR)},
             1)], results
            )
        else:
            self.assertRevmetaLogEquals(
               [ ('pykleur/trunk', {
                   'pykleur': (u'A', None, -1, NODE_DIR),
                   'pykleur/trunk': (u'A', None, -1, NODE_DIR),
                   'pykleur/trunk/pykleur': (u'A', None, -1, NODE_DIR)},
                1)], results
            )

    def test_follow_branch_move_single(self):
        repos_url = self.make_svn_repository('a')

        dc = self.get_commit_editor(repos_url)
        pykleur = dc.add_dir("pykleur")
        pykleur.add_dir("pykleur/bla")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("pygments", "pykleur", 1)
        dc.close()

        repos = Repository.open(repos_url)
        try:
            repos.set_layout(CustomLayout(["pygments"]))
        except svn_errors.LayoutUnusable:
            raise TestNotApplicable
        changes = [c for c,h,m in repos._revmeta_provider._iter_reverse_revmeta_mapping_history("pygments", 2, 0, repos.get_mapping())]
        if repos.get_mapping().is_branch("pykleur"):
            self.assertRevmetaLogEquals([('pygments',
                  {'pygments': ('A', 'pykleur', 1, NODE_DIR)},
                    2),
                    ('pykleur', {'pykleur': ('A', None, -1, NODE_DIR), 'pykleur/bla': ('A', None, -1, NODE_DIR)}, 1)], changes)
        else:
            self.assertRevmetaLogEquals([('pygments', {'pygments': ('A', 'pykleur', 1, NODE_DIR)}, 2), ], changes)


    def test_history_all(self):
        repos_url = self.make_svn_repository("a")

        dc = self.get_commit_editor(repos_url)
        trunk = dc.add_dir("trunk")
        trunk.add_file("trunk/file").modify(b"data")
        foo = dc.add_dir("foo")
        foo.add_file("foo/file").modify(b"data")
        dc.close()

        repos = Repository.open(repos_url)
        repos.set_layout(TrunkLayout())

        self.assertEqual(1,
               len(set(repos.all_revision_ids(TrunkLayout()))))

    def test_all_revs_empty(self):
        repos_url = self.make_svn_repository("a")
        repos = Repository.open(repos_url)
        repos.set_layout(TrunkLayout(0))
        self.assertEqual(set([]), set(repos.all_revision_ids()))

    def test_all_revs(self):
        repos_url = self.make_svn_repository("a")

        dc = self.get_commit_editor(repos_url)
        trunk = dc.add_dir("trunk")
        trunk.add_file("trunk/file").modify(b"data")
        foo = dc.add_dir("foo")
        foo.add_file("foo/file").modify(b"data")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        branches = dc.add_dir("branches")
        somebranch = branches.add_dir("branches/somebranch")
        somebranch.add_file("branches/somebranch/somefile").modify(b"data")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        branches = dc.open_dir("branches")
        branches.delete("branches/somebranch")
        dc.close()

        repos = Repository.open(repos_url)
        repos.set_layout(TrunkLayout(0))
        mapping = repos.get_mapping()
        self.assertEqual(set([
            repos.generate_revision_id(1, "trunk", mapping),
            repos.generate_revision_id(2, "branches/somebranch", mapping)]),
            set(repos.all_revision_ids()))

    def test_follow_history_empty(self):
        repos_url = self.make_svn_repository("a")
        repos = Repository.open(repos_url)
        repos.set_layout(RootLayout())
        self.assertEqual(set([repos.generate_revision_id(0, '', repos.get_mapping())]),
              set(repos.all_revision_ids(repos.get_layout())))

    def test_follow_history_empty_branch(self):
        repos_url = self.make_svn_repository("a")

        dc = self.get_commit_editor(repos_url)
        trunk = dc.add_dir("trunk")
        trunk.add_file("trunk/afile").modify(b"data")
        dc.add_dir("branches")
        dc.close()

        repos = Repository.open(repos_url)
        repos.set_layout(TrunkLayout(0))
        self.assertEqual(set([repos.generate_revision_id(1, 'trunk', repos.get_mapping())]),
                set(repos.all_revision_ids(repos.get_layout())))

    def test_follow_history_follow(self):
        repos_url = self.make_svn_repository("a")

        dc = self.get_commit_editor(repos_url)
        trunk = dc.add_dir("trunk")
        trunk.add_file("trunk/afile").modify(b"data")
        dc.add_dir("branches")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        branches = dc.open_dir("branches")
        branches.add_dir("branches/abranch", "trunk", 1)
        dc.close()

        repos = Repository.open(repos_url)
        repos.set_layout(TrunkLayout(0))

        items = set(repos.all_revision_ids(repos.get_layout()))
        self.assertEqual(set([repos.generate_revision_id(1, 'trunk', repos.get_mapping()),
                          repos.generate_revision_id(2, 'branches/abranch', repos.get_mapping())
                          ]), items)

    def test_branch_log_specific(self):
        repos_url = self.make_client("a", "dc")
        self.build_tree({
            'dc/branches': None,
            'dc/branches/brancha': None,
            'dc/branches/branchab': None,
            'dc/branches/brancha/data': "data",
            "dc/branches/branchab/data":"data"})
        self.client_add("dc/branches")
        self.client_commit("dc", "My Message")

        repos = Repository.open(repos_url)
        repos.set_layout(TrunkLayout(0))

        self.assertEqual(1, len(list(repos._revmeta_provider.iter_reverse_branch_changes("branches/brancha",
            1, 0))))

    def test_branch_log_specific_ignore(self):
        repos_url = self.make_client("a", "dc")
        self.build_tree({'dc/branches': None})
        self.client_add("dc/branches")
        self.build_tree({
            'dc/branches/brancha': None,
            'dc/branches/branchab': None,
            'dc/branches/brancha/data': "data",
            "dc/branches/branchab/data":"data"})
        self.client_add("dc/branches/brancha")
        self.client_commit("dc", "My Message")

        self.client_add("dc/branches/branchab")
        self.client_commit("dc", "My Message2")

        repos = Repository.open(repos_url)
        repos.set_layout(TrunkLayout(0))

        self.assertEqual(1, len(list(repos._revmeta_provider.iter_reverse_branch_changes("branches/brancha",
            2, 0))))

    def test_find_branches(self):
        repos_url = self.make_client("a", "dc")
        self.build_tree({
            'dc/branches/brancha': None,
            'dc/branches/branchab': None,
            'dc/branches/brancha/data': "data",
            "dc/branches/branchab/data":"data"})
        self.client_add("dc/branches")
        self.client_commit("dc", "My Message")
        repos = Repository.open(repos_url)
        repos.set_layout(TrunkLayout(0))
        branches = repos.find_branches()
        self.assertEquals(2, len(branches))
        self.assertEquals(urlutils.join(repos.base, "branches/brancha"),
                          branches[1].base)
        self.assertEquals(urlutils.join(repos.base, "branches/branchab"),
                          branches[0].base)

    def test_find_tags(self):
        repos_url = self.make_svn_repository('a')

        dc = self.get_commit_editor(repos_url)
        tags = dc.add_dir("tags")
        tags.add_dir("tags/brancha").add_file("tags/brancha/data").modify()
        tags.add_dir("tags/branchab").add_file("tags/branchab/data").modify()
        dc.close()

        repos = Repository.open(repos_url)
        repos.set_layout(TrunkLayout(0))
        tags = repos.find_tags("")
        rm_provider = repos._revmeta_provider
        self.assertEquals({"brancha": rm_provider.get_revision("tags/brancha", 1),
                           "branchab": rm_provider.get_revision("tags/branchab", 1)}, tags)

    def test_find_tags_unmodified(self):
        repos_url = self.make_svn_repository('a')

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("trunk").add_file("trunk/data").modify()
        dc.close()

        dc = self.get_commit_editor(repos_url)
        tags = dc.add_dir("tags")
        tags.add_dir("tags/brancha", "trunk")
        dc.close()

        repos = Repository.open(repos_url)
        repos.set_layout(TrunkLayout(0))
        tags = repos.find_tags("")
        rm_provider = repos._revmeta_provider
        self.assertEquals({"brancha": rm_provider.get_revision("trunk", 1)}, tags)

    def test_find_tags_modified(self):
        repos_url = self.make_svn_repository('a')

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("trunk").add_file("trunk/data").modify()
        dc.close()

        dc = self.get_commit_editor(repos_url)
        tags = dc.add_dir("tags")
        tags.add_dir("tags/brancha", "trunk")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        tags = dc.open_dir("tags")
        brancha = tags.open_dir("tags/brancha")
        brancha.add_file("tags/brancha/release-notes").modify()
        dc.close()

        repos = Repository.open(repos_url)
        repos.set_layout(TrunkLayout(0))
        tags = repos.find_tags("")
        rm_provider = repos._revmeta_provider
        self.assertEquals({"brancha": rm_provider.get_revision("tags/brancha", 3)}, tags)

    def test_find_branches_between_moved(self):
        repos_url = self.make_client("a", "dc")
        self.build_tree({
            'dc/tmp/branches/brancha': None,
            'dc/tmp/branches/branchab': None,
            'dc/tmp/branches/brancha/data': "data",
            "dc/tmp/branches/branchab/data":"data"})
        self.client_add("dc/tmp")
        self.client_commit("dc", "My Message")
        self.client_copy("dc/tmp/branches", "dc/tags")
        self.client_commit("dc", "My Message 2")

        repos = Repository.open(repos_url)
        repos.set_layout(TrunkLayout(0))

        self.assertEqual([("tags/branchab", 2, True),
                          ("tags/brancha", 2, True)],
                list(find_branches_between(repos._log, repos.transport, TrunkLayout(0), from_revnum=2, to_revnum=0)))

    def test_find_branches_between_start_revno(self):
        repos_url = self.make_svn_repository("a")

        dc = self.get_commit_editor(repos_url)
        branches = dc.add_dir("branches")
        branches.add_dir("branches/brancha")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        branches = dc.open_dir("branches")
        branches.add_dir("branches/branchb")
        dc.close()

        repos = Repository.open(repos_url)
        repos.set_layout(TrunkLayout(0))

        self.assertEqual([("branches/branchb", 2, True)],
                list(find_branches_between(repos._log, repos.transport, TrunkLayout(0), from_revnum=2, to_revnum=2)))

    def test_find_branches_between_file_moved_from_nobranch(self):
        repos_url = self.make_client("a", "dc")
        self.build_tree({
            'dc/tmp/trunk': None,
            'dc/bla/somefile': "contents"})
        self.client_add("dc/tmp")
        self.client_add("dc/bla")
        self.client_commit("dc", "My Message")
        self.client_copy("dc/bla", "dc/tmp/branches")
        self.client_delete("dc/tmp/branches/somefile")
        self.client_commit("dc", "My Message 2")

        repos = Repository.open(repos_url)
        find_branches_between(repos._log, repos.transport, TrunkLayout(2), from_revnum=2, to_revnum=0)

    def test_find_branches_between_deleted_from_nobranch(self):
        repos_url = self.make_client("a", "dc")
        self.build_tree({
            'dc/tmp/trunk': None,
            'dc/bla/somefile': "contents"})
        self.client_add("dc/tmp")
        self.client_add("dc/bla")
        self.client_commit("dc", "My Message")
        self.client_copy("dc/bla", "dc/tmp/branches")
        self.client_delete("dc/tmp/branches/somefile")
        self.client_commit("dc", "My Message 2")

        repos = Repository.open(repos_url)
        find_branches_between(repos._log, repos.transport, TrunkLayout(1), from_revnum=2, to_revnum=0)

    def test_find_branches_between_moved_nobranch(self):
        repos_url = self.make_client("a", "dc")
        self.build_tree({
            'dc/tmp/nested/foobar': None,
            'dc/tmp/nested/branches/brancha': None,
            'dc/tmp/nested/branches/branchab': None,
            'dc/tmp/nested/branches/brancha/data': "data",
            "dc/tmp/nested/branches/branchab/data":"data"})
        self.client_add("dc/tmp")
        self.client_commit("dc", "My Message")
        self.client_copy("dc/tmp/nested", "dc/t2")
        self.client_commit("dc", "My Message 2")

        repos = Repository.open(repos_url)
        repos.set_layout(TrunkLayout(1))

        self.assertEqual([("t2/branches/brancha", 2, True),
                          ("t2/branches/branchab", 2, True)],
                list(find_branches_between(repos._log, repos.transport, TrunkLayout(1), to_revnum=2, from_revnum=2)))

    def test_find_branches_between_root(self):
        repos_url = self.make_svn_repository("a")

        repos = Repository.open(repos_url)
        repos.set_layout(RootLayout())

        self.assertEqual([("", 0, True)],
                list(find_branches_between(repos._log, repos.transport, RootLayout(), to_revnum=0, from_revnum=0)))

    def test_find_branches_between_no_later(self):
        repos_url = self.make_svn_repository("a")

        repos = Repository.open(repos_url)
        repos.set_layout(RootLayout())

        self.assertEqual([("", 0, True)],
                list(find_branches_between(repos._log, repos.transport, RootLayout(), to_revnum=0, from_revnum=0)))

    def test_find_branches_between_trunk_empty(self):
        repos_url = self.make_svn_repository("a")

        repos = Repository.open(repos_url)
        repos.set_layout(TrunkLayout(0))

        self.assertEqual([],
                list(find_branches_between(repos._log, repos.transport, TrunkLayout(0), to_revnum=0, from_revnum=0)))

    def test_find_branches_between_trunk_one(self):
        repos_url = self.make_svn_repository("a")

        repos = Repository.open(repos_url)
        repos.set_layout(TrunkLayout(0))

        dc = self.get_commit_editor(repos_url)
        trunk = dc.add_dir("trunk")
        trunk.add_file("trunk/foo").modify(b"data")
        dc.close()

        self.assertEqual([("trunk", 1, True)],
                list(find_branches_between(repos._log, repos.transport, TrunkLayout(0), from_revnum=1, to_revnum=0)))

    def test_find_branches_between_removed(self):
        repos_url = self.make_svn_repository("a")

        repos = Repository.open(repos_url)
        repos.set_layout(TrunkLayout(0))

        dc = self.get_commit_editor(repos_url)
        trunk = dc.add_dir("trunk")
        trunk.add_file("trunk/foo").modify(b"data")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.delete("trunk")
        dc.close()

        self.assertEqual([("trunk", 1, True)],
                list(find_branches_between(repos._log, repos.transport, TrunkLayout(0), from_revnum=2, to_revnum=0)))
        self.assertEqual([("trunk", 1, True)],
                list(find_branches_between(repos._log, repos.transport, TrunkLayout(0), from_revnum=1, to_revnum=0)))

    def test_has_revision(self):
        repos_url = self.make_client('d', 'dc')
        repository = Repository.open(repos_url)
        self.build_tree({'dc/foo': b"data"})
        self.client_add("dc/foo")
        self.client_commit("dc", "My Message")
        self.assertTrue(repository.has_revision(
            repository.generate_revision_id(1, "", repository.get_mapping())))
        self.assertFalse(repository.has_revision("some-other-revision"))

    def test_has_revision_none(self):
        repos_url = self.make_client('d', 'dc')
        repository = Repository.open(repos_url)
        self.assertTrue(repository.has_revision(None))

    def test_has_revision_future(self):
        repos_url = self.make_client('d', 'dc')
        repository = Repository.open(repos_url)
        self.assertFalse(repository.has_revision(
            repository.get_mapping().revision_id_foreign_to_bzr((repository.uuid, "", 5))))

    def test_get_parent_map(self):
        repos_url = self.make_client('d', 'dc')
        self.build_tree({'dc/foo': b"data"})
        self.client_add("dc/foo")
        self.client_commit("dc", "My Message")
        self.build_tree({'dc/foo': b"data2"})
        self.client_commit("dc", "Second Message")
        repository = Repository.open(repos_url)
        mapping = repository.get_mapping()
        revid = repository.generate_revision_id(0, "", mapping)
        self.assertEqual({revid: (NULL_REVISION,)}, repository.get_parent_map([revid]))
        revid = repository.generate_revision_id(1, "", mapping)
        self.assertEqual({revid: (repository.generate_revision_id(0, "", mapping),)}, repository.get_parent_map([revid]))
        revid = repository.generate_revision_id(2, "", mapping)
        self.assertEqual({revid: (repository.generate_revision_id(1, "", mapping),)},
            repository.get_parent_map([revid]))
        self.assertEqual({}, repository.get_parent_map(["notexisting"]))

    def test_get_revision_delta(self):
        repos_url = self.make_svn_repository('d')

        dc = self.get_commit_editor(repos_url)
        dc.add_file("foo").modify(b"data")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.open_file("foo").modify(b"data2")
        dc.close()

        r = Repository.open(repos_url)
        r.set_layout(RootLayout())
        d1 = r.get_revision_delta(r.generate_revision_id(1, "", r.get_mapping()))
        self.assertEquals(None, d1.unchanged)
        self.assertEquals(1, len(d1.added))
        self.assertEquals("foo", d1.added[0][0])
        self.assertEquals(0, len(d1.modified))
        self.assertEquals(0, len(d1.removed))

        d2 = r.get_revision_delta(r.generate_revision_id(2, "", r.get_mapping()))
        self.assertEquals(None, d2.unchanged)
        self.assertEquals(0, len(d2.added))
        self.assertEquals("foo", d2.modified[0][0])
        self.assertEquals(0, len(d2.removed))

    def test_revision_svk_parent(self):
        repos_url = self.make_client('d', 'dc')
        self.build_tree({'dc/trunk/foo': b"data", 'dc/branches/foo': None})
        self.client_add("dc/trunk")
        self.client_add("dc/branches")
        self.client_commit("dc", "My Message")
        self.client_update("dc")
        self.build_tree({'dc/trunk/foo': b"data2"})
        repository = Repository.open(repos_url)
        repository.set_layout(TrunkLayout(0))
        self.client_set_prop("dc/trunk", "svk:merge",
            "%s:/branches/foo:1\n" % repository.uuid)
        self.client_commit("dc", "Second Message")
        mapping = repository.get_mapping()
        self.assertEqual((repository.generate_revision_id(1, "trunk", mapping),
            repository.generate_revision_id(1, "branches/foo", mapping)),
                repository.get_revision(
                    repository.generate_revision_id(2, "trunk", mapping)).parent_ids)

    def test_get_revision(self):
        repos_url = self.make_client('d', 'dc')
        repository = Repository.open(repos_url)
        self.assertRaises(NoSuchRevision, repository.get_revision,
                "nonexisting")
        self.build_tree({'dc/foo': b"data"})
        self.client_add("dc/foo")
        self.client_commit("dc", "My Message")
        self.client_update("dc")
        self.build_tree({'dc/foo': b"data2"})
        (num, date, author) = self.client_commit("dc", "Second Message")
        repository = Repository.open(repos_url)
        mapping = repository.get_mapping()
        rev = repository.get_revision(
            repository.generate_revision_id(2, "", mapping))
        self.assertEqual((repository.generate_revision_id(1, "", mapping),),
                rev.parent_ids)
        self.assertEqual(rev.revision_id,
                repository.generate_revision_id(2, "", mapping))
        self.assertEqual(author, rev.committer)
        self.assertIsInstance(rev.properties, dict)

    def test_get_revision_zero(self):
        repos_url = self.make_client('d', 'dc')
        repository = Repository.open(repos_url)
        mapping = repository.get_mapping()
        rev = repository.get_revision(
            repository.generate_revision_id(0, "", mapping))
        self.assertEqual(repository.generate_revision_id(0, "", mapping),
                         rev.revision_id)
        self.assertEqual("", rev.committer)
        self.assertEqual({}, rev.properties)
        self.assertEqual(0, rev.timezone)

    def test_get_inventory(self):
        repos_url = self.make_svn_repository('d')
        repository = Repository.open(repos_url)
        self.assertRaises(NotImplementedError, repository.get_inventory,
                "nonexisting")

    def test_generate_revision_id(self):
        repos_url = self.make_client('d', 'dc')
        self.build_tree({'dc/bla/bloe': None})
        self.client_add("dc/bla")
        self.client_commit("dc", "bla")
        repository = Repository.open(repos_url)
        mapping = repository.get_mapping()
        self.assertEqual(
               mapping.revision_id_foreign_to_bzr((repository.uuid, "bla/bloe", 1)),
            repository.generate_revision_id(1, "bla/bloe", mapping))

    def test_generate_revision_id_zero(self):
        repos_url = self.make_client('d', 'dc')
        repository = Repository.open(repos_url)
        mapping = repository.get_mapping()
        self.assertEqual(mapping.revision_id_foreign_to_bzr((repository.uuid, "", 0)),
                repository.generate_revision_id(0, "", mapping))

    def test_lookup_revision_id(self):
        repos_url = self.make_client('d', 'dc')
        self.build_tree({'dc/bloe': None})
        self.client_add("dc/bloe")
        self.client_commit("dc", "foobar")
        repository = Repository.open(repos_url)
        self.assertRaises(NoSuchRevision, repository.lookup_bzr_revision_id,
            "nonexisting")
        mapping = repository.get_mapping()
        self.assertEqual(((repository.uuid, "bloe", 1), mapping),
            repository.lookup_bzr_revision_id(
                repository.generate_revision_id(1, "bloe", mapping))[:2])

    def test_lookup_revision_id_invalid_uuid(self):
        repos_url = self.make_client('d', 'dc')
        repository = Repository.open(repos_url)
        mapping = repository.get_mapping()
        self.assertRaises(NoSuchRevision,
            repository.lookup_bzr_revision_id,
                mapping.revision_id_foreign_to_bzr(("invaliduuid", "", 0)))

    def test_check(self):
        repos_url = self.make_client('d', 'dc')
        self.build_tree({'dc/foo': b"data"})
        self.client_add("dc/foo")
        self.client_commit("dc", "My Message")
        repository = Repository.open(repos_url)
        mapping = repository.get_mapping()
        repository.check([
            repository.generate_revision_id(0, "", mapping),
            repository.generate_revision_id(1, "", mapping)])

    def test_copy_contents_into(self):
        repos_url = self.make_client('d', 'dc')
        self.build_tree({'dc/foo/bla': b"data"})
        self.client_add("dc/foo")
        self.client_commit("dc", "My Message")
        self.build_tree({'dc/foo/blo': b"data2", "dc/bar/foo": b"data3", 'dc/foo/bla': b"data"})
        self.client_add("dc/foo/blo")
        self.client_add("dc/bar")
        self.client_commit("dc", "Second Message")
        repository = Repository.open(repos_url)
        repository.set_layout(RootLayout())
        mapping = repository.get_mapping()

        to_controldir = ControlDir.create("e")
        to_repos = to_controldir.create_repository()

        repository.copy_content_into(to_repos,
                repository.generate_revision_id(2, "", mapping))

        self.assertTrue(repository.has_revision(
            repository.generate_revision_id(2, "", mapping)))
        self.assertTrue(repository.has_revision(
            repository.generate_revision_id(1, "", mapping)))

    def assertRevmetaLogEquals(self, expected, got, msg=None):
        got = list(got)
        if len(expected) != len(got):
            self.assertEquals(expected, got, msg)
        for (branch_path, changes, revnum), g in zip(expected, got):
            self.assertEquals(revnum, g.metarev.revnum)
            self.assertEquals(branch_path, g.metarev.branch_path)
            self.assertChangedPathsEquals(changes, g.metarev.paths)

    def test_fetch_property_change_only_trunk(self):
        repos_url = self.make_svn_repository('d')

        dc = self.get_commit_editor(repos_url)
        trunk = dc.add_dir("trunk")
        trunk.add_file("trunk/bla").modify(b"data")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        trunk = dc.open_dir("trunk")
        trunk.change_prop("some:property", "some data\n")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        trunk = dc.open_dir("trunk")
        trunk.change_prop("some:property2", "some data\n")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        trunk = dc.open_dir("trunk")
        trunk.change_prop("some:property", "some other data\n")
        dc.close()

        oldrepos = Repository.open(repos_url)
        oldrepos.set_layout(TrunkLayout(0))
        self.assertRevmetaLogEquals([('trunk', {'trunk': (u'M', None, -1, NODE_DIR)}, 3),
                           ('trunk', {'trunk': (u'M', None, -1, NODE_DIR)}, 2),
                           ('trunk', {'trunk/bla': (u'A', None, -1, NODE_FILE), 'trunk': (u'A', None, -1, NODE_DIR)}, 1)],
                   oldrepos._revmeta_provider.iter_reverse_branch_changes("trunk", 3, 0))

    def test_control_code_msg(self):
        if ra.version()[1] >= 5:
            raise TestSkipped("Test not runnable with Subversion >= 1.5")

        repos_url = self.make_svn_repository('d')

        cb = self.get_commit_editor(repos_url)
        cb.add_dir("trunk")
        cb.close("\x24")

        cb = self.get_commit_editor(repos_url)
        trunk = cb.open_dir("trunk")
        hosts = trunk.add_file("trunk/hosts")
        hosts.modify(b"hej2")
        cb.close("bla\xfcbla") #2

        cb = self.get_commit_editor(repos_url)
        trunk = cb.open_dir("trunk")
        trunk.open_file("trunk/hosts").modify(b"hej3")
        cb.close("a\x0cb") #3

        cb = self.get_commit_editor(repos_url)
        branches = cb.add_dir("branches")
        foobranch = branches.add_dir("branches/foobranch")
        foobranch.add_file("branches/foobranch/file").modify()
        cb.close("foohosts") #4

        oldrepos = Repository.open(repos_url)
        oldrepos.set_layout(TrunkLayout(0))
        dir = ControlDir.create("f")
        newrepos = dir.create_repository()
        oldrepos.copy_content_into(newrepos)

        mapping = oldrepos.get_mapping()

        self.assertTrue(newrepos.has_revision(
            oldrepos.generate_revision_id(1, "trunk", mapping)))
        self.assertTrue(newrepos.has_revision(
            oldrepos.generate_revision_id(2, "trunk", mapping)))
        self.assertTrue(newrepos.has_revision(
            oldrepos.generate_revision_id(3, "trunk", mapping)))
        self.assertTrue(newrepos.has_revision(
            oldrepos.generate_revision_id(4, "branches/foobranch", mapping)))
        self.assertFalse(newrepos.has_revision(
            oldrepos.generate_revision_id(4, "trunk", mapping)))
        self.assertFalse(newrepos.has_revision(
            oldrepos.generate_revision_id(2, "", mapping)))

        rev = newrepos.get_revision(oldrepos.generate_revision_id(1, "trunk", mapping))
        self.assertEqual("$", rev.message)

        rev = newrepos.get_revision(
            oldrepos.generate_revision_id(2, "trunk", mapping))
        self.assertEqual('bla\xc3\xbcbla', rev.message.encode("utf-8"))

        rev = newrepos.get_revision(oldrepos.generate_revision_id(3, "trunk", mapping))
        self.assertEqual(u"a\\x0cb", rev.message)

    def test_set_layout(self):
        repos_url = self.make_svn_repository('d')
        repos = Repository.open(repos_url)
        repos.set_layout(RootLayout())

    def testlhs_revision_parent_none(self):
        repos_url = self.make_svn_repository('d')
        repos = Repository.open(repos_url)
        repos.set_layout(RootLayout())
        revmeta0 = repos._revmeta_provider.get_revision("", 0)
        self.assertEquals(NULL_REVISION, revmeta0.get_lhs_parent_revid(repos.get_mapping(), None))

    def testlhs_revision_parent_first(self):
        repos_url = self.make_client('d', 'dc')
        repos = Repository.open(repos_url)
        repos.set_layout(RootLayout())
        self.build_tree({'dc/adir/afile': b"data"})
        self.client_add("dc/adir")
        self.client_commit("dc", "Initial commit")
        mapping = repos.get_mapping()
        revmeta0 = repos._revmeta_provider.get_revision("", 0)
        revmeta1 = repos._revmeta_provider.get_revision("", 1)
        self.assertEquals(repos.generate_revision_id(0, "", mapping),
            revmeta1.get_lhs_parent_revid(mapping, revmeta0))

    def testlhs_revision_parent_simple(self):
        repos_url = self.make_client('d', 'dc')
        self.build_tree({'dc/trunk/adir/afile': b"data",
                         'dc/trunk/adir/stationary': None,
                         'dc/branches/abranch': None})
        self.client_add("dc/trunk")
        self.client_add("dc/branches")
        self.client_commit("dc", "Initial commit")
        self.build_tree({'dc/trunk/adir/afile': b"bla"})
        self.client_commit("dc", "Incremental commit")
        repos = Repository.open(repos_url)
        repos.set_layout(TrunkLayout(0))
        mapping = repos.get_mapping()
        revmeta1 = repos._revmeta_provider.get_revision("trunk", 1)
        revmeta2 = repos._revmeta_provider.get_revision("trunk", 2)
        self.assertEquals(repos.generate_revision_id(1, "trunk", mapping), \
                revmeta2.get_lhs_parent_revid(mapping, revmeta1))

    def testlhs_revision_parent_copied(self):
        repos_url = self.make_svn_repository('d')

        dc = self.get_commit_editor(repos_url)
        py = dc.add_dir("py")
        trunk = py.add_dir("py/trunk")
        adir = trunk.add_dir("py/trunk/adir")
        adir.add_file("py/trunk/adir/afile").modify(b"data")
        adir.add_dir("py/trunk/adir/stationary")
        dc.close() #1

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("de", "py")
        dc.close() #2

        dc = self.get_commit_editor(repos_url)
        de = dc.open_dir("de")
        trunk = de.open_dir("de/trunk")
        adir = trunk.open_dir("de/trunk/adir")
        adir.open_file("de/trunk/adir/afile").modify(b"bla")
        dc.close()
        repos = Repository.open(repos_url)
        repos.set_layout(TrunkLayout(1))
        mapping = repos.get_mapping()
        revmeta3 = repos._revmeta_provider.get_revision("de/trunk", 3)
        self.assertEquals(repos.generate_revision_id(1, "py/trunk", mapping), \
                revmeta3.get_lhs_parent_revid(mapping, revmeta3.get_lhs_parent_revmeta(mapping)))

    def test_mainline_revision_copied(self):
        repos_url = self.make_svn_repository('d')

        dc = self.get_commit_editor(repos_url)
        py = dc.add_dir("py")
        trunk = py.add_dir("py/trunk")
        adir = trunk.add_dir("py/trunk/adir")
        afile = adir.add_file("py/trunk/adir/afile").modify(b"data")
        stationary = adir.add_dir("py/trunk/adir/stationary")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        de = dc.add_dir("de")
        de.add_dir("de/trunk", "py/trunk")
        dc.close()

        repos = Repository.open(repos_url)
        repos.set_layout(TrunkLayout(1))
        mapping = repos.get_mapping()
        revmeta2 = repos._revmeta_provider.get_revision("de/trunk", 2)
        self.assertEquals(repos.generate_revision_id(1, "py/trunk", mapping), \
                revmeta2.get_lhs_parent_revid(mapping, revmeta2.get_lhs_parent_revmeta(mapping)))

    def test_mainline_revision_nested_deleted(self):
        repos_url = self.make_svn_repository('d')

        dc = self.get_commit_editor(repos_url)
        py = dc.add_dir("py")
        trunk = py.add_dir("py/trunk")
        adir = trunk.add_dir("py/trunk/adir")
        afile = adir.add_file("py/trunk/adir/file").modify(b"data")
        adir.add_dir("py/trunk/adir/stationary")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("de", "py")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        de = dc.open_dir("de")
        trunk = de.open_dir("de/trunk")
        trunk.delete("de/trunk/adir")
        dc.close()

        repos = Repository.open(repos_url)
        repos.set_layout(TrunkLayout(1))
        mapping = repos.get_mapping()
        revmeta3 = repos._revmeta_provider.get_revision("de/trunk", 3)
        self.assertEquals(repos.generate_revision_id(1, "py/trunk", mapping),
            revmeta3.get_lhs_parent_revid(mapping, revmeta3.get_lhs_parent_revmeta(mapping)))

    def test_fetch_file_from_non_branch(self):
        repos_url = self.make_svn_repository('d')

        dc = self.get_commit_editor(repos_url)
        old_trunk = dc.add_dir("old-trunk")
        lib = old_trunk.add_dir("old-trunk/lib")
        lib.add_file("old-trunk/lib/file").modify(b"data")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        trunk = dc.add_dir("trunk")
        lib = trunk.add_dir("trunk/lib")
        lib.add_file("trunk/lib/file", "old-trunk/lib/file")
        dc.close()

        oldrepos = Repository.open(repos_url)
        oldrepos.set_layout(TrunkLayout(0))
        dir = ControlDir.create("f")
        newrepos = dir.create_repository()
        oldrepos.copy_content_into(newrepos)

        mapping = oldrepos.get_mapping()
        branch = Branch.open("%s/trunk" % repos_url)
        self.assertEquals(branch.mapping, mapping)
        self.assertEqual((1, oldrepos.generate_revision_id(2, "trunk", mapping)),
                     branch.last_revision_info())
