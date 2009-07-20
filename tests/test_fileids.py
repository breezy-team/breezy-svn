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

"""File id tests."""

from bzrlib.bzrdir import BzrDir
from bzrlib.errors import RevisionNotPresent
from bzrlib.repository import Repository
from bzrlib.trace import mutter
from bzrlib.tests import (
    TestCase,
    TestCaseWithMemoryTransport,
    )
from bzrlib.workingtree import WorkingTree

from bzrlib.plugins.svn.fileids import (
    FileIdMapCache,
    get_local_changes,
    idmap_lookup,
    idmap_reverse_lookup,
    simple_apply_changes,
    )
from bzrlib.plugins.svn.mapping import (
    mapping_registry,
    )
from bzrlib.plugins.svn.layout.standard import (
    RootLayout,
    TrunkLayout,
    )
from bzrlib.plugins.svn.tests import SubversionTestCase

class MockRepo(object):

    def __init__(self, mapping, uuid="uuid"):
        self.uuid = uuid

    def lookup_revision_id(self, revid):
        ret = self.mapping.revision_id_foreign_to_bzr(revid)
        return ret, None


class TestComplexFileids(SubversionTestCase):
    # branchtagcopy.dump
    # changeaftercp.dump
    # combinedbranch.dump
    # executable.dump
    # ignore.dump
    # inheritance.dump
    # movebranch.dump
    # movefileorder.dump
    # recreatebranch.dump
    def test_simplemove(self):
        repos_url = self.make_repository('d')

        dc = self.get_commit_editor(repos_url)
        dc.add_file("foo").modify("data")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.add_file("bar", "foo", 1)
        dc.delete("foo")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.open_file("bar").modify("data2")
        dc.close()

        repository = Repository.open(repos_url)
        repository.set_layout(RootLayout())
        mapping = repository.get_mapping()

        inv1 = repository.get_inventory(
                repository.generate_revision_id(1, "", mapping))
        inv2 = repository.get_inventory(
                repository.generate_revision_id(2, "", mapping))
        mutter('inv1: %r' % inv1.entries())
        mutter('inv2: %r' % inv2.entries())
        self.assertNotEqual(None, inv1.path2id("foo"))
        self.assertIs(None, inv2.path2id("foo"))
        self.assertNotEqual(None, inv2.path2id("bar"))
        self.assertNotEqual(inv1.path2id("foo"), inv2.path2id("blie"))
        self.assertNotEqual(inv2.path2id("bar"), inv2.path2id("blie"))

    def test_simplecopy(self):
        repos_url = self.make_repository('d')

        dc = self.get_commit_editor(repos_url)
        dc.add_file("foo").modify("data")
        dc.add_file("blie").modify("bloe")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.add_file("bar", "foo", 1).modify("data2")
        dc.close()

        bzrdir = BzrDir.open(repos_url)
        repository = bzrdir.find_repository()

        mapping = repository.get_mapping()

        inv1 = repository.get_inventory(
                repository.generate_revision_id(1, "", mapping))
        inv2 = repository.get_inventory(
                repository.generate_revision_id(2, "", mapping))
        self.assertNotEqual(inv1.path2id("foo"), inv2.path2id("bar"))
        self.assertNotEqual(inv1.path2id("foo"), inv2.path2id("blie"))
        self.assertIs(None, inv1.path2id("bar"))
        self.assertNotEqual(None, inv1.path2id("blie"))

    def test_simpledelete(self):
        repos_url = self.make_repository('d')

        dc = self.get_commit_editor(repos_url)
        dc.add_file("foo").modify("data")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.delete("foo")
        dc.close()

        bzrdir = BzrDir.open(repos_url)
        repository = bzrdir.find_repository()
        mapping = repository.get_mapping()

        inv1 = repository.get_inventory(
                repository.generate_revision_id(1, "", mapping))
        inv2 = repository.get_inventory(
                repository.generate_revision_id(2, "", mapping))
        self.assertNotEqual(None, inv1.path2id("foo"))
        self.assertIs(None, inv2.path2id("foo"))

    def test_replace(self):
        repos_url = self.make_repository('d')

        dc = self.get_commit_editor(repos_url)
        dc.add_file("foo").modify("data")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.delete("foo")
        dc.add_file("foo").modify("data")
        dc.close()

        bzrdir = BzrDir.open(repos_url)
        repository = bzrdir.find_repository()

        mapping = repository.get_mapping()

        inv1 = repository.get_inventory(
                repository.generate_revision_id(1, "", mapping))
        inv2 = repository.get_inventory(
                repository.generate_revision_id(2, "", mapping))
        self.assertNotEqual(inv1.path2id("foo"), inv2.path2id("foo"))

    def test_copy_branch(self):
        repos_url = self.make_repository('d')

        dc = self.get_commit_editor(repos_url)
        trunk = dc.add_dir("trunk")
        dir = trunk.add_dir("trunk/dir")
        dir.add_file("trunk/dir/file").modify("data")
        dc.add_dir("branches")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        branches = dc.open_dir("branches")
        branches.add_dir("branches/mybranch", "trunk", 1)
        dc.close()

        bzrdir = BzrDir.open(repos_url + "/branches/mybranch")
        repository = bzrdir.find_repository()

        mapping = repository.get_mapping()

        inv1 = repository.get_inventory(
                repository.generate_revision_id(1, "trunk", mapping))
        inv2 = repository.get_inventory(
                repository.generate_revision_id(2, "branches/mybranch", mapping))
        self.assertEqual(inv1.path2id("dir"), inv2.path2id("dir"))
        self.assertEqual(inv1.path2id("dir/file"), inv2.path2id("dir/file"))

        rm_provider = repository._revmeta_provider
        fileid, revid, child_create_revid = repository.get_fileid_map(rm_provider.get_revision("branches/mybranch", 2), mapping).as_dict()["dir/file"]
        self.assertEqual(fileid, inv1.path2id("dir/file"))
        self.assertEqual(repository.generate_revision_id(1, "trunk", mapping), revid)


class TestFileMapping(TestCase):

    def setUp(self):
        TestCase.setUp(self)
        self.generate_file_id = lambda (uuid, bp, revnum), ip: "%d@%s:%s:%s" % (revnum, uuid, bp, ip)

    def apply_mappings(self, mappings, renames={}):
        map = {}
        brns = mappings.keys()
        brns.sort()
        for r in brns:
            (revnum, branchpath) = r
            def new_file_id(x):
                if renames.has_key(r) and renames[r].has_key(x):
                    return renames[r][x]
                return self.generate_file_id(("uuid", branchpath, revnum), x)
            revmap = simple_apply_changes(new_file_id, mappings[r])
            map.update(dict([(x, (revmap[x], r)) for x in revmap]))
        return map

    def test_simple(self):
        map = self.apply_mappings({(1, ""): {u"foo": ('A', None)}})
        self.assertEqual({ u'foo': ("1@uuid::foo",
                                       (1, ""))
                         }, map)

    def test_simple_add(self):
        map = self.apply_mappings({(1, ""): {u"": ('A', None), u"foo": ('A', None)}})
        self.assertEqual({
            u'': ('1@uuid::', (1, "")),
            u'foo': ("1@uuid::foo", (1, "")) 
            }, map)

    def test_copy(self):
        map = self.apply_mappings({
                (1, ""): {
                                   u"foo": ('A', None), 
                                   u"foo/blie": ('A', None),
                                   u"foo/bla": ('A', None)},
                (2, ""): {
                                   u"foob": ('A', ('foo', 1)), 
                                   u"foob/bla": ('M', None)}
                })
        self.assertEquals(map,
            {u'foo': (u'1@uuid::foo', (1, '')),
             u'foo/bla': (u'1@uuid::foo/bla', (1, '')),
             u'foo/blie': (u'1@uuid::foo/blie', (1, '')),
             u'foob': (u'2@uuid::foob', (2, ''))})

    def test_touchparent(self):
        map = self.apply_mappings(
                {(1, ""): {
                                   u"foo": ('A', None), 
                                   u"foo/bla": ('A', None)},
                 (2, ""): {
                                   u"foo/bla": ('M', None)}
                })
        self.assertEqual((1, ""), 
                         map[u"foo"][1])
        self.assertEqual((1, ""), 
                         map[u"foo/bla"][1])

    def test_usemap(self):
        map = self.apply_mappings(
                {(1, ""): {
                                   u"foo": ('A', None), 
                                   u"foo/bla": ('A', None)},
                 (2, ""): {
                                   u"foo/bla": ('M', None)}
                 }, 
                renames={(1, ""): {u"foo": "myid"}})
        self.assertEqual("myid", map[u"foo"][0])

    def test_usemap_later(self):
        map = self.apply_mappings(
                {(1, ""): {
                                   u"foo": ('A', None), 
                                   u"foo/bla": ('A', None)},
                 (2, ""): {
                                   u"foo/bla": ('M', None)}
                 }, 
                renames={(2, ""): {u"foo": "myid"}})
        self.assertEqual("1@uuid::foo", map[u"foo"][0])
        self.assertEqual((1, ""), map[u"foo"][1])


class GetMapTests(SubversionTestCase):

    def setUp(self):
        super(GetMapTests, self).setUp()
        self.repos_url = self.make_repository("d")
        self.repos = Repository.open(self.repos_url)

    def get_map(self, path, revnum, mapping):
        rm_provider = self.repos._revmeta_provider
        revmeta = rm_provider.get_revision(path, revnum)
        fileid_map = self.repos.get_fileid_map(revmeta, mapping)
        return fileid_map.as_dict()

    def test_empty(self):
        self.repos.set_layout(RootLayout())
        self.mapping = self.repos.get_mapping()
        self.assertEqual({"": (self.mapping.generate_file_id((self.repos.uuid, "", 0), u""), self.repos.generate_revision_id(0, "", self.mapping), None)}, self.get_map("", 0, self.mapping))

    def test_empty_trunk(self):
        self.repos.set_layout(TrunkLayout(0))
        self.mapping = self.repos.get_mapping()
        dc = self.get_commit_editor(self.repos_url)
        dc.add_dir("trunk")
        dc.close()

        self.assertEqual({"": (self.mapping.generate_file_id((self.repos.uuid, "trunk", 1), u""), self.repos.generate_revision_id(1, "trunk", self.mapping), None)}, 
                self.get_map("trunk", 1, self.mapping))

    def test_change_parent(self):
        self.repos.set_layout(TrunkLayout(0))
        self.mapping = self.repos.get_mapping()
        
        dc = self.get_commit_editor(self.repos_url)
        dc.add_dir("trunk")
        dc.close()

        dc = self.get_commit_editor(self.repos_url)
        dc.open_dir("trunk").add_file("trunk/file").modify("data")
        dc.close()

        self.assertEqual({
            "": (self.mapping.generate_file_id((self.repos.uuid, "trunk", 1), u""), 
                 self.repos.generate_revision_id(1, "trunk", self.mapping),
                 None), 
            "file": (self.mapping.generate_file_id((self.repos.uuid, "trunk", 2), u"file"), 
                     self.repos.generate_revision_id(2, "trunk", self.mapping),
                     None)}, 
            self.get_map("trunk", 2, self.mapping))

    def test_change_updates(self):
        self.repos.set_layout(TrunkLayout(0))
        self.mapping = self.repos.get_mapping()

        dc = self.get_commit_editor(self.repos_url)
        dc.add_dir("trunk")
        dc.close()

        dc = self.get_commit_editor(self.repos_url)
        dc.open_dir("trunk").add_file("trunk/file").modify("data")
        dc.close()

        dc = self.get_commit_editor(self.repos_url)
        dc.open_dir("trunk").open_file("trunk/file").modify("otherdata")
        dc.close()

        self.assertEqual({
            "": (self.mapping.generate_file_id((self.repos.uuid, "trunk", 1), u""), 
                 self.repos.generate_revision_id(1, "trunk", self.mapping),
                 None), 
            "file": (self.mapping.generate_file_id((self.repos.uuid, "trunk", 2), u"file"), 
                     self.repos.generate_revision_id(3, "trunk", self.mapping),
                     None)}, 
            self.get_map("trunk", 3, self.mapping))

    def test_sibling_unrelated(self):
        self.repos.set_layout(TrunkLayout(0))
        self.mapping = self.repos.get_mapping()

        dc = self.get_commit_editor(self.repos_url)
        dc.add_dir("trunk")
        dc.close()

        dc = self.get_commit_editor(self.repos_url)
        trunk = dc.open_dir("trunk")
        trunk.add_file("trunk/file").modify("data")
        trunk.add_file("trunk/bar").modify("data2")
        dc.close()

        dc = self.get_commit_editor(self.repos_url)
        trunk = dc.open_dir("trunk")
        trunk.open_file("trunk/file").modify('otherdata')
        dc.close()

        self.assertEqual({
            "": (self.mapping.generate_file_id((self.repos.uuid, "trunk", 1), u""), 
                 self.repos.generate_revision_id(1, "trunk", self.mapping),
                 None), 
            "bar": (self.mapping.generate_file_id((self.repos.uuid, "trunk", 2), u"bar"), 
                    self.repos.generate_revision_id(2, "trunk", self.mapping),
                    None), 
            "file": (self.mapping.generate_file_id((self.repos.uuid, "trunk", 2), u"file"), 
                     self.repos.generate_revision_id(3, "trunk", self.mapping),
                     None)}, 
            self.get_map("trunk", 3, self.mapping))

    def test_copy(self):
        self.repos.set_layout(TrunkLayout(0))
        self.mapping = self.repos.get_mapping()

        dc = self.get_commit_editor(self.repos_url)
        dc.add_dir("trunk")
        dc.close()

        dc = self.get_commit_editor(self.repos_url)
        trunk = dc.open_dir("trunk")
        trunk.add_file("trunk/file").modify("data")
        dc.close()

        dc = self.get_commit_editor(self.repos_url)
        trunk = dc.open_dir("trunk")
        trunk.add_file("trunk/bar", "trunk/file", 2)
        dc.close()

        self.assertEqual({
            "": (self.mapping.generate_file_id((self.repos.uuid, "trunk", 1), u""), 
                 self.repos.generate_revision_id(1, "trunk", self.mapping),
                 None), 
            "bar": (self.mapping.generate_file_id((self.repos.uuid, "trunk", 3), u"bar"), 
                    self.repos.generate_revision_id(3, "trunk", self.mapping),
                    (self.repos.uuid, "trunk", 3)), 
            "file": (self.mapping.generate_file_id((self.repos.uuid, "trunk", 2), u"file"), 
                     self.repos.generate_revision_id(2, "trunk", self.mapping),
                     None)}, 
            self.get_map("trunk", 3, self.mapping))

    def test_copy_nested_modified(self):
        self.repos.set_layout(TrunkLayout(0))
        self.mapping = self.repos.get_mapping()

        dc = self.get_commit_editor(self.repos_url)
        dc.add_dir("trunk")
        dc.close()

        dc = self.get_commit_editor(self.repos_url)
        trunk = dc.open_dir("trunk")
        dir = trunk.add_dir("trunk/dir")
        dir.add_file("trunk/dir/file").modify("data")
        dc.close()

        dc = self.get_commit_editor(self.repos_url)
        trunk = dc.open_dir("trunk")
        dir = trunk.add_dir("trunk/bar", "trunk/dir")
        dir.open_file("trunk/bar/file").modify("data2")
        dc.close()

        self.assertEqual({
          "": (self.mapping.generate_file_id((self.repos.uuid, "trunk", 1), u""), 
            self.repos.generate_revision_id(1, "trunk", self.mapping),
            None), 
          "dir": (self.mapping.generate_file_id((self.repos.uuid, "trunk", 2), u"dir"), 
                self.repos.generate_revision_id(2, "trunk", self.mapping),
                None),
          "dir/file": (self.mapping.generate_file_id((self.repos.uuid, "trunk", 2), u"dir/file"), 
              self.repos.generate_revision_id(2, "trunk", self.mapping),
              None),
          "bar": (self.mapping.generate_file_id((self.repos.uuid, "trunk", 3), u"bar"), 
              self.repos.generate_revision_id(3, "trunk", self.mapping),
              (self.repos.uuid, "trunk", 3)),
          "bar/file": (self.mapping.generate_file_id((self.repos.uuid, "trunk", 3), u"bar/file"), 
              self.repos.generate_revision_id(3, "trunk", self.mapping),
              (self.repos.uuid, "trunk", 3))},
            self.get_map("trunk", 3, self.mapping))

    def test_304134(self):
        self.make_checkout(self.repos_url, 'svn-co')
        self.build_tree({
            'svn-co/subdir1/file1': '', 
            'svn-co/subdir1/file2': '',
            'svn-co/subdir2/file3': '',
            'svn-co/subdir2/file4': ''})
        self.client_add('svn-co/subdir1')
        self.client_add('svn-co/subdir2')
        self.client_commit('svn-co', "Initial tree.")
        self.build_tree({'svn-co/subdir1/subdir3/file5': '',
                         'svn-co/subdir1/subdir3/file6': ''})
        self.client_add('svn-co/subdir1/subdir3')
        self.client_commit('svn-co', "More files.")
        self.build_tree({
            'svn-co/subdir2/file3': 'addaline',
            'svn-co/subdir2/file4': 'addbline',
            'svn-co/subdir2/file7': ''})
        self.client_add('svn-co/subdir2/file7')
        self.client_set_prop("svn-co/subdir2/file4", "svn:executable", "true")
        self.client_copy("svn-co/subdir2", "svn-co/subdir1")
        self.client_delete("svn-co/subdir2")
        self.client_commit("svn-co", "Directory move with modifications.")
        self.client_update("svn-co")
        wt = WorkingTree.open("svn-co")
        wt.lock_write()
        wt.update()
        wt.unlock()
        wt = None
        wt = WorkingTree.open("svn-co/subdir1")
        wt.lock_write()
        #wt.update()
        wt.unlock()


class FileIdMapCacheTests(TestCaseWithMemoryTransport):

    def setUp(self):
        super(FileIdMapCacheTests, self).setUp()
        self.cache = FileIdMapCache(self.get_transport())

    def test_nonexisting(self):
        self.assertRaises(RevisionNotPresent, self.cache.load, "bla")

    def test_empty(self):
        self.cache.save("bla", [], {})
        self.assertEquals({}, self.cache.load("bla"))

    def test_simple(self):
        data = {u"bla": ("myfileid", "myrev", None)}
        self.cache.save("bla", [], data)
        self.assertEquals(data, self.cache.load("bla"))

    def test_multiple(self):
        data = {u"bla": ("myfileid", "myrev", None),
                u"bloe/blie": ("otherfileid", "myrev", None)}
        self.cache.save("bla", [], data)
        self.assertEquals(data, self.cache.load("bla"))

    def test_parents(self):
        data1 = {u"bla": ("myfileid", "myrev", None)}
        data2 = {u"bla": ("myfileid", "mynewrev", None)}
        self.cache.save("bla1", [], data1)
        self.cache.save("bla2", ["bla1"], data2)
        self.assertEquals(data1, self.cache.load("bla1"))
        self.assertEquals(data2, self.cache.load("bla2"))

    def test_store_child_create_revid(self):
        data = {u"bla": ("myfileid", "mynewrev", 
                    ("myuuid", "mybp", 42))}
        self.cache.save("bla", [], data)
        self.assertEquals(data, self.cache.load("bla"))


class LookupTests(TestCase):

    def test_trivial(self):
        idmap = {"filename": ("myfileid", "myrev", None)}
        self.assertEquals(("myfileid", "myrev", None), idmap_lookup(idmap.__getitem__, None, "filename"))

    def test_nonexistent(self):
        idmap = {}
        self.assertRaises(KeyError, idmap_lookup, idmap.__getitem__, None, "filename")

    def test_implicit(self):
        idmap = {"parent": ("parentfileid", "parentrev", ("someuuid", "somebp", 42))}
        mapping = mapping_registry.get_default()()
        self.assertEquals((mapping.generate_file_id(("someuuid", "somebp", 42), u"parent/foo"),
                           mapping.revision_id_foreign_to_bzr(("someuuid", "somebp", 42)),
                         ('someuuid', 'somebp', 42)),
                          idmap_lookup(idmap.__getitem__, mapping, u"parent/foo"))

    def test_not_implicit_asserts(self):
        idmap = {"parent": ("parentfileid", "parentrev", None)}
        mapping = mapping_registry.get_default()()
        self.assertRaises(KeyError, idmap_lookup, idmap.__getitem__, mapping, u"parent/foo")


class ReverseLookupTests(TestCase):

    def test_simple(self):
        idmap = {"filename": ("myfileid", "myrev", None)}
        mapping = mapping_registry.get_default()()
        self.assertEquals("filename", idmap_reverse_lookup(idmap, mapping, "myfileid"))

    def test_nonexistant(self):
        idmap = {}
        mapping = mapping_registry.get_default()()
        self.assertRaises(KeyError, idmap_reverse_lookup, idmap, mapping, "myfileid")

    def test_implicit(self):
        idmap = {"parent": ("parentfileid", "parentrev", ("someuuid", "somebp", 42))}
        mapping = mapping_registry.get_default()()
        self.assertEquals(u"parent/foo",
                          idmap_reverse_lookup(idmap, mapping, 
                          mapping.generate_file_id(("someuuid", "somebp", 42), u"parent/foo")))

    def test_not_implicit(self):
        idmap = {"parent": ("parentfileid", "parentrev", None)}
        mapping = mapping_registry.get_default()()
        self.assertRaises(KeyError, 
                          idmap_reverse_lookup, idmap, mapping, 
                          mapping.generate_file_id(("someuuid", "somebp", 42), u"parent/foo"))


class LocalChangesTests(TestCase):

    def _generate_revid(self, revnum, path):
        return "%s:%d" % (path, revnum)

    def test_trivial_change(self):
        paths = { "trunk/path": ("M", None, -1)}
        branch = "trunk"
        self.assertEquals({"path": ("M", None)}, get_local_changes(paths, branch))

    def test_bug_352509(self):
        paths = { "trunk/internal/org.restlet": ('R', 'trunk/plugins/internal/org.restlet', 1111) }
        branch = "trunk/internal/org.restlet"
        self.assertEquals({"": ("M", None)},
            get_local_changes(paths, branch))
