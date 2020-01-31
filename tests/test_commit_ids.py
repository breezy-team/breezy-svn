# -*- coding: utf-8 -*-

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
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

"""Tests for the file id/revision generation in commit."""

import os
import shutil

from breezy import osutils
from breezy.controldir import ControlDir
from breezy.conflicts import ConflictList
from breezy.tests import (
    TestCaseWithTransport,
    treeshape,
    )
from breezy.tests.features import (
    SymlinkFeature,
    )

from breezy.workingtree import WorkingTree

from breezy.plugins.svn.tests import SubversionTestCase

class CommitIdTesting:
    """Test that file ids and file revisions are appropriately recorded."""

    def commit_tree(self, tree, revision_id=None):
        revid = tree.commit("This is a message", rev_id=revision_id)
        return tree.branch.repository.revision_tree(revid)

    def tree_items(self, tree):
        tree.lock_read()
        try:
            graph = tree._repository.get_file_graph()
            ret = {}
            for (path, versioned, kind, file_id, ie) in tree.list_files(
                    include_root=True, recursive=True):
                key = (ie.file_id, ie.revision)
                pkeys = graph.get_parent_map([key])
                parents = []
                for pf, pr in pkeys[key]:
                    assert pf == ie.file_id
                    parents.append(pr)
                ret[path] = (ie.file_id, ie.revision, parents)
        finally:
            tree.unlock()
        return ret

    def commit_tree_items(self, tree, revision_id=None):
        return self.tree_items(self.commit_tree(tree, revision_id))

    def prepare_wt(self, path):
        raise NotImplementedError(self.prepare_wt)

    def test_set_root(self):
        tree = self.prepare_wt('.')
        tree.lock_write()
        self.addCleanup(tree.unlock)
        tree.set_root_id("THEROOTID")
        items = self.commit_tree_items(tree, b"reva")
        self.assertEquals({
            "": ("THEROOTID", b"reva", [])}, items)

    def test_add_file(self):
        tree = self.prepare_wt('.')
        tree.lock_write()
        self.addCleanup(tree.unlock)
        tree.set_root_id("THEROOTID")
        self.build_tree_contents([('afile', 'contents')])
        tree.add(["afile"], ['THEFILEID'])
        items = self.commit_tree_items(tree, b"reva")
        self.assertEquals({
            "": ("THEROOTID", b"reva", []),
            "afile": ("THEFILEID", b"reva", []),
            }, items)

    def test_modify_file(self):
        tree = self.prepare_wt('.')
        tree.lock_write()
        self.addCleanup(tree.unlock)
        tree.set_root_id("therootid")
        self.build_tree_contents([
            ('afile', 'contents'),
            ("unchanged", "content")])
        tree.add(["afile", "unchanged"], ['thefileid', "unchangedid"])
        self.commit_tree(tree, b"reva")
        self.build_tree_contents([('afile', 'contents2')])
        items = self.commit_tree_items(tree, "revb")
        self.assertEquals({
            "": ("therootid", b"reva", []),
            "afile": ("thefileid", "revb", [b"reva"]),
            "unchanged": ("unchangedid", b"reva", []),
            }, items)

    def test_change_link_target(self):
        self.requireFeature(SymlinkFeature)
        tree = self.prepare_wt('.')
        tree.lock_write()
        self.addCleanup(tree.unlock)
        tree.set_root_id("therootid")
        os.symlink('oldtarget', 'link')
        tree.add(["link"], ['thefileid'])
        self.commit_tree(tree, b"reva")
        os.unlink('link')
        os.symlink('newtarget', 'link')
        items = self.commit_tree_items(tree, "revb")
        self.assertEquals({
            "": ("therootid", b"reva", []),
            "link": ("thefileid", "revb", [b"reva"]),
            }, items)

    def test_new_parent(self):
        tree = self.prepare_wt('.')
        tree.lock_write()
        self.addCleanup(tree.unlock)
        tree.set_root_id("therootid")
        self.build_tree_contents([
            ('afile', 'contents'),
            ('adir/', )])
        tree.add(["afile", "adir"],
                 ['thefileid', "thedirid"])
        self.commit_tree(tree, b"reva")
        tree.rename_one("afile", "adir/afile")
        items = self.commit_tree_items(tree, "revb")
        self.assertEquals({
            "": ("therootid", b"reva", []),
            "adir": ("thedirid", b"reva", []),
            "adir/afile": ("thefileid", "revb", [b"reva"]),
            }, items)

    def test_rename_unmodified(self):
        tree = self.prepare_wt('.')
        tree.lock_write()
        self.addCleanup(tree.unlock)
        tree.set_root_id("therootid")
        self.build_tree_contents([
            ('afile', 'contents'),
            ('adir/', )])
        tree.add(["afile", "adir"],
                 ['thefileid', "thedirid"])
        self.commit_tree(tree, b"reva")
        tree.rename_one("afile", "bfile")
        tree.rename_one("adir", "bdir")
        items = self.commit_tree_items(tree, "revb")
        self.assertEquals({
            "": ("therootid", b"reva", []),
            "bfile": ("thefileid", "revb", [b"reva"]),
            "bdir": ("thedirid", "revb", [b"reva"]),
            }, items)

    def test_change_mode(self):
        tree = self.prepare_wt('.')
        tree.lock_write()
        self.addCleanup(tree.unlock)
        tree.set_root_id("therootid")
        self.build_tree_contents([
            ('afile', 'contents')])
        tree.add(["afile"], ['thefileid'])
        self.commit_tree(tree, b"reva")
        os.chmod("afile", 0o755)
        items = self.commit_tree_items(tree, "revb")
        self.assertEquals({
            "": ("therootid", b"reva", []),
            "afile": ("thefileid", "revb", [b"reva"]),
            }, items)

    def test_rename_parent(self):
        tree = self.prepare_wt('.')
        tree.lock_write()
        self.addCleanup(tree.unlock)
        tree.set_root_id("therootid")
        self.build_tree_contents([
            ('adir/', ),
            ('adir/afile', 'contents')])
        tree.add(["adir", "adir/afile"], ['thedirid', 'thefileid'])
        self.commit_tree(tree, b"reva")
        tree.rename_one('adir', 'bdir')
        items = self.commit_tree_items(tree, "revb")
        self.assertEquals({
            "": ("therootid", b"reva", []),
            "bdir": ("thedirid", "revb", [b"reva"]),
            "bdir/afile": ("thefileid", b"reva", []),
            }, items)
        self.assertOverrideTextRevisions(tree, "revb", {
            "bdir/afile": b"reva"})
        self.assertOverrideFileIds(tree, "revb", {
            "bdir/afile": "thefileid",
            "bdir": "thedirid"})

    def test_new_child(self):
        tree = self.prepare_wt('.')
        tree.lock_write()
        self.addCleanup(tree.unlock)
        tree.set_root_id("therootid")
        self.build_tree_contents([
            ('adir/', )])
        tree.add(["adir"], ['thedirid'])
        self.commit_tree(tree, b"reva")
        self.assertOverrideTextRevisions(tree, b"reva", {})
        self.assertOverrideFileIds(tree, b"reva", {
            "": "therootid",
            "adir": "thedirid"})
        self.build_tree_contents([
            ('adir/afile', 'contents')])
        tree.add(['adir/afile'], ['thefileid'])
        items = self.commit_tree_items(tree, "revb")
        self.assertOverrideFileIds(tree, "revb", {
            "adir/afile": "thefileid"})
        self.assertEquals({
            "": ("therootid", b"reva", []),
            "adir": ("thedirid", b"reva", []),
            "adir/afile": ("thefileid", "revb", []),
            }, items)
        self.assertOverrideTextRevisions(tree, "revb", {})
        self.assertOverrideFileIds(tree, "revb", {
            "adir/afile": "thefileid"})

    def test_merge(self):
        # reva
        #  |  \
        # revb revc
        #  |   /
        # revd
        tree = self.prepare_wt('main')
        tree.lock_write()
        self.addCleanup(tree.unlock)
        tree.set_root_id("therootid")
        self.build_tree_contents([
            ('main/afile', 'contents'),
            ('main/bfile', 'contents'),
            ('main/cfile', 'contents')])
        tree.add(["afile", "bfile", "cfile"],
                 ['thefileid', 'bfileid', 'cfileid'])
        self.commit_tree(tree, b"reva")

        os.mkdir('feature')
        other_dir = tree.controldir.sprout('feature')

        self.build_tree_contents([
            ('main/bfile', 'contents-a'),
            ('main/cfile', 'contents-a')])
        self.commit_tree(tree, "revb")

        other_tree = other_dir.open_workingtree()

        self.build_tree_contents([
            ('feature/afile', 'contents-2'),
            ('feature/bfile', 'contents-b'),
            ('feature/cfile', 'contents-b')])
        self.commit_tree(other_tree, "revc")

        conflicts = other_tree.merge_from_branch(tree.branch)
        self.assertEquals(2, conflicts)
        bconflict = other_tree.conflicts()[0]
        self.assertEquals(bconflict.path, "bfile")
        cconflict = other_tree.conflicts()[1]
        self.assertEquals(cconflict.path, "cfile")
        self.build_tree_contents({"feature/bfile": "contents-resolved"})
        bconflict._do("done", other_tree)
        bconflict.cleanup(other_tree)
        cconflict._do("take_other", other_tree)
        cconflict.cleanup(other_tree)
        other_tree.set_conflicts(ConflictList())
        self.assertEquals(0, len(other_tree.conflicts()))
        self.commit_tree(other_tree, "merge")
        tree.branch.set_append_revisions_only(False)
        tree.pull(other_tree.branch)
        items = self.tree_items(tree.revision_tree("merge"))
        self.assertEquals({
            "": ("therootid", b"reva", []),
            "afile": ("thefileid", "revc", [b"reva"]),
            "bfile": ("bfileid", "merge", ["revc", "revb"]),
            "cfile": ("cfileid", "merge", ["revc", "revb"]),
            }, items)
        self.assertOverrideTextRevisions(tree, "merge", {
            })

    def test_pointless(self):
        tree = self.prepare_wt('.')
        tree.lock_write()
        self.addCleanup(tree.unlock)
        tree.set_root_id("THEROOTID")
        self.build_tree_contents([('afile', 'contents')])
        tree.add(["afile"], ['THEFILEID'])
        items = self.commit_tree_items(tree, b"reva")
        self.assertEquals({
            "": ("THEROOTID", b"reva", []),
            "afile": ("THEFILEID", b"reva", []),
            }, items)
        items = self.commit_tree_items(tree, "revb")
        self.assertEquals({
            "": ("THEROOTID", b"reva", []),
            "afile": ("THEFILEID", b"reva", []),
            }, items)


class BzrCommitIdTesting(TestCaseWithTransport,CommitIdTesting):
    """Test ids from a revision tree after committing to bzr."""

    def prepare_wt(self, path):
        tree = self.make_branch_and_tree(path)
        tree.commit("Add root")
        return tree

    def assertOverrideTextRevisions(self, tree, revid, expected):
        pass

    def assertOverrideFileIds(self, tree, revid, expected):
        pass


class SvnRevisionTreeCommitIdTesting(SubversionTestCase,CommitIdTesting):
    """Test ids from a svn revision tree after committing to svn."""

    def setUp(self):
        SubversionTestCase.setUp(self)
        os.mkdir("repo")

    build_tree_contents = staticmethod(treeshape.build_tree_contents)

    def prepare_wt(self, path):
        repo_url = self.make_svn_repository(os.path.join("repo", path))
        dc = self.get_commit_editor(repo_url)
        trunk = dc.add_dir("trunk")
        dc.close()

        self.make_checkout(repo_url+"/trunk", path)
        return WorkingTree.open(path)

    def assertOverrideTextRevisions(self, tree, revid, expected):
        (revmeta, mapping) = tree.branch.repository._get_revmeta(revid)
        self.assertEquals(expected, revmeta.get_text_revisions(mapping))

    def assertOverrideFileIds(self, tree, revid, expected):
        (revmeta, mapping) = tree.branch.repository._get_revmeta(revid)
        self.assertEquals(expected, revmeta.get_fileid_overrides(mapping))


class SvnFetchCommitIdTesting(SubversionTestCase,CommitIdTesting):
    """Test ids from a bzr revision tree after committing to svn and fetching
    back into bzr."""

    def setUp(self):
        SubversionTestCase.setUp(self)
        os.mkdir("repo")

    build_tree_contents = staticmethod(treeshape.build_tree_contents)

    def commit_tree(self, tree, revision_id=None):
        revid = tree.commit("This is a message", rev_id=revision_id)
        tempdir = osutils.mkdtemp(prefix='testbzrsvn-', suffix='.tmp', dir=self.test_dir)
        self.addCleanup(shutil.rmtree, tempdir)
        to_branch = ControlDir.create_branch_and_repo(os.path.join(tempdir, 'branch'))
        tree.branch.push(to_branch)
        return to_branch.basis_tree()

    def prepare_wt(self, path):
        repo_url = self.make_svn_repository(os.path.join("repo", path))
        dc = self.get_commit_editor(repo_url)
        trunk = dc.add_dir("trunk")
        dc.close()

        self.make_checkout(repo_url+"/trunk", path)
        return WorkingTree.open(path)

    def assertOverrideTextRevisions(self, tree, revid, expected):
        (revmeta, mapping) = tree.branch.repository._get_revmeta(revid)
        self.assertEquals(expected, revmeta.get_text_revisions(mapping))

    def assertOverrideFileIds(self, tree, revid, expected):
        (revmeta, mapping) = tree.branch.repository._get_revmeta(revid)
        self.assertEquals(expected, revmeta.get_fileid_overrides(mapping))
