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

"""Branch tests."""

import os
import subvertpy

from bzrlib import urlutils
from bzrlib.branch import (
    Branch,
    InterBranch,
    )
from bzrlib.bzrdir import (
    BzrDir,
    )
from bzrlib.errors import (
    NoSuchRevision,
    NoSuchTag,
    NotBranchError,
    TagsNotSupported,
    )
from bzrlib.repository import Repository
from bzrlib.revision import NULL_REVISION
from bzrlib.tests import (
    TestCase,
    TestSkipped,
    )
from bzrlib.trace import mutter

from bzrlib.plugins.svn.branch import (
    SvnBranchFormat,
    )
from bzrlib.plugins.svn.convert import load_dumpfile
from bzrlib.plugins.svn.tests import SubversionTestCase


class WorkingSubversionBranch(SubversionTestCase):

    def test_last_rev_rev_hist(self):
        repos_url = self.make_repository("a")
        branch = Branch.open(repos_url)
        branch.revision_history()
        self.assertEqual(branch.generate_revision_id(0),
                         branch.last_revision())

    def test_get_branch_path_root(self):
        repos_url = self.make_repository("a")
        branch = Branch.open(repos_url)
        self.assertEqual("", branch.get_branch_path())

    def test_get_child_submit_format_default(self):
        repos_url = self.make_repository("a")
        branch = Branch.open(repos_url)
        self.assertEquals("svn", branch.get_child_submit_format())

    def test_tags_dict(self):
        repos_url = self.make_repository("a")

        dc = self.get_commit_editor(repos_url)
        tags = dc.add_dir("tags")
        tags.add_dir("tags/foo")
        dc.add_dir("trunk")
        dc.close()

        b = Branch.open(repos_url + "/trunk")
        self.assertEquals(["foo"], b.tags.get_tag_dict().keys())

    def test_reverse_tags_dict(self):
        repos_url = self.make_repository("a")

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("trunk")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        tags = dc.add_dir("tags")
        tags.add_dir("tags/foo", "trunk")
        dc.close()

        b = Branch.open(repos_url + "/trunk")
        revid = b.repository.generate_revision_id(1, "trunk",
            b.repository.get_mapping())

        revtagdict = b.tags.get_reverse_tag_dict()
        self.assertEquals([revid], revtagdict.keys())
        self.assertEquals(["foo"], revtagdict[revid])
        self.assertEquals([(revid, ["foo"])], revtagdict.items())

    def test_tags_other_project(self):
        repos_url = self.make_repository("a")

        dc = self.get_commit_editor(repos_url)
        other = dc.add_dir("otherproj")
        other.add_dir("otherproj/trunk")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        my = dc.add_dir("myproj")
        trunk = my.add_dir("myproj/trunk")
        tags = my.add_dir("myproj/tags")
        tags.add_dir("myproj/tags/foo", "otherproj/trunk")
        dc.close()

        b = Branch.open(repos_url + "/myproj/trunk")
        self.assertEquals(["foo"], b.tags.get_tag_dict().keys())

    def test_tag_set(self):
        repos_url = self.make_repository('a')

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("trunk")
        dc.add_dir("tags")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        trunk = dc.open_dir("trunk")
        trunk.add_file("trunk/bla").modify()
        dc.close()

        b = Branch.open(repos_url + "/trunk")
        b.tags.set_tag(u"mytag",
            b.repository.generate_revision_id(1, "trunk", b.repository.get_mapping()))

        self.assertEquals(subvertpy.NODE_DIR,
                b.repository.transport.check_path("tags/mytag", 3))

    def test_tag_set_dupe(self):
        repos_url = self.make_repository('a')

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("trunk")
        dc.add_dir("tags")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        trunk = dc.open_dir("trunk")
        trunk.add_file("trunk/bla").modify()
        dc.close()

        b = Branch.open(repos_url + "/trunk")
        target = b.repository.generate_revision_id(1, "trunk", b.repository.get_mapping())
        b.tags.set_tag(u"mytag", b.repository.generate_revision_id(1, "trunk", b.repository.get_mapping()))

        self.assertEquals(subvertpy.NODE_DIR,
                b.repository.transport.check_path("tags/mytag", 3))
        self.assertEquals(3, b.repository.get_latest_revnum())

        b.tags.set_tag(u"mytag", target)

        self.assertEquals(3, b.repository.get_latest_revnum())
        b = Branch.open(repos_url + "/trunk")
        self.assertEquals({u"mytag": target}, b.tags.get_tag_dict())

    def test_tag_set_existing(self):
        repos_url = self.make_repository('a')

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("trunk")
        dc.add_dir("tags")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        trunk = dc.open_dir("trunk")
        trunk.add_file("trunk/bla").modify()
        dc.close()

        b = Branch.open(repos_url + "/trunk")
        b.tags.set_tag(u"mytag", b.repository.generate_revision_id(1, "trunk", b.repository.get_mapping()))

        self.assertEquals(subvertpy.NODE_DIR,
                b.repository.transport.check_path("tags/mytag", 3))
        self.assertEquals(3, b.repository.get_latest_revnum())

        oldtagrevid = b.repository.generate_revision_id(1, "trunk", b.repository.get_mapping())
        b = Branch.open(repos_url + "/trunk")
        self.assertEquals({u"mytag": oldtagrevid}, b.tags.get_tag_dict())

        newtagrevid = b.repository.generate_revision_id(2, "trunk", b.repository.get_mapping())
        b.tags.set_tag(u"mytag", newtagrevid)

        self.assertEquals(subvertpy.NODE_DIR,
                b.repository.transport.check_path("tags/mytag", 4))
        self.assertEquals(4, b.repository.get_latest_revnum())
        b = Branch.open(repos_url + "/trunk")
        log = self.client_log(repos_url, 4, 0)
        self.assertEquals(log[0][0], None)
        self.assertEquals(log[1][0], {'/tags': ('A', None, -1),
                                      '/trunk': ('A', None, -1)})
        self.assertEquals(log[2][0], {'/trunk/bla': ('A', None, -1)})
        self.assertEquals(log[3][0], {'/tags/mytag': ('A', '/trunk', 1)})
        self.assertEquals(log[4][0], {'/tags/mytag': ('R', '/trunk', 2)})

        self.assertEquals({u"mytag": newtagrevid}, b.tags.get_tag_dict())

    def test_tags_delete(self):
        repos_url = self.make_repository("a")

        dc = self.get_commit_editor(repos_url)
        tags = dc.add_dir("tags")
        tags.add_dir("tags/foo")
        dc.add_dir("trunk")
        dc.close()

        b = Branch.open(repos_url + "/trunk")
        self.assertEquals(["foo"], b.tags.get_tag_dict().keys())
        b.tags.delete_tag(u"foo")
        b = Branch.open(repos_url + "/trunk")
        self.assertEquals([], b.tags.get_tag_dict().keys())

    def test_tag_set_no_parent_dir(self):
        repos_url = self.make_repository('a')

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("trunk")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        trunk = dc.open_dir("trunk")
        trunk.add_file("trunk/bla").modify()
        dc.close()

        b = Branch.open(repos_url + "/trunk")
        b.tags.set_tag(u"mytag",
            b.repository.generate_revision_id(1, "trunk",
                b.repository.get_mapping()))

        self.assertEquals(subvertpy.NODE_DIR,
                b.repository.transport.check_path("tags", 3))

        self.assertEquals(subvertpy.NODE_DIR,
                b.repository.transport.check_path("tags/mytag", 4))
        self.assertEquals(4, b.repository.get_latest_revnum())

    def test_tag_set_not_supported(self):
        repos_url = self.make_repository('a')

        dc = self.get_commit_editor(repos_url)
        trunk = dc.add_dir("trunk")
        trunk.add_dir("trunk/gui")
        dc.close()

        b = Branch.open(repos_url + "/trunk/gui")
        self.assertRaises(TagsNotSupported,
            b.tags.set_tag, u"mytag",
            b.repository.generate_revision_id(1, "trunk/gui",
                b.repository.get_mapping()))

    def test_tag_lookup(self):
        repos_url = self.make_repository("a")

        dc = self.get_commit_editor(repos_url)
        tags = dc.add_dir("tags")
        tags.add_dir("tags/foo")
        dc.add_dir("trunk")
        dc.close()

        b = Branch.open(repos_url + "/trunk")
        self.assertEquals("", b.project)
        self.assertEquals(
            b.repository.generate_revision_id(1, "tags/foo",
                b.repository.get_mapping()),
            b.tags.lookup_tag("foo"))

    def test_tag_lookup_nonexistant(self):
        repos_url = self.make_repository("a")

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("trunk")
        dc.close()

        b = Branch.open(repos_url + "/trunk")
        self.assertRaises(NoSuchTag, b.tags.lookup_tag, "foo")

    def test_tags_delete_nonexistent(self):
        repos_url = self.make_repository("a")

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("trunk")
        dc.close()

        b = Branch.open(repos_url + "/trunk")
        self.assertRaises(NoSuchTag, b.tags.delete_tag, u"foo")

    def test_get_branch_path_old(self):
        repos_url = self.make_repository("a")

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("trunk")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("trunk2", "trunk", 1)
        dc.close()

        branch = Branch.open(urlutils.join(repos_url, "trunk2"))
        self.assertEqual("trunk2", branch.get_branch_path(2))
        self.assertEqual("trunk", branch.get_branch_path(1))

    def test_pull_internal(self):
        repos_url = self.make_repository("a")

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("trunk")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        branches = dc.add_dir("branches")
        branches.add_dir("branches/foo", "trunk", 1)
        dc.close()

        otherbranch = Branch.open(urlutils.join(repos_url, "branches", "foo"))
        branch = Branch.open(urlutils.join(repos_url, "trunk"))
        result = branch.pull(otherbranch)
        self.assertEquals(branch.last_revision(), otherbranch.last_revision())
        self.assertEquals(result.new_revid, otherbranch.last_revision())
        self.assertEquals(result.old_revid, branch.revision_history()[0])
        self.assertEquals(result.old_revno, 1)
        self.assertEquals(result.new_revno, 2)
        self.assertEquals(result.master_branch, None)
        self.assertEquals(result.source_branch, otherbranch)
        self.assertEquals(result.target_branch, branch)

    def make_tworev_branch(self):
        repos_url = self.make_repository("a")

        dc = self.get_commit_editor(repos_url)
        dc.add_file('foo')
        dc.close()

        b = Branch.open(repos_url)
        mapping = b.repository.get_mapping()
        uuid = b.repository.uuid
        revid1 = mapping.revision_id_foreign_to_bzr((uuid, '', 0))
        revid2 = mapping.revision_id_foreign_to_bzr((uuid, '', 1))
        return b, (revid1, revid2)

    def make_branch(self, relpath):
        # The inherited make_branch is broken, thanks to the make_repository
        # from subvertpy.
        bzrdir = self.make_bzrdir(relpath)
        bzrdir._find_or_create_repository(True)
        return bzrdir.create_branch()

    def test_interbranch_pull(self):
        svn_branch, (revid1, revid2) = self.make_tworev_branch()
        new_branch = self.make_branch("b")
        inter_branch = InterBranch.get(svn_branch, new_branch)
        inter_branch.pull()
        self.assertEquals(revid2, new_branch.last_revision())

    def test_interbranch_pull_noop(self):
        svn_branch, (revid1, revid2) = self.make_tworev_branch()
        new_branch = self.make_branch("b")
        inter_branch = InterBranch.get(svn_branch, new_branch)
        inter_branch.pull()
        # This is basically "assertNotRaises"
        inter_branch.pull()
        self.assertEquals(revid2, new_branch.last_revision())

    def test_interbranch_pull_stop_revision(self):
        svn_branch, (revid1, revid2) = self.make_tworev_branch()
        new_branch = self.make_branch("b")
        inter_branch = InterBranch.get(svn_branch, new_branch)
        inter_branch.pull(stop_revision=revid1)
        self.assertEquals(revid1, new_branch.last_revision())

    def test_get_branch_path_subdir(self):
        repos_url = self.make_repository("a")

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("trunk")
        dc.close()

        branch = Branch.open(repos_url+"/trunk")
        self.assertEqual("trunk", branch.get_branch_path())

    def test_tag_added_later(self):
        repos_url = self.make_repository("a")

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("trunk")
        dc.close()

        olddir = BzrDir.open(repos_url+"/trunk")
        oldbranch = olddir.open_branch()
        newdir = olddir.sprout("mycopy")
        newbranch = newdir.open_branch()
        self.assertEquals({}, newbranch.tags.get_tag_dict())

        dc = self.get_commit_editor(repos_url)
        tags = dc.add_dir("tags")
        new_tag = tags.add_dir("tags/newtag", "trunk")
        dc.close()

        self.assertEquals(1, oldbranch.get_revnum())
        newbranch.pull(oldbranch, stop_revision=oldbranch.last_revision())

        self.assertEquals(["newtag"], newbranch.tags.get_tag_dict().keys())

    def test_open_non_ascii_url(self):
        repos_url = self.make_repository("a")

        dc = self.get_commit_editor(repos_url)
        dc.add_dir('\xd0\xb4\xd0\xbe\xd0\xbc')
        dc.close()

        branch = Branch.open(repos_url + "/%D0%B4%D0%BE%D0%BC")

    def test_open_nonexistant(self):
        repos_url = self.make_repository("a")
        self.assertRaises(NotBranchError, Branch.open, repos_url + "/trunk")

    def test_last_rev_rev_info(self):
        repos_url = self.make_repository("a")
        branch = Branch.open(repos_url)
        self.assertEqual((1, branch.generate_revision_id(0)),
                branch.last_revision_info())
        branch.revision_history()
        self.assertEqual((1, branch.generate_revision_id(0)),
                branch.last_revision_info())

    def test_lookup_revision_id_unknown(self):
        repos_url = self.make_repository("a")
        branch = Branch.open(repos_url)
        self.assertRaises(NoSuchRevision, lambda: branch.lookup_bzr_revision_id("bla"))

    def test_lookup_revision_id(self):
        repos_url = self.make_repository("a")
        branch = Branch.open(repos_url)
        self.assertEquals(0, branch.lookup_bzr_revision_id(branch.last_revision()))

    def test_set_parent(self):
        repos_url = self.make_repository('a')
        branch = Branch.open(repos_url)
        branch.set_parent("foobar")

    def test_num_revnums(self):
        repos_url = self.make_repository('a')
        bzrdir = BzrDir.open(repos_url)
        branch = bzrdir.open_branch()
        self.assertEqual(branch.generate_revision_id(0), branch.last_revision())

        dc = self.get_commit_editor(repos_url)
        dc.add_file("foo").modify()
        dc.close()

        bzrdir = BzrDir.open(repos_url)
        branch = bzrdir.open_branch()
        repos = bzrdir.find_repository()

        mapping = repos.get_mapping()

        self.assertEqual(repos.generate_revision_id(1, "", mapping),
                branch.last_revision())

        dc = self.get_commit_editor(repos_url)
        dc.open_file("foo").modify()
        dc.close()

        branch = Branch.open(repos_url)
        repos = Repository.open(repos_url)

        self.assertEqual(repos.generate_revision_id(2, "", mapping),
                branch.last_revision())

    def test_set_revision_history_empty(self):
        repos_url = self.make_repository('a')
        branch = Branch.open(repos_url)
        self.assertRaises(NotImplementedError, branch.set_revision_history, [])

    def test_set_revision_history_ghost(self):
        repos_url = self.make_repository('a')

        dc = self.get_commit_editor(repos_url)
        trunk = dc.add_dir("trunk")
        trunk.add_file('trunk/foo').modify()
        dc.close()

        branch = Branch.open(repos_url+"/trunk")
        self.assertRaises(NotImplementedError,
            branch.set_revision_history, ["nonexistantt"])

    def test_set_revision_history(self):
        repos_url = self.make_repository('a')

        dc = self.get_commit_editor(repos_url)
        trunk = dc.add_dir("trunk")
        trunk.add_file('trunk/foo').modify()
        dc.close()

        dc = self.get_commit_editor(repos_url)
        trunk = dc.open_dir("trunk")
        trunk.add_file('trunk/bla').modify()
        dc.close()

        dc = self.get_commit_editor(repos_url)
        trunk = dc.open_dir("trunk")
        trunk.add_file('trunk/bar').modify()
        dc.close()

        branch = Branch.open(repos_url+"/trunk")
        orig_history = branch.revision_history()
        branch.get_config().set_user_option('append_revisions_only', 'False')
        branch.set_revision_history(orig_history[:-1])
        self.assertEquals(orig_history[:-1], branch.revision_history())

    def test_break_lock(self):
        # duplicated by bzrlib.tests.per_branch.test_break_lock
        repos_url = self.make_repository('a')
        branch = Branch.open(repos_url)
        branch.break_lock()

    def test_repr(self):
        repos_url = self.make_repository('a')
        branch = Branch.open(repos_url)
        self.assertEqual("SvnBranch('%s')" % repos_url, branch.__repr__())

    def test_get_physical_lock_status(self):
        repos_url = self.make_repository('a')
        branch = Branch.open(repos_url)
        self.assertFalse(branch.get_physical_lock_status())

    def test_set_push_location(self):
        # duplicated by bt.per_branch.TestBranchPushLocations.test_set_push_location
        repos_url = self.make_repository('a')
        branch = Branch.open(repos_url)
        branch.set_push_location("http://bar/bloe")

    def test_get_parent(self):
        repos_url = self.make_repository('a')
        branch = Branch.open(repos_url)
        self.assertEqual(None, branch.get_parent())

    def test_get_push_location(self):
        # duplicated by bt.per_branch.TestBranchPushLocations.test_get_push_location_unset
        repos_url = self.make_repository('a')
        branch = Branch.open(repos_url)
        self.assertIs(None, branch.get_push_location())

    def test_revision_id_to_revno_none(self):
        """The None revid should map to revno 0."""
        # duplicated by bt.per_branch.test_revision_id_to_revno
        repos_url = self.make_repository('a')
        branch = Branch.open(repos_url)
        self.assertEquals(0, branch.revision_id_to_revno(NULL_REVISION))

    def test_revision_id_to_revno_nonexistant(self):
        """revision_id_to_revno() should raise NoSuchRevision if
        the specified revision did not exist in the branch history."""
        # duplicated by bt.per_branch.test_revision_id_to_revno
        repos_url = self.make_repository('a')
        branch = Branch.open(repos_url)
        self.assertRaises(NoSuchRevision, branch.revision_id_to_revno, "bla")

    def test_get_nick_none(self):
        repos_url = self.make_repository('a')

        dc = self.get_commit_editor(repos_url)
        dc.add_file("foo").modify()
        dc.close()

        branch = Branch.open(repos_url)

        self.assertEquals("a", branch.nick)

    def test_get_nick_path(self):
        repos_url = self.make_repository('a')

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("trunk")
        dc.close()

        branch = Branch.open(repos_url+"/trunk")

        self.assertEqual("trunk", branch.nick)

    def test_get_revprops(self):
        repos_url = self.make_repository('a')

        dc = self.get_commit_editor(repos_url)
        dc.add_file("foo").modify()
        dc.change_prop("bzr:revision-info",
                "properties: \n\tbranch-nick: mybranch\n")
        dc.close()

        branch = Branch.open(repos_url)

        rev = branch.repository.get_revision(branch.last_revision())

        self.assertEqual("mybranch", rev.properties["branch-nick"])

    def test_fetch_replace(self):
        filename = os.path.join(self.test_dir, "dumpfile")
        open(filename, 'w').write("""SVN-fs-dump-format-version: 2

UUID: 6f95bc5c-e18d-4021-aca8-49ed51dbcb75

Revision-number: 0
Prop-content-length: 56
Content-length: 56

K 8
svn:date
V 27
2006-07-30T12:41:25.270824Z
PROPS-END

Revision-number: 1
Prop-content-length: 94
Content-length: 94

K 7
svn:log
V 0

K 10
svn:author
V 0

K 8
svn:date
V 27
2006-07-30T12:41:26.117512Z
PROPS-END

Node-path: trunk
Node-kind: dir
Node-action: add
Prop-content-length: 10
Content-length: 10

PROPS-END


Node-path: trunk/hosts
Node-kind: file
Node-action: add
Prop-content-length: 10
Text-content-length: 4
Text-content-md5: 771ec3328c29d17af5aacf7f895dd885
Content-length: 14

PROPS-END
hej1

Revision-number: 2
Prop-content-length: 94
Content-length: 94

K 7
svn:log
V 0

K 10
svn:author
V 0

K 8
svn:date
V 27
2006-07-30T12:41:27.130044Z
PROPS-END

Node-path: trunk/hosts
Node-kind: file
Node-action: change
Text-content-length: 4
Text-content-md5: 6c2479dbb342b8df96d84db7ab92c412
Content-length: 4

hej2

Revision-number: 3
Prop-content-length: 94
Content-length: 94

K 7
svn:log
V 0

K 10
svn:author
V 0

K 8
svn:date
V 27
2006-07-30T12:41:28.114350Z
PROPS-END

Node-path: trunk/hosts
Node-kind: file
Node-action: change
Text-content-length: 4
Text-content-md5: 368cb8d3db6186e2e83d9434f165c525
Content-length: 4

hej3

Revision-number: 4
Prop-content-length: 94
Content-length: 94

K 7
svn:log
V 0

K 10
svn:author
V 0

K 8
svn:date
V 27
2006-07-30T12:41:29.129563Z
PROPS-END

Node-path: branches
Node-kind: dir
Node-action: add
Prop-content-length: 10
Content-length: 10

PROPS-END


Revision-number: 5
Prop-content-length: 94
Content-length: 94

K 7
svn:log
V 0

K 10
svn:author
V 0

K 8
svn:date
V 27
2006-07-30T12:41:31.130508Z
PROPS-END

Node-path: branches/foobranch
Node-kind: dir
Node-action: add
Node-copyfrom-rev: 4
Node-copyfrom-path: trunk


Revision-number: 6
Prop-content-length: 94
Content-length: 94

K 7
svn:log
V 0

K 10
svn:author
V 0

K 8
svn:date
V 27
2006-07-30T12:41:33.129149Z
PROPS-END

Node-path: branches/foobranch/hosts
Node-kind: file
Node-action: delete

Node-path: branches/foobranch/hosts
Node-kind: file
Node-action: add
Node-copyfrom-rev: 2
Node-copyfrom-path: trunk/hosts




Revision-number: 7
Prop-content-length: 94
Content-length: 94

K 7
svn:log
V 0

K 10
svn:author
V 0

K 8
svn:date
V 27
2006-07-30T12:41:34.136423Z
PROPS-END

Node-path: branches/foobranch/hosts
Node-kind: file
Node-action: change
Text-content-length: 8
Text-content-md5: 0e328d3517a333a4879ebf3d88fd82bb
Content-length: 8

foohosts""")
        os.mkdir("new")
        os.mkdir("old")

        load_dumpfile("dumpfile", "old")

        url = "old/branches/foobranch"
        mutter('open %r' % url)
        olddir = BzrDir.open(url)

        newdir = olddir.sprout("new")

        newbranch = newdir.open_branch()

        oldbranch = Branch.open(url)

        uuid = "6f95bc5c-e18d-4021-aca8-49ed51dbcb75"
        newbranch.lock_read()
        tree = newbranch.repository.revision_tree(oldbranch.generate_revision_id(7))

        host_fileid = tree.path2id("hosts")

        self.assertVersionsPresentEquals(newbranch.repository.texts,
                                        host_fileid, [
            oldbranch.generate_revision_id(6),
            oldbranch.generate_revision_id(7)])
        newbranch.unlock()

    def test_fetch_odd(self):
        repos_url = self.make_repository('d')

        dc = self.get_commit_editor(repos_url)
        trunk = dc.add_dir("trunk")
        trunk.add_file("trunk/hosts").modify()
        dc.close()

        dc = self.get_commit_editor(repos_url)
        trunk = dc.open_dir("trunk")
        trunk.open_file("trunk/hosts").modify()
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.open_file("trunk/hosts").modify()
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("branches")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        branches = dc.open_dir("branches")
        branches.add_dir("branches/foobranch", "trunk")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        branches = dc.open_dir("branches")
        foobranch = branches.open_dir("branches/foobranch")
        foobranch.open_file("branches/foobranch/hosts").modify()
        dc.close()

        os.mkdir("new")

        url = repos_url+"/branches/foobranch"
        mutter('open %r' % url)
        olddir = BzrDir.open(url)

        newdir = olddir.sprout("new")

        newbranch = newdir.open_branch()
        oldbranch = olddir.open_branch()

        uuid = olddir.find_repository().uuid
        tree = newbranch.repository.revision_tree(
             oldbranch.generate_revision_id(6))
        transaction = newbranch.repository.get_transaction()
        newbranch.repository.lock_read()
        texts = newbranch.repository.texts
        host_fileid = tree.path2id("hosts")
        mapping = oldbranch.repository.get_mapping()
        self.assertVersionsPresentEquals(texts, host_fileid, [
            mapping.revision_id_foreign_to_bzr((uuid, "trunk", 1)),
            mapping.revision_id_foreign_to_bzr((uuid, "trunk", 2)),
            mapping.revision_id_foreign_to_bzr((uuid, "trunk", 3)),
            oldbranch.generate_revision_id(6)])
        newbranch.repository.unlock()

    def assertVersionsPresentEquals(self, texts, fileid, versions):
        self.assertEqual(set([(fileid, v) for v in versions]),
            set(filter(lambda (fid, rid): fid == fileid, texts.keys())))

    def test_check(self):
        self.make_repository('d')
        branch = Branch.open('d')
        result = branch.check()
        self.assertEqual(branch, result.branch)

    def test_generate_revision_id(self):
        repos_url = self.make_repository('d')

        dc = self.get_commit_editor(repos_url)
        bla = dc.add_dir("bla")
        bla.add_dir("bla/bloe")
        dc.close()

        branch = Branch.open('d')
        mapping = branch.repository.get_mapping()
        self.assertEqual(mapping.revision_id_foreign_to_bzr((branch.repository.uuid, "", 1)), branch.generate_revision_id(1))

    def test_create_checkout(self):
        repos_url = self.make_repository('d')

        dc = self.get_commit_editor(repos_url)
        trunk = dc.add_dir("trunk")
        trunk.add_file("trunk/hosts").modify()
        dc.close()

        url = repos_url+"/trunk"
        oldbranch = Branch.open(url)

        newtree = oldbranch.create_checkout("e")
        self.assertTrue(newtree.branch.repository.has_revision(
           oldbranch.generate_revision_id(1)))

        self.assertTrue(os.path.exists("e/.bzr"))
        self.assertFalse(os.path.exists("e/.svn"))

    def test_create_checkout_lightweight(self):
        repos_url = self.make_repository('d')

        dc = self.get_commit_editor(repos_url)
        trunk = dc.add_dir("trunk")
        trunk.add_file("trunk/hosts")
        dc.close()

        oldbranch = Branch.open(repos_url+"/trunk")
        newtree = oldbranch.create_checkout("e", lightweight=True)
        self.assertEqual(oldbranch.generate_revision_id(1), newtree.last_revision())
        self.assertTrue(os.path.exists("e/.svn"))
        self.assertFalse(os.path.exists("e/.bzr"))

    def test_create_checkout_lightweight_stop_rev(self):
        repos_url = self.make_repository('d')

        dc = self.get_commit_editor(repos_url)
        trunk = dc.add_dir("trunk")
        trunk.add_file("trunk/hosts").modify()
        dc.close()

        dc = self.get_commit_editor(repos_url)
        trunk = dc.open_dir("trunk")
        trunk.open_file("trunk/hosts").modify()
        dc.close()

        url = repos_url+"/trunk"
        oldbranch = Branch.open(url)

        newtree = oldbranch.create_checkout("e", revision_id=
           oldbranch.generate_revision_id(1), lightweight=True)
        self.assertEqual(oldbranch.generate_revision_id(1),
           newtree.last_revision())
        self.assertTrue(os.path.exists("e/.svn"))
        self.assertFalse(os.path.exists("e/.bzr"))

    def test_fetch_branch(self):
        repos_url = self.make_client('d', 'sc')

        sc = self.get_commit_editor(repos_url)
        foo = sc.add_dir("foo")
        foo.add_file("foo/bla").modify()
        sc.close()

        olddir = BzrDir.open("sc")

        os.mkdir("dc")

        newdir = olddir.sprout('dc')

        self.assertEqual(
                olddir.open_branch().last_revision(),
                newdir.open_branch().last_revision())

    def test_fetch_dir_upgrade(self):
        repos_url = self.make_client('d', 'sc')

        sc = self.get_commit_editor(repos_url)
        trunk = sc.add_dir("trunk")
        mylib = trunk.add_dir("trunk/mylib")
        mylib.add_file("trunk/mylib/bla").modify()
        sc.add_dir("branches")
        sc.close()

        sc = self.get_commit_editor(repos_url)
        branches = sc.open_dir("branches")
        branches.add_dir("branches/abranch", "trunk/mylib")
        sc.close()

        self.client_update('sc')
        olddir = BzrDir.open("sc/branches/abranch")

        os.mkdir("dc")

        newdir = olddir.sprout('dc')

        self.assertEqual(
                olddir.open_branch().last_revision(),
                newdir.open_branch().last_revision())

    def test_fetch_branch_downgrade(self):
        repos_url = self.make_client('d', 'sc')

        sc = self.get_commit_editor(repos_url)
        sc.add_dir("trunk")
        branches = sc.add_dir("branches")
        abranch = branches.add_dir("branches/abranch")
        abranch.add_file("branches/abranch/bla").modify()
        sc.close()

        sc = self.get_commit_editor(repos_url)
        trunk = sc.open_dir("trunk")
        sc.add_dir("trunk/mylib", "branches/abranch")
        sc.close()

        self.client_update('sc')
        olddir = BzrDir.open("sc/trunk")

        os.mkdir("dc")

        newdir = olddir.sprout('dc')

        self.assertEqual(
                olddir.open_branch().last_revision(),
                newdir.open_branch().last_revision())

    def test_ghost_workingtree(self):
        # Looks like bazaar has trouble creating a working tree of a
        # revision that has ghost parents
        repos_url = self.make_client('d', 'sc')

        sc = self.get_commit_editor(repos_url)
        foo = sc.add_dir("foo")
        foo.add_file("foo/bla").modify()
        sc.change_prop("bzr:ancestry:v3-none", "some-ghost\n")
        sc.close()

        olddir = BzrDir.open("sc")

        os.mkdir("dc")

        newdir = olddir.sprout('dc')
        newdir.find_repository().get_revision(
                newdir.open_branch().last_revision())
        newdir.find_repository().revision_tree(
                newdir.open_branch().last_revision())


class BranchFormatTests(TestCase):

    def setUp(self):
        super(BranchFormatTests, self).setUp()
        self.format = SvnBranchFormat()

    def test_get_format_string(self):
        self.assertRaises(NotImplementedError, self.format.get_format_string)

    def test_get_format_description(self):
        self.assertEqual("Subversion Smart Server",
                         self.format.get_format_description())


class ForeignTestsBranchFactory(object):

    def make_empty_branch(self, transport):
        raise TestSkipped()

    def make_branch(self, transport):
        subvertpy.repos.create(transport.local_abspath("."))
        return Branch.open(transport.base)


class TestInterBranchFetch(SubversionTestCase):

    def make_branch(self, relpath):
        # The inherited make_branch is broken, thanks to the make_repository
        # from subvertpy.
        bzrdir = self.make_bzrdir(relpath)
        bzrdir._find_or_create_repository(True)
        return bzrdir.create_branch()

    def make_tworev_branch(self):
        self.repos_url = self.make_repository("a")

        dc = self.get_commit_editor(self.repos_url)
        trunk = dc.add_dir("trunk")
        trunk.add_file('trunk/foo')
        dc.close()

        b = Branch.open(self.repos_url+"/trunk")
        mapping = b.repository.get_mapping()
        uuid = b.repository.uuid
        revid1 = mapping.revision_id_foreign_to_bzr((uuid, 'trunk', 1))
        return b, revid1

    def test_fetch_stop_revision(self):
        svn_branch, revid1 = self.make_tworev_branch()
        new_branch = self.make_branch("b")
        inter_branch = InterBranch.get(svn_branch, new_branch)
        inter_branch.fetch(stop_revision=revid1)
        self.assertEquals(set([revid1]), set(new_branch.repository.all_revision_ids()))

    def test_fetch_no_stop_revision(self):
        svn_branch, revid1 = self.make_tworev_branch()
        new_branch = self.make_branch("b")
        inter_branch = InterBranch.get(svn_branch, new_branch)
        inter_branch.fetch()
        self.assertEquals(set([revid1]), set(new_branch.repository.all_revision_ids()))

    def test_fetch_tags(self):
        svn_branch, revid1 = self.make_tworev_branch()

        dc = self.get_commit_editor(self.repos_url)
        tags = dc.add_dir("tags")
        tags.add_dir("tags/foo")
        dc.close()

        mapping = svn_branch.repository.get_mapping()
        uuid = svn_branch.repository.uuid
        revid2 = mapping.revision_id_foreign_to_bzr((uuid, 'tags/foo', 2))

        new_branch = self.make_branch("b")
        inter_branch = InterBranch.get(svn_branch, new_branch)
        inter_branch.fetch(fetch_tags=True)
        self.assertEquals(set([revid1, revid2]), set(new_branch.repository.all_revision_ids()))
