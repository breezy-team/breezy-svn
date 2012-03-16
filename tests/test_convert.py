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

"""Full repository conversion tests."""

import os, sys
from subvertpy import repos

from bzrlib.branch import (
    Branch,
    )
from bzrlib.bzrdir import (
    BzrDir,
    format_registry,
    )
from bzrlib.errors import (
    IncompatibleRepositories,
    NotBranchError,
    NoSuchFile,
    )
from bzrlib.urlutils import (
    local_path_to_url,
    )
from bzrlib.repository import (
    Repository,
    )
from bzrlib.tests import (
    TestCaseInTempDir,
    TestCase,
    )

from bzrlib.plugins.svn.layout.standard import (
    RootLayout,
    TrunkLayout,
    )
from bzrlib.plugins.svn.convert import (
    NotDumpFile,
    contains_parent_path,
    convert_repository,
    load_dumpfile,
    )
from bzrlib.plugins.svn.tests import (
    SubversionTestCase,
    )

class TestLoadDumpfile(TestCaseInTempDir):

    def test_loaddumpfile(self):
        dumpfile = os.path.join(self.test_dir, "dumpfile")
        open(dumpfile, 'w').write(
"""SVN-fs-dump-format-version: 2

UUID: 6987ef2d-cd6b-461f-9991-6f1abef3bd59

Revision-number: 0
Prop-content-length: 56
Content-length: 56

K 8
svn:date
V 27
2006-07-02T13:14:51.972532Z
PROPS-END
""")
        load_dumpfile(dumpfile, os.path.join(self.test_dir, "d"))
        fs = repos.Repository("d").fs()
        self.assertEqual("6987ef2d-cd6b-461f-9991-6f1abef3bd59",
                fs.get_uuid())

    def test_loaddumpfile_invalid(self):
        dumpfile = os.path.join(self.test_dir, "dumpfile")
        open(dumpfile, 'w').write("""FooBar\n""")
        self.assertRaises(NotDumpFile, load_dumpfile, dumpfile,
            os.path.join(self.test_dir, "d"))


class TestConversion(SubversionTestCase):

    def setUp(self):
        super(TestConversion, self).setUp()
        self.repos_url = self.make_svn_repository('d')

        dc = self.get_commit_editor()
        t = dc.add_dir("trunk")
        t.add_file("trunk/file").modify("data")
        bs = dc.add_dir("branches")
        ab = bs.add_dir("branches/abranch")
        ab.add_file("branches/abranch/anotherfile").modify("data2")
        dc.close()

        dc = self.get_commit_editor()
        t = dc.open_dir("trunk")
        t.open_file("trunk/file").modify("otherdata")
        dc.close()

    def get_commit_editor(self):
        return super(TestConversion, self).get_commit_editor(self.repos_url)

    def test_parent_branch_outside_prefix(self):
        dc = self.get_commit_editor()

        foo = dc.add_dir("foo")
        foo.add_dir("foo/trunk")
        dc.close()

        dc = self.get_commit_editor()
        bar = dc.add_dir("bar")
        bar.add_dir("bar/trunk", "foo/trunk")
        dc.close()

        convert_repository(Repository.open(self.repos_url), "e",
                           TrunkLayout(1),
                           all=True, create_shared_repo=True, prefix="bar")

    def test_sets_parent_urls(self):
        convert_repository(Repository.open(self.repos_url), "e",
                           TrunkLayout(0),
                           all=False, create_shared_repo=True)
        self.assertEquals(self.repos_url+"/trunk",
                Branch.open("e/trunk").get_parent())
        self.assertEquals(self.repos_url+"/branches/abranch",
                Branch.open("e/branches/abranch").get_parent())

    def test_fetch_alive(self):
        dc = self.get_commit_editor()
        bs = dc.open_dir("branches")
        sb = bs.add_dir("branches/somebranch")
        sb.add_file("branches/somebranch/somefile").modify('data')
        dc.close()

        dc = self.get_commit_editor()
        bs = dc.open_dir("branches")
        bs.delete("branches/somebranch")
        dc.close()

        oldrepos = Repository.open(self.repos_url)
        convert_repository(oldrepos, "e",
                           TrunkLayout(0),
                           all=False, create_shared_repo=True)
        newrepos = Repository.open("e")
        oldrepos.set_layout(TrunkLayout(0))
        revid = oldrepos.generate_revision_id(2, "branches/somebranch",
            oldrepos.get_mapping())
        self.assertFalse(newrepos.has_revision(revid))

    def test_fetch_filebranch(self):
        dc = self.get_commit_editor()
        bs = dc.open_dir("branches")
        bs.add_file("branches/somebranch").modify('data')
        dc.close()

        oldrepos = Repository.open(self.repos_url)
        convert_repository(oldrepos, "e", TrunkLayout(0))
        newrepos = Repository.open("e")
        oldrepos.set_layout(TrunkLayout(0))
        self.assertFalse(newrepos.has_revision(oldrepos.generate_revision_id(2, "branches/somebranch", oldrepos.get_mapping())))

    def test_fetch_dead(self):
        dc = self.get_commit_editor()
        bs = dc.open_dir("branches")
        sb = bs.add_dir("branches/somebranch")
        sb.add_file("branches/somebranch/somefile").modify('data')
        dc.close()

        dc = self.get_commit_editor()
        bs = dc.open_dir("branches")
        bs.delete("branches/somebranch")
        dc.close()

        oldrepos = Repository.open(self.repos_url)
        convert_repository(oldrepos, "e", TrunkLayout(0),
                           all=True, create_shared_repo=True)
        newrepos = Repository.open("e")
        self.assertTrue(newrepos.has_revision(
            oldrepos.generate_revision_id(3, "branches/somebranch", oldrepos.get_mapping())))

    def test_fetch_filter(self):
        dc = self.get_commit_editor()
        branches = dc.open_dir("branches")
        dc.add_dir("branches/somebranch")
        dc.add_file("branches/somebranch/somefile").modify('data')
        dc.close()

        dc = self.get_commit_editor()
        branches = dc.open_dir("branches")
        ab = branches.add_dir("branches/anotherbranch")
        ab.add_file("branches/anotherbranch/somefile").modify('data')
        dc.close()

        oldrepos = Repository.open(self.repos_url)
        convert_repository(oldrepos, "e", TrunkLayout(0),
            create_shared_repo=True,
            filter_branch=lambda branch: branch.get_branch_path().endswith("somebranch"))
        self.assertTrue(os.path.exists("e/branches/somebranch"))
        self.assertFalse(os.path.exists("e/branches/anotherbranch"))

    def test_shared_import_continue(self):
        dir = BzrDir.create("e")
        dir.create_repository(shared=True)

        convert_repository(Repository.open(self.repos_url), "e",
                TrunkLayout(0), create_shared_repo=True)

        self.assertTrue(Repository.open("e").is_shared())

    def test_shared_import_continue_remove(self):
        convert_repository(Repository.open(self.repos_url), "e",
                TrunkLayout(0), create_shared_repo=True)

        dc = self.get_commit_editor()
        dc.delete("trunk")
        dc.close()

        dc = self.get_commit_editor()
        trunk = dc.add_dir("trunk")
        trunk.add_file("trunk/file").modify()
        dc.close()

        convert_repository(Repository.open(self.repos_url), "e",
                           TrunkLayout(0), create_shared_repo=True)

    def test_shared_import_remove_nokeep(self):
        convert_repository(Repository.open(self.repos_url), "e",
                TrunkLayout(0), create_shared_repo=True)

        dc = self.get_commit_editor()
        dc.delete("trunk")
        dc.close()

        self.assertTrue(os.path.exists("e/trunk"))

        convert_repository(Repository.open(self.repos_url), "e",
                           TrunkLayout(0), create_shared_repo=True)

        self.assertFalse(os.path.exists("e/trunk"))

    def test_shared_import_continue_with_wt(self):
        convert_repository(Repository.open(self.repos_url), "e",
                TrunkLayout(0), working_trees=True)
        convert_repository(Repository.open(self.repos_url), "e",
                TrunkLayout(0), working_trees=True)

    def test_shared_import_rootlayout_empty(self):
        dir = BzrDir.create("e")
        dir.create_repository(shared=True)

        convert_repository(Repository.open(self.repos_url), "e",
                RootLayout(), create_shared_repo=True)

    def test_shared_import_with_wt(self):
        dir = BzrDir.create("e")
        dir.create_repository(shared=True)

        convert_repository(Repository.open(self.repos_url), "e",
                TrunkLayout(0), create_shared_repo=True,
                working_trees=True)

        self.assertTrue(os.path.isfile(os.path.join(
                        self.test_dir, "e", "trunk", "file")))

    def test_shared_import_without_wt(self):
        dir = BzrDir.create("e")
        dir.create_repository(shared=True)

        convert_repository(Repository.open(self.repos_url), "e",
                TrunkLayout(0), create_shared_repo=True,
                working_trees=False)

        self.assertFalse(os.path.isfile(os.path.join(
                        self.test_dir, "e", "trunk", "file")))

    def test_shared_import_old_repos_fails(self):
        dir = BzrDir.create("e", format=format_registry.make_bzrdir('knit'))
        dir.create_repository(shared=True)

        self.assertRaises(IncompatibleRepositories,
            lambda: convert_repository(Repository.open(self.repos_url), "e",
                TrunkLayout(0), create_shared_repo=True,
                working_trees=False))

    def test_shared_import_continue_branch(self):
        oldrepos = Repository.open(self.repos_url)
        convert_repository(oldrepos, "e",
                TrunkLayout(0), create_shared_repo=True)

        mapping = oldrepos.get_mapping()

        dc = self.get_commit_editor()
        trunk = dc.open_dir("trunk")
        trunk.open_file("trunk/file").modify()
        dc.close()

        self.assertEqual(
                Repository.open(self.repos_url).generate_revision_id(2, "trunk", mapping),
                Branch.open("e/trunk").last_revision())

        convert_repository(Repository.open(self.repos_url), "e",
                TrunkLayout(0), create_shared_repo=True)

        self.assertEqual(Repository.open(self.repos_url).generate_revision_id(3, "trunk", mapping),
                        Branch.open("e/trunk").last_revision())


    def test_shared_import(self):
        convert_repository(Repository.open(self.repos_url), "e",
                TrunkLayout(0), create_shared_repo=True)

        self.assertTrue(Repository.open("e").is_shared())

    def test_simple(self):
        convert_repository(Repository.open(self.repos_url), os.path.join(self.test_dir, "e"), TrunkLayout(0))
        self.assertTrue(os.path.isdir(os.path.join(self.test_dir, "e", "trunk")))
        self.assertTrue(os.path.isdir(os.path.join(self.test_dir, "e", "branches", "abranch")))

    def test_convert_to_nonexistant(self):
        self.assertRaises(NoSuchFile, convert_repository, Repository.open(self.repos_url), os.path.join(self.test_dir, "e", "foo", "bar"), TrunkLayout(0))

    def test_notshared_import(self):
        convert_repository(Repository.open(self.repos_url), "e",
                           TrunkLayout(0), create_shared_repo=False)

        self.assertRaises(NotBranchError, Repository.open, "e")


class TestConversionFromDumpfile(SubversionTestCase):

    def test_dumpfile_open_empty(self):
        dumpfile = os.path.join(self.test_dir, "dumpfile")
        open(dumpfile, 'w').write(
"""SVN-fs-dump-format-version: 2

UUID: 6987ef2d-cd6b-461f-9991-6f1abef3bd59

Revision-number: 0
Prop-content-length: 56
Content-length: 56

K 8
svn:date
V 27
2006-07-02T13:14:51.972532Z
PROPS-END
""")
        branch_path = os.path.join(self.test_dir, "f")
        repos = self.load_dumpfile(dumpfile, 'g')
        convert_repository(repos, branch_path, RootLayout(),
            remember_parent=False)
        branch = Repository.open(branch_path)
        self.assertEqual(len(branch.all_revision_ids()), 1)
        b = Branch.open(branch_path)
        self.assertEquals(1, b.revno())
        self.assertEquals(None, b.get_parent())

    def load_dumpfile(self, dumpfile, target_path):
        load_dumpfile(dumpfile, target_path)
        return Repository.open(target_path)

    def test_dumpfile_open_empty_trunk(self):
        dumpfile = os.path.join(self.test_dir, "dumpfile")
        open(dumpfile, 'w').write(
"""SVN-fs-dump-format-version: 2

UUID: 6987ef2d-cd6b-461f-9991-6f1abef3bd59

Revision-number: 0
Prop-content-length: 56
Content-length: 56

K 8
svn:date
V 27
2006-07-02T13:14:51.972532Z
PROPS-END
""")
        branch_path = os.path.join(self.test_dir, "f")
        repos = self.load_dumpfile(dumpfile, 'g')
        convert_repository(repos, branch_path, TrunkLayout(0))
        repository = Repository.open(branch_path)
        self.assertEqual([], repository.all_revision_ids())
        self.assertRaises(NotBranchError, Branch.open, branch_path)

    def test_open_internal(self):
        filename = os.path.join(self.test_dir, "dumpfile")
        open(filename, 'w').write(
"""SVN-fs-dump-format-version: 2

UUID: 6987ef2d-cd6b-461f-9991-6f1abef3bd59

Revision-number: 0
Prop-content-length: 56
Content-length: 56

K 8
svn:date
V 27
2006-07-02T13:14:51.972532Z
PROPS-END

Revision-number: 1
Prop-content-length: 109
Content-length: 109

K 7
svn:log
V 9
Add trunk
K 10
svn:author
V 6
jelmer
K 8
svn:date
V 27
2006-07-02T13:58:02.528258Z
PROPS-END

Node-path: trunk
Node-kind: dir
Node-action: add
Prop-content-length: 10
Content-length: 10

PROPS-END


Node-path: trunk/bla
Node-kind: file
Node-action: add
Prop-content-length: 10
Text-content-length: 5
Text-content-md5: 6137cde4893c59f76f005a8123d8e8e6
Content-length: 15

PROPS-END
data


""")
        repos = self.load_dumpfile(filename, 'g')
        convert_repository(repos, os.path.join(self.test_dir, "e"),
                           TrunkLayout(0))
        mapping = repos.get_mapping()
        abspath = self.test_dir
        if sys.platform == 'win32':
            abspath = '/' + abspath
        branch = Branch.open(os.path.join(self.test_dir, "e", "trunk"))
        self.assertEqual(local_path_to_url(os.path.join(self.test_dir, "e", "trunk")), branch.base.rstrip("/"))
        self.assertEqual(mapping.revision_id_foreign_to_bzr(("6987ef2d-cd6b-461f-9991-6f1abef3bd59", 'trunk', 1)), branch.last_revision())


class TestPrefixed(SubversionTestCase):

    def setUp(self):
        super(TestPrefixed, self).setUp()
        self.repos_url = self.make_svn_repository('d')

        dc = self.get_commit_editor()
        b = dc.add_dir("base")
        t = b.add_dir("base/trunk")
        t.add_file("base/trunk/file").modify("data")
        bs = b.add_dir("base/branches")
        ab = bs.add_dir("base/branches/abranch")
        ab.add_file("base/branches/abranch/anotherfile").modify("data2")
        dc.close()

        dc = self.get_commit_editor()
        b = dc.open_dir("base")
        t = b.open_dir("base/trunk")
        t.open_file("base/trunk/file").modify("otherdata")
        dc.close()

    def get_commit_editor(self):
        return super(TestPrefixed, self).get_commit_editor(self.repos_url)

    def test_convert_simple(self):
        oldrepos = Repository.open(self.repos_url)
        convert_repository(oldrepos, "e", TrunkLayout(1),
                prefix="base", create_shared_repo=True)
        self.assertTrue(os.path.exists("e/trunk"))
        self.assertTrue(os.path.exists("e/branches/abranch"))
        self.assertFalse(os.path.exists("e/base"))


class TestContainsParentPath(TestCase):

    def test_doesnexist(self):
        self.assertFalse(contains_parent_path([], "foo"))

    def test_exists(self):
        self.assertTrue(contains_parent_path(["foo"], "foo"))
        self.assertTrue(contains_parent_path(["foo"], "foo/bar/blla"))
        self.assertTrue(contains_parent_path(["bar", "foo"], "foo/bar/blla"))
