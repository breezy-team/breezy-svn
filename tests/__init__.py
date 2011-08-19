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

"""Tests for the bzr-svn plugin."""

from subvertpy import NODE_UNKNOWN
import subvertpy.tests

from bzrlib import osutils
from bzrlib.bzrdir import (
    BzrDir,
    )
from bzrlib.tests import (
    TestCaseInTempDir,
    )

from bzrlib.plugins.svn import (
    transport as _mod_svn_transport,
    )

from subvertpy.ra import RemoteAccess


class RecordingRemoteAccess(object):
    """Trivial RemoteAccess wrapper that records all activity."""

    busy = property(lambda self: self.actual.busy)
    url = property(lambda self: self.actual.url)

    def __init__(self, *args, **kwargs):
        self.actual = RemoteAccess(*args, **kwargs)

    def check_path(self, path, revnum):
        self.__class__.calls.append(("check-path", (revnum, path)))
        return self.actual.check_path(path, revnum)

    def stat(self, path, revnum):
        self.__class__.calls.append(("stat", (revnum, path)))
        return self.actual.stat(path, revnum)

    def has_capability(self, cap):
        self.__class__.calls.append(("has-capability", (cap,)))
        return self.actual.has_capability(cap)

    def get_uuid(self):
        self.__class__.calls.append(("get-uuid", ()))
        return self.actual.get_uuid()

    def get_repos_root(self):
        self.__class__.calls.append(("get-repos", ()))
        return self.actual.get_repos_root()

    def get_latest_revnum(self):
        self.__class__.calls.append(("get-latest-revnum", ()))
        return self.actual.get_latest_revnum()

    def get_log(self, callback, paths, from_revnum, to_revnum, limit, *args, **kwargs):
        self.__class__.calls.append(("log", (from_revnum, to_revnum, paths, limit)))
        return self.actual.get_log(callback, paths,
                    from_revnum, to_revnum, limit, *args, **kwargs)

    def iter_log(self, paths, from_revnum, to_revnum, limit, *args, **kwargs):
        self.__class__.calls.append(("log", (from_revnum, to_revnum, paths, limit)))
        return self.actual.iter_log(paths,
                    from_revnum, to_revnum, limit, *args, **kwargs)

    def change_rev_prop(self, revnum, name, value):
        self.__class__.calls.append(("change-revprop", (revnum, name, value)))
        return self.actual.change_rev_prop(revnum, name, value)

    def get_dir(self, path, revnum=-1, fields=0):
        self.__class__.calls.append(("get-dir", (path, revnum, fields)))
        return self.actual.get_dir(path, revnum, fields)

    def get_file(self, path, stream, revnum):
        self.__class__.calls.append(("get-file", (path, revnum)))
        return self.actual.get_file(path, stream, revnum)

    def get_file_revs(self, path, start_revnum, end_revnum, handler):
        self.__class__.calls.append(("get-file-revs", (path, start_revnum, end_revnum)))
        return self.actual.get_file_revs(path, start_revnum, end_revnum,
            handler)

    def revprop_list(self, revnum):
        self.__class__.calls.append(("revprop-list", (revnum,)))
        return self.actual.revprop_list(revnum)

    def get_locations(self, path, peg_revnum, revnums):
        self.__class__.calls.append(("get-locations", (path, peg_revnum, revnums)))
        return self.actual.get_locations(path, peg_revnum, revnums)

    def do_update(self, revnum, path, start_empty, editor):
        self.__class__.calls.append(("do-update", (revnum, path, start_empty)))
        return self.actual.do_update(revnum, path, start_empty, editor)

    def do_diff(self, revision_to_update, diff_target, versus_url,
                diff_editor, recurse=True, ignore_ancestry=False, text_deltas=False,
                depth=None):
        self.__class__.calls.append(("diff", (revision_to_update, diff_target,
            versus_url, text_deltas, depth)))
        return self.actual.do_diff(revision_to_update, diff_target, versus_url,
                diff_editor, recurse, ignore_ancestry, text_deltas)

    def do_switch(self, revnum, path, start_empty, to_url, editor):
        self.__class__.calls.append(("switch", (revnum, path, start_empty, to_url)))
        return self.actual.do_switch(revnum, path, start_empty, to_url, editor)

    def reparent(self, url):
        self.__class__.calls.append(("reparent", (url,)))
        return self.actual.reparent(url)

    def get_commit_editor(self, *args, **kwargs):
        self.__class__.calls.append(("commit", ()))
        return self.actual.get_commit_editor(*args, **kwargs)

    def rev_proplist(self, revnum):
        self.__class__.calls.append(("rev-proplist", (revnum,)))
        return self.actual.rev_proplist(revnum)

    def replay_range(self, start_revision, end_revision, low_water_mark, cbs,
                     send_deltas=True):
        self.__class__.calls.append(("replay-range", (start_revision,
            end_revision, low_water_mark, send_deltas)))
        mutter("svn replay-range %d -> %d (low water mark: %d)" % (start_revision, end_revision, low_water_mark))
        return self.actual.replay_range(start_revision, end_revision, low_water_mark, cbs,
                   send_deltas)

    def replay(self, revision, low_water_mark, editor, send_deltas=True):
        self.__class__.calls.append(("replay", (revision, low_water_mark, send_deltas)))
        return self.actual.replay(revision, low_water_mark, editor, send_deltas)



class SubversionTestCase(subvertpy.tests.SubversionTestCase,TestCaseInTempDir):

    def make_repository(self, relpath, allow_revprop_changes=True):
        """Create an SVN repository.

        :param relpath: Relative path at which to create the repository.
        :param allow_revprop_changes: Allow changes to SVN revision
            properties.
        :return: A bzr-friendly URL for the created repository.
        """
        return _mod_svn_transport.svn_to_bzr_url(
            subvertpy.tests.SubversionTestCase.make_repository(self,
                relpath, allow_revprop_changes))

    def setUp(self):
        subvertpy.tests.SubversionTestCase.setUp(self)
        subvertpy.tests.SubversionTestCase.tearDown(self)
        TestCaseInTempDir.setUp(self)
        if type(self.test_dir) == unicode:
            self.test_dir = self.test_dir.encode(osutils._fs_enc)

    def tearDown(self):
        TestCaseInTempDir.tearDown(self)

    def make_local_bzrdir(self, repos_path, relpath):
        """Create a repository and checkout."""

        repos_url = self.make_repository(repos_path)
        self.make_checkout(repos_url, relpath)

        return BzrDir.open(relpath)

    def make_client_and_bzrdir(self, repospath, clientpath):
        repos_url = self.make_client(repospath, clientpath)

        return BzrDir.open(repos_url)

    def assertChangedPathEquals(self, expected, got, msg=None):
        if expected[:3] == got[:3] and got[3] in (expected[3], NODE_UNKNOWN):
            return
        self.assertEquals(expected, got, msg)

    def assertChangedPathsEquals(self, expected, got, msg=None):
        self.assertIsInstance(expected, dict)
        self.assertIsInstance(got, dict)
        if len(expected) != len(got):
            self.assertEquals(expected, got, msg)
        for p, v1 in expected.iteritems():
            try:
                v2 = got[p]
            except KeyError:
                self.assertEquals(expected, got, msg)
            self.assertChangedPathEquals(v1, v2, msg)

    def assertBranchLogEquals(self, expected, got, msg=None):
        if len(expected) != len(got):
            self.assertEquals(expected, got, msg)
        for (root1, changes1, revnum1), (root2, changes2, revnum2) in zip(expected, got):
            self.assertEquals(revnum1, revnum2)
            self.assertEquals(root1, root2)
            self.assertChangedPathsEquals(changes1, changes2, msg)

    def recordRemoteAccessCalls(self):
        RecordingRemoteAccess.calls = []
        self.overrideAttr(_mod_svn_transport, "RemoteAccess",
                          RecordingRemoteAccess)

    def assertRemoteAccessCalls(self, expected):
        self.assertEquals(RecordingRemoteAccess.calls, expected)


def test_suite():
    from unittest import TestSuite
    from bzrlib.tests import TestUtil

    loader = TestUtil.TestLoader()

    suite = TestSuite()

    testmod_names = [
            'test_branch',
            'test_branchprops',
            'test_cache',
            'test_changes',
            'test_checkout',
            'test_commit',
            'test_commit_ids',
            'test_config',
            'test_convert',
            'test_errors',
            'test_fetch',
            'test_fileids',
            'test_keywords',
            'layout.test_guess',
            'layout.test_standard',
            'test_logwalker',
            'test_mapping',
            'test_metagraph',
            'test_parents',
            'test_push',
            'test_remote',
            'test_repository',
            'test_revmeta',
            'test_revspec',
            'test_send',
            'test_svk',
            'test_transport',
            'test_tree',
            'test_workingtree',
            'test_blackbox',
            'mapping_implementations',
            'mapping3',
            'mapping3.test_scheme']
    suite.addTest(loader.loadTestsFromModuleNames(["%s.%s" % (__name__, i) for i in testmod_names]))

    return suite
