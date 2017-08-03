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

"""Subversion transport tests."""

import stat
import subvertpy
from subvertpy import ra
from unittest import TestCase
import urlparse

from breezy import urlutils
from breezy.errors import (
    FileExists,
    NoSuchFile,
    TransportNotPossible,
    )

from breezy.plugins.svn.tests import SubversionTestCase
from breezy.plugins.svn.transport import (
    Connection,
    SvnRaTransport,
    bzr_to_svn_url,
    url_join_unescaped_path,
    _url_escape_uri,
    _url_unescape_uri,
    )

class SvnRaTest(SubversionTestCase):

    def test_open_nonexisting(self):
        self.assertRaises(urlutils.InvalidURL, SvnRaTransport,
                          "svn+nonexisting://foo/bar")

    def test_create(self):
        repos_url = self.make_svn_repository('a')
        t = SvnRaTransport(repos_url)
        self.assertIsInstance(t, SvnRaTransport)
        self.assertEqual(t.base, repos_url)
        self.assertEqual(t.is_readonly(), False)

    def test_segments(self):
        repos_url = self.make_svn_repository('a')
        t = SvnRaTransport("%s,branch=name" % repos_url)
        self.assertIsInstance(t, SvnRaTransport)
        self.assertEqual(t.svn_url, repos_url)
        self.assertEqual(t.get_segment_parameters(), {"branch": "name"})

    def test_create_direct(self):
        repos_url = self.make_svn_repository('a')
        t = SvnRaTransport(repos_url)
        self.assertIsInstance(t, SvnRaTransport)
        self.assertEqual(t.base, repos_url)

    def test_create_readonly(self):
        repos_url = self.make_svn_repository('a')
        t = SvnRaTransport("readonly+"+repos_url)
        self.assertIsInstance(t, SvnRaTransport)
        self.assertEqual(t.base, "readonly+"+repos_url)
        self.assertEqual(t.is_readonly(), True)

    def test_lock_read(self):
        repos_url = self.make_svn_repository('a')
        t = SvnRaTransport(repos_url)
        lock = t.lock_read(".")
        lock.unlock()

    def test_lock_write(self):
        repos_url = self.make_svn_repository('a')
        t = SvnRaTransport(repos_url)
        lock = t.lock_write(".")
        lock.unlock()

    def test_listable(self):
        repos_url = self.make_svn_repository('a')
        t = SvnRaTransport(repos_url)
        self.assertTrue(t.listable())

    def test_get_dir_rev(self):
        repos_url = self.make_svn_repository('d')

        dc = self.get_commit_editor(repos_url)
        foo = dc.add_dir("foo")
        foo.add_file("foo/bar").modify("Data")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.delete("foo")
        dc.close()

        t = SvnRaTransport(repos_url)
        lists = t.get_dir("foo", 1, 0)
        self.assertTrue("bar" in lists[0])

    def test_list_dir(self):
        repos_url = self.make_svn_repository('a')
        t = SvnRaTransport(repos_url)
        self.assertEqual([], t.list_dir("."))
        t.mkdir("foo")
        self.assertEqual(["foo"], t.list_dir("."))
        self.assertEqual([], t.list_dir("foo"))
        t.mkdir("foo/bar")
        self.assertEqual(["bar"], t.list_dir("foo"))

    def test_list_dir_file(self):
        repos_url = self.make_svn_repository('d')

        dc = self.get_commit_editor(repos_url)
        dc.add_file("file").modify("data")
        dc.close()

        t = SvnRaTransport(repos_url)
        self.assertEqual(["file"], t.list_dir("."))
        self.assertRaises(NoSuchFile, t.list_dir, "file")

    def test_clone(self):
        repos_url = self.make_svn_repository('d')

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("dir")
        dc.add_file("bl").modify("data")
        dc.close()

        t = SvnRaTransport(repos_url)
        self.assertEqual("%s/dir" % repos_url, t.clone('dir').base)

    def test_clone_none(self):
        repos_url = self.make_svn_repository('d')

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("dir")
        dc.add_file("bl").modify("data")
        dc.close()

        t = SvnRaTransport(repos_url)
        tt = t.clone()
        self.assertEqual(tt.base, t.base)

    def test_mkdir_readonly(self):
        repos_url = self.make_svn_repository('a')
        t = SvnRaTransport("readonly+"+repos_url)
        self.assertRaises(TransportNotPossible, t.mkdir, "bla")

    def test_mkdir(self):
        repos_url = self.make_svn_repository('a')
        t = SvnRaTransport(repos_url)
        t.mkdir("bla")

        c = ra.RemoteAccess(repos_url)
        self.assertEquals(c.check_path("bla", c.get_latest_revnum()),
                          subvertpy.NODE_DIR)
        t.mkdir("bla/subdir")
        self.assertEquals(c.check_path("bla/subdir", c.get_latest_revnum()),
                          subvertpy.NODE_DIR)

        t = SvnRaTransport(repos_url+"/nonexistant")
        t.mkdir(".")

    def test_has_dot(self):
        t = SvnRaTransport(self.make_svn_repository('a'))
        self.assertEqual(True, t.has("."))

    def test_stat(self):
        t = SvnRaTransport(self.make_svn_repository('a'))
        try:
            statob = t.stat(".")
        except TransportNotPossible:
            pass
        else:
            self.assertTrue(stat.S_ISDIR(statob.st_mode))

    def test_has_nonexistent(self):
        t = SvnRaTransport(self.make_svn_repository('a'))
        self.assertEqual(False, t.has("bar"))

    def test_mkdir_missing_parent(self):
        repos_url = self.make_svn_repository('a')
        t = SvnRaTransport(repos_url)
        self.assertRaises(NoSuchFile, t.mkdir, "bla/subdir")
        c = ra.RemoteAccess(repos_url)
        self.assertEquals(c.check_path("bla/subdir", c.get_latest_revnum()),
                          subvertpy.NODE_NONE)

    def test_mkdir_twice(self):
        repos_url = self.make_svn_repository('a')
        t = SvnRaTransport(repos_url)
        t.mkdir("bla")
        self.assertRaises(FileExists, t.mkdir, "bla")

    def test_clone2(self):
        repos_url = self.make_svn_repository('d')

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("dir")
        dc.add_file("bl").modify("data")
        dc.close()

        t = SvnRaTransport(repos_url)
        self.assertEqual("%s/dir" % repos_url, t.clone('dir').base)

    def test_get_root(self):
        repos_url = self.make_svn_repository('d')

        dc = self.get_commit_editor(repos_url)
        dc.add_dir("dir")
        dc.add_file("bl").modify("data")
        dc.close()

        t = SvnRaTransport("%s/dir" % repos_url)
        root = t.get_svn_repos_root()
        self.assertEqual(repos_url, root)

    def test_local_abspath(self):
        repos_url = self.make_svn_repository('a')
        t = SvnRaTransport("%s" % repos_url)
        self.assertEquals(urlutils.join(self.test_dir, "a"), t.local_abspath('.'))


class UrlConversionTest(TestCase):

    def test_bzr_to_svn_url(self):
        self.assertEqual(("svn://host/location", False),
                         bzr_to_svn_url("svn://host/location"))
        self.assertEqual(("svn+ssh://host/location", False),
                         bzr_to_svn_url("svn+ssh://host/location"))
        self.assertEqual(("http://host/location", False),
                         bzr_to_svn_url("http://host/location"))
        self.assertEqual(("http://host/location", False),
                         bzr_to_svn_url("svn+http://host/location"))
        self.assertEqual(("http://host/location", True),
                         bzr_to_svn_url("readonly+http://host/location"))
        self.assertEqual(("http://host/gtk+/location", False),
                         bzr_to_svn_url("svn+http://host/gtk%2B/location"))

    def test_url_unescape_uri(self):
        self.assertEquals("http://svn.gnome.org/svn/gtk+/trunk",
                _url_unescape_uri("http://svn.gnome.org/svn/gtk%2B/trunk"))
        self.assertEquals("http://svn.gnome.org/svn/gtk%20/trunk",
                _url_unescape_uri("http://svn.gnome.org/svn/gtk%20/trunk"))

    def test_url_escape_uri(self):
        self.assertEquals("http://svn.gnome.org/svn/gtk+/trunk",
                _url_escape_uri("http://svn.gnome.org/svn/gtk+/trunk"))
        self.assertEquals("http://svn.gnome.org/svn/gtk%20/trunk",
                _url_escape_uri("http://svn.gnome.org/svn/gtk /trunk"))

    def test_url_join_unescaped_path(self):
        self.assertEquals("http://svn.gnome.org/svn/gtk+/trunk",
                url_join_unescaped_path("http://svn.gnome.org/svn/", "gtk+/trunk"))

    def test_url_join_unescaped_comment(self):
        self.assertEquals("http://svn.gnome.org/svn/gtk+/trunk%23",
                url_join_unescaped_path("http://svn.gnome.org/svn/", "gtk+/trunk#"))


class UrlSplitSvnPlus(TestCase):

    def test_svn_plus_http(self):
        self.assertEquals(
            urlparse.SplitResult(scheme='svn+http',
                netloc='foo.bar@host.com', path='/svn/trunk', query='',
                fragment=''),
            urlparse.urlsplit('svn+http://foo.bar@host.com/svn/trunk'))


class ReadonlyConnectionTests(SubversionTestCase):

    def setUp(self):
        super(ReadonlyConnectionTests, self).setUp()
        self.repos_url = self.make_svn_repository("d")

    def test_get_latest_revnum(self):
        self.recordRemoteAccessCalls()
        conn = Connection(self.repos_url, readonly=True)
        self.assertEquals(0, conn.get_latest_revnum())
        self.assertRemoteAccessCalls([('get-latest-revnum', ())])

    def test_change_rev_prop(self):
        conn = Connection(self.repos_url, readonly=True)
        self.recordRemoteAccessCalls()
        self.assertRaises(TransportNotPossible, conn.change_rev_prop,
             3, "foo", "bar")
        self.assertRemoteAccessCalls([])

    def test_get_commit_editor(self):
        conn = Connection(self.repos_url, readonly=True)
        self.recordRemoteAccessCalls()
        self.assertRaises(TransportNotPossible, conn.get_commit_editor,
                { "svn:log": "msg" } )
        self.assertRemoteAccessCalls([])
