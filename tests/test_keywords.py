# Copyright (C) 2009 Jelmer Vernooij <jelmer@samba.org>

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


"""Tests for svn-keywords."""


from bzrlib.revision import (
    Revision,
    )
from bzrlib.tests import (
    TestCase,
    )

from bzrlib.plugins.svn.keywords import (
    compress_keywords,
    keywords,
    )


class CompressKeywordsTests(TestCase):

    def test_simple(self):
        self.assertEquals("foo $Id$ blie",
                compress_keywords("foo $Id: bla$ blie", ["Id"]))

    def test_not_allowed(self):
        self.assertEquals("foo $Id: bla$ blie",
                compress_keywords("foo $Id: bla$ blie", ["Bla"]))

    def test_already_compressed(self):
        self.assertEquals("foo $Id$ blie",
                compress_keywords("foo $Id$ blie", ["Bla"]))


class MockRevmeta(object):

    def __init__(self, revprops, revnum, branch_path, repo_url):
        self.revprops = revprops
        self.revnum = revnum
        self.branch_path = branch_path
        class MockRepo(object):

            def __init__(self, url):
                self.base = url
        self.repository = MockRepo(repo_url)


class TestKeywordExpansion(TestCase):

    def test_date_revmeta_not_native(self):
        rev = Revision("somerevid")
        rev.timestamp = 43842423
        self.assertEquals('1971-05-23T10:27:03.000000Z',
                keywords['Date']("somerevid", rev, "somepath", None))

    def test_date_revmeta_native(self):
        rev = Revision("somerevid")
        revmeta = MockRevmeta({"svn:date": "somedateblabla"}, 42, "/trunk",
            "http://foo.host")
        self.assertEquals('somedateblabla',
                keywords['Date']("somerevid", rev, "somepath", revmeta))

    def test_rev_not_native_not_svn(self):
        rev = Revision("somerevid")
        self.assertEquals('somerevid',
                keywords['Rev']("somerevid", rev, "somepath", None))

    def test_rev_not_native_svn(self):
        revid = "svn-v4:612f8ebc-c883-4be0-9ee0-a4e9ef946e3a:trunk:36984"
        rev = Revision(revid)
        self.assertEquals('36984',
                keywords['Rev'](revid, rev, "somepath", None))

    def test_rev_native(self):
        revmeta = MockRevmeta({}, 42, "/trunk", "http://foo.host")
        rev = Revision("somerevid")
        self.assertEquals('42',
                keywords['Rev']("somerevid", rev, "somepath", revmeta))

    def test_author_native(self):
        revmeta = MockRevmeta({"svn:author": "someauthor"}, 42,
                "/trunk", "http://foo.host")
        rev = Revision("somerevid")
        self.assertEquals('someauthor',
                keywords['Author']("somerevid", rev, "somepath", revmeta))

    def test_author_non_native(self):
        rev = Revision("somerevid")
        rev.committer = "Some Committer <foo@example.com>"
        self.assertEquals("Some Committer <foo@example.com>",
                keywords['Author']("somerevid", rev, "somepath", None))

    def test_url_native(self):
        rev = Revision("somerevid")
        revmeta = MockRevmeta({}, 42, "/trunk", "http://some.host")
        self.assertEquals("http://some.host/trunk/somepath",
                keywords['URL']("somerevid", rev, "somepath", revmeta))

    def test_url_non_native(self):
        rev = Revision("somerevid")
        self.assertEquals("somepath",
                keywords['URL']("somerevid", rev, "somepath", None))

    def test_id_native(self):
        rev = Revision("somerevid")
        rev.timestamp = 43842423
        rev.committer = "Joe <joe@example.com>"
        self.assertEquals(
            "somepath somerevid 1971-05-23T10:27:03.000000Z Joe <joe@example.com>",
            keywords['Id']("somerevid", rev, "somedir/somepath", None))
