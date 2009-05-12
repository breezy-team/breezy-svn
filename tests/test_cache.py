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


from bzrlib.tests import (
    TestCase,
    TestCaseInTempDir,
    UnavailableFeature,
    )


class LogCacheTests(object):

    def test_insert_paths(self):
        self.cache.insert_paths(42, {"foo": ("A", None, -1)})
        self.assertEquals({"foo": ("A", None, -1)}, self.cache.get_revision_paths(42))

    def test_insert_revprops(self):
        self.cache.insert_revprops(100, {"some": "data"})
        self.assertEquals({"some": "data"}, self.cache.get_revprops(100))

    def test_insert_revinfo(self):
        self.cache.insert_revinfo(45, True)
        self.cache.insert_revinfo(42, False)
        self.assertTrue(self.cache.has_all_revprops(45))
        self.assertFalse(self.cache.has_all_revprops(42))

    def test_find_latest_change(self):
        self.cache.insert_paths(42, {"foo": ("A", None, -1)})
        self.assertEquals(42, self.cache.find_latest_change("foo", 42))
        self.assertEquals(42, self.cache.find_latest_change("foo", 45))


class SqliteLogCacheTests(TestCase,LogCacheTests):

    def setUp(self):
        super(SqliteLogCacheTests, self).setUp()
        from bzrlib.plugins.svn.cache.sqlitecache import LogCache
        self.cache = LogCache()


class TdbLogCacheTests(TestCaseInTempDir,LogCacheTests):

    def setUp(self):
        super(TdbLogCacheTests, self).setUp()
        try:
            from bzrlib.plugins.svn.cache.tdbcache import LogCache, tdb_open
        except ImportError:
            raise UnavailableFeature
        self.cache = LogCache(tdb_open("cache.tdb"))

