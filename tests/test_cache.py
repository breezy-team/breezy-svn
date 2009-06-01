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

import os


from bzrlib.errors import (
    NoSuchRevision,
    )
from bzrlib.tests import (
    TestCase,
    TestCaseInTempDir,
    TestSkipped,
    UnavailableFeature,
    )


from bzrlib.plugins.svn.mapping4 import (
    BzrSvnMappingv4,
    )


class LogCacheTests(object):

    def test_last_revnum(self):
        self.cache.insert_paths(42, {"foo": ("A", None, -1)})
        self.assertEquals(42, self.cache.last_revnum())

    def test_insert_paths(self):
        self.cache.insert_paths(42, {"foo": ("A", None, -1)})
        self.assertEquals({"foo": ("A", None, -1)}, 
                self.cache.get_revision_paths(42))

    def test_insert_revprops(self):
        self.cache.insert_revprops(100, {"some": "data"}, True)
        self.assertEquals(({"some": "data"}, True), 
                           self.cache.get_revprops(100))

    def test_insert_revinfo(self):
        self.cache.insert_revprops(45, {"some": "data"}, True)
        self.cache.insert_revprops(42, {"some": "data"}, False)
        self.assertEquals(({"some": "data"}, True), self.cache.get_revprops(45))
        self.assertEquals(({"some": "data"}, False), self.cache.get_revprops(42))

    def test_find_latest_change(self):
        self.cache.insert_paths(42, {"foo": ("A", None, -1)})
        try:
            self.assertEquals(42, self.cache.find_latest_change("foo", 42))
            self.assertEquals(42, self.cache.find_latest_change("foo", 45))
        except NotImplementedError:
            raise TestSkipped()


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
        import tdb
        self.cache = LogCache(tdb_open("cache.tdb", 0, tdb.DEFAULT, os.O_RDWR|os.O_CREAT))


class RevidMapCacheTests(object):

    def test_lookup_revids_seen(self):
        self.assertEquals(0, self.cache.last_revnum_checked("trunk"))
        self.cache.set_last_revnum_checked("trunk", 45)
        self.assertEquals(45, self.cache.last_revnum_checked("trunk"))

    def test_lookup_revid_nonexistant(self):
        self.assertRaises(NoSuchRevision, lambda: self.cache.lookup_revid("bla"))

    def test_lookup_revid(self):
        self.cache.insert_revid("bla", "mypath", 42, 42, "brainslug")
        self.assertEquals(("mypath", 42, 42, "brainslug"), 
                self.cache.lookup_revid("bla"))

    def test_lookup_revid_space(self):
        self.cache.insert_revid("bla", "my path", 42, 42, "brainslug")
        self.assertEquals(("my path", 42, 42, "brainslug"), 
                self.cache.lookup_revid("bla"))

    def test_lookup_branch(self):
        self.cache.insert_revid("bla", "mypath", 42, 42, "brainslug")
        self.assertEquals("bla", 
                self.cache.lookup_branch_revnum(42, "mypath", "brainslug"))

    def test_lookup_branch_nonexistant(self):
        self.assertIs(None,
                self.cache.lookup_branch_revnum(42, "mypath", "foo"))

    def test_lookup_branch_incomplete(self):
        self.cache.insert_revid("bla", "mypath", 42, 200, "brainslug")
        self.assertEquals(None, 
                self.cache.lookup_branch_revnum(42, "mypath", "brainslug"))


class SqliteRevidMapCacheTests(TestCase,RevidMapCacheTests):

    def setUp(self):
        super(SqliteRevidMapCacheTests, self).setUp()
        from bzrlib.plugins.svn.cache.sqlitecache import RevisionIdMapCache
        self.cache = RevisionIdMapCache()


class TdbRevidMapCacheTests(TestCaseInTempDir,RevidMapCacheTests):

    def setUp(self):
        super(TdbRevidMapCacheTests, self).setUp()
        try:
            from bzrlib.plugins.svn.cache.tdbcache import RevisionIdMapCache, tdb_open
        except ImportError:
            raise UnavailableFeature
        import tdb
        self.cache = RevisionIdMapCache(tdb_open("cache.tdb", 0, tdb.DEFAULT, os.O_RDWR|os.O_CREAT))


class RevInfoCacheTests(object):

    def test_get_unknown_revision(self):
        self.assertRaises(KeyError, 
            self.cache.get_revision, ("bfdshfksdjh", "mypath", 1), 
            BzrSvnMappingv4())

    def test_get_revision(self):
        self.cache.insert_revision(("fsdkjhfsdkjhfsd", "mypath", 1), 
            BzrSvnMappingv4(), (42, "somerevid", False), "oldlhs")
        self.assertEquals(((42, "somerevid", False), "oldlhs"),
            self.cache.get_revision(("bfdshfksdjh", "mypath", 1), 
            BzrSvnMappingv4()))

    def test_get_revision_null_revid(self):
        mapping = BzrSvnMappingv4()
        foreign_revid = ("fsdkjhfsdkjhfsd", "mypath", 1)
        self.cache.insert_revision(foreign_revid, 
            mapping, (None, None, False), "oldlhs")
        self.assertEquals(
            ((None, mapping.revision_id_foreign_to_bzr(foreign_revid), False), "oldlhs"),
            self.cache.get_revision(foreign_revid, mapping))

    def test_get_original_mapping_none(self):
        self.cache.set_original_mapping(("fsdkjhfsdkjhfsd", "mypath", 1), 
            None)
        self.assertEquals(None, self.cache.get_original_mapping(("fkjhfsdkjh", "mypath", 1)))

    def test_get_original_mapping_unknown(self):
        self.assertRaises(KeyError, self.cache.get_original_mapping, ("fkjhfsdkjh", "mypath", 1))

    def test_get_original_mapping_v4(self):
        self.cache.set_original_mapping(("fsdkjhfsdkjhfsd", "mypath", 1), 
            BzrSvnMappingv4())
        self.assertEquals(BzrSvnMappingv4(), self.cache.get_original_mapping(("fkjhfsdkjh", "mypath", 1)))


class SqliteRevInfoCacheTests(TestCase,RevInfoCacheTests):

    def setUp(self):
        super(SqliteRevInfoCacheTests, self).setUp()
        from bzrlib.plugins.svn.cache.sqlitecache import RevisionInfoCache
        self.cache = RevisionInfoCache()


class TdbRevInfoCacheTests(TestCaseInTempDir,RevInfoCacheTests):

    def setUp(self):
        super(TdbRevInfoCacheTests, self).setUp()
        try:
            from bzrlib.plugins.svn.cache.tdbcache import RevisionInfoCache, tdb_open
        except ImportError:
            raise UnavailableFeature
        import tdb
        self.cache = RevisionInfoCache(tdb_open("cache.tdb", 0, tdb.DEFAULT, os.O_RDWR|os.O_CREAT))


class ParentsCacheTests:

    def test_noparents(self):
        self.cache.insert_parents("myrevid", ())
        self.assertEquals((), self.cache.lookup_parents("myrevid"))

    def test_single(self):
        self.cache.insert_parents("myrevid", ("single",))
        self.assertEquals(("single",), self.cache.lookup_parents("myrevid"))

    def test_multiple(self):
        self.cache.insert_parents("myrevid", ("one", "two"))
        self.assertEquals(("one", "two"), self.cache.lookup_parents("myrevid"))

    def test_nonexistant(self):
        self.assertEquals(None, self.cache.lookup_parents("myrevid"))

    def test_insert_twice(self):
        self.cache.insert_parents("myrevid", ("single",))
        self.cache.insert_parents("myrevid", ("second",))
        self.assertEquals(("second",), self.cache.lookup_parents("myrevid"))
        

class SqliteParentsCacheTests(TestCase,ParentsCacheTests):

    def setUp(self):
        super(SqliteParentsCacheTests, self).setUp()
        from bzrlib.plugins.svn.cache.sqlitecache import ParentsCache
        self.cache = ParentsCache()


class TdbParentsCacheTests(TestCaseInTempDir,ParentsCacheTests):

    def setUp(self):
        super(TdbParentsCacheTests, self).setUp()
        try:
            from bzrlib.plugins.svn.cache.tdbcache import ParentsCache, tdb_open
        except ImportError:
            raise UnavailableFeature
        import tdb
        self.cache = ParentsCache(tdb_open("cache.tdb", 0, tdb.DEFAULT, os.O_RDWR|os.O_CREAT))
