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


"""SQLite implementation of the bzr-svn cache."""

import os

from bzrlib import (
    debug,
    errors,
    trace,
    osutils,
    )

from bzrlib.plugins.svn.cache import (
    RepositoryCache,
    cachedbs,
    )
from bzrlib.plugins.svn.mapping import (
    mapping_registry,
    )


def check_pysqlite_version(sqlite3):
    """Check that sqlite library is compatible.

    """
    if (sqlite3.sqlite_version_info[0] < 3 or
            (sqlite3.sqlite_version_info[0] == 3 and
             sqlite3.sqlite_version_info[1] < 3)):
        trace.warning('Needs at least sqlite 3.3.x')
        raise errors.BzrError("incompatible sqlite library")

try:
    try:
        import sqlite3
        check_pysqlite_version(sqlite3)
    except (ImportError, errors.BzrError), e:
        from pysqlite2 import dbapi2 as sqlite3
        check_pysqlite_version(sqlite3)
except:
    trace.warning('Needs at least Python2.5 or Python2.4 with the pysqlite2 '
            'module')
    raise errors.BzrError("missing sqlite library")


connect_cachefile = sqlite3.connect


class CacheTable(object):
    """Simple base class for SQLite-based caches."""

    def __init__(self, cache_db=None):
        if cache_db is None:
            self.cachedb = sqlite3.connect(":memory:")
        else:
            self.cachedb = cache_db
        self._commit_interval = 500
        self._create_table()
        self.cachedb.commit()
        self._commit_countdown = self._commit_interval

    def commit(self):
        """Commit the changes to the database."""
        self.cachedb.commit()
        self._commit_countdown = self._commit_interval

    def commit_conditionally(self):
        self._commit_countdown -= 1
        if self._commit_countdown <= 0:
            self.commit()

    def _create_table(self):
        pass

    @staticmethod
    def mutter(text, *args):
        if "cache" in debug.debug_flags:
            trace.mutter(text, *args)


CACHE_DB_VERSION = 5


class RevisionIdMapCache(CacheTable):
    """Revision id mapping store.

    Stores mapping from revid -> (path, revnum, mapping)
    """

    def _create_table(self):
        self.cachedb.executescript("""
        create table if not exists revmap (
            revid text not null,
            path text not null,
            min_revnum integer,
            max_revnum integer,
            mapping text not null
            );
        create index if not exists revid on revmap (revid);
        create unique index if not exists revid_path_mapping on revmap (revid, path, mapping);
        drop index if exists lookup_branch_revnum;
        create index if not exists lookup_branch_revnum_non_unique on revmap (max_revnum, min_revnum, path, mapping);
        create table if not exists revids_seen (
            layout text not null,
            max_revnum int
            );
        create unique index if not exists layout on revids_seen (layout);
        """)
        # Revisions ids are quite expensive
        self._commit_interval = 500

    def set_last_revnum_checked(self, layout, revnum):
        """Remember the latest revision number that has been checked
        for a particular layout.

        :param layout: Repository layout.
        :param revnum: Revision number.
        """
        self.cachedb.execute("replace into revids_seen (layout, max_revnum) VALUES (?, ?)", (layout, revnum))
        self.commit_conditionally()

    def last_revnum_checked(self, layout):
        """Retrieve the latest revision number that has been checked
        for revision ids for a particular layout.

        :param layout: Repository layout.
        :return: Last revision number checked or 0.
        """
        self.mutter("last revnum checked %r", layout)
        ret = self.cachedb.execute(
            "select max_revnum from revids_seen where layout = ?", (layout,)).fetchone()
        if ret is None:
            return 0
        return int(ret[0])

    def lookup_revid(self, revid):
        """Lookup the details for a particular revision id.

        :param revid: Revision id.
        :return: Tuple with path inside repository, minimum revision number, maximum revision number and
            mapping.
        """
        assert isinstance(revid, str)
        self.mutter("lookup revid %r", revid)
        ret = self.cachedb.execute(
            "select path, min_revnum, max_revnum, mapping from revmap where revid=? order by abs(min_revnum-max_revnum) asc", (revid,)).fetchone()
        if ret is None:
            raise errors.NoSuchRevision(self, revid)
        (path, min_revnum, max_revnum, mapping) = (ret[0].encode("utf-8"), int(ret[1]), int(ret[2]), ret[3].encode("utf-8"))
        if min_revnum > max_revnum:
            return (path, max_revnum, min_revnum, mapping)
        else:
            return (path, min_revnum, max_revnum, mapping)

    def lookup_branch_revnum(self, revnum, path, mapping):
        """Lookup a revision by revision number, branch path and mapping.

        :param revnum: Subversion revision number.
        :param path: Subversion branch path.
        :param mapping: Mapping
        """
        assert isinstance(revnum, int)
        assert isinstance(path, str)
        assert isinstance(mapping, str)
        row = self.cachedb.execute(
                "select revid from revmap where max_revnum=? and min_revnum=? and path=? and mapping=?", (revnum, revnum, path, mapping)).fetchone()
        if row is not None:
            ret = str(row[0])
        else:
            ret = None
        self.mutter("lookup branch,revnum %r:%r -> %r", path, revnum, ret)
        return ret

    def insert_revid(self, revid, branch, min_revnum, max_revnum, mapping):
        """Insert a revision id into the revision id cache.

        :param revid: Revision id for which to insert metadata.
        :param branch: Branch path at which the revision was seen
        :param min_revnum: Minimum Subversion revision number in which the
                           revid was found
        :param max_revnum: Maximum Subversion revision number in which the
                           revid was found
        :param mapping: Name of the mapping with which the revision
                       was found
        """
        assert revid is not None and revid != ""
        assert isinstance(mapping, str)
        assert isinstance(branch, str)
        assert isinstance(min_revnum, int) and isinstance(max_revnum, int)
        assert min_revnum <= max_revnum
        self.mutter("insert revid %r:%r-%r -> %r", branch, min_revnum, max_revnum, revid)
        if min_revnum == max_revnum:
            cursor = self.cachedb.execute(
                "update revmap set min_revnum = ?, max_revnum = ? WHERE revid=? AND path=? AND mapping=?",
                (min_revnum, max_revnum, revid, branch, mapping))
        else:
            cursor = self.cachedb.execute(
                "update revmap set min_revnum = MAX(min_revnum,?), max_revnum = MIN(max_revnum, ?) WHERE revid=? AND path=? AND mapping=?",
                (min_revnum, max_revnum, revid, branch, mapping))
        if cursor.rowcount == 0:
            self.cachedb.execute(
                "insert into revmap (revid,path,min_revnum,max_revnum,mapping) VALUES (?,?,?,?,?)",
                (revid, branch, min_revnum, max_revnum, mapping))


class RevisionInfoCache(CacheTable):

    def _create_table(self):
        self.cachedb.executescript("""
        create table if not exists revmetainfo (path text, revnum integer, mapping text, revid text, revno int, hidden int, original_mapping text, stored_lhs_parent_revid text);
        create unique index if not exists revmeta_path_mapping on revmetainfo(revnum, path, mapping);
        create table if not exists original_mapping (path text, revnum integer, original_mapping text);
        create unique index if not exists original_mapping_path_revnum on original_mapping (path, revnum);
        """)
        self._commit_interval = 500

    def set_original_mapping(self, foreign_revid, original_mapping):
        if original_mapping is not None:
            orig_mapping_name = original_mapping.name
        else:
            orig_mapping_name = None
        self.cachedb.execute("insert into original_mapping (path, revnum, original_mapping) values (?, ?, ?)", (foreign_revid[1], foreign_revid[2], orig_mapping_name))

    def insert_revision(self, foreign_revid, mapping, (revno, revid, hidden),
            stored_lhs_parent_revid):
        """Insert a revision to the cache.

        :param foreign_revid: Foreign revision id
        :param mapping: Mapping used
        :param info: Tuple with (revno, revid, hidden)
        :param stored_lhs_parent_revid: Stored lhs parent revision
        """
        self.cachedb.execute("insert into revmetainfo (path, revnum, mapping, revid, revno, hidden, stored_lhs_parent_revid) values (?, ?, ?, ?, ?, ?, ?)", (foreign_revid[1], foreign_revid[2], mapping.name, revid, revno, hidden, stored_lhs_parent_revid))

    def get_revision(self, foreign_revid, mapping):
        """Get the revision metadata info for a (foreign_revid, mapping) tuple.

        :param foreign_revid: Foreign revision id
        :param mapping: Mapping
        :return: Tuple with (stored revno, revid, hidden),
            stored_lhs_parent_revid
        """
        # Will raise KeyError if not present
        row = self.cachedb.execute("select revno, revid, hidden, stored_lhs_parent_revid from revmetainfo where path = ? and revnum = ? and mapping = ?", (foreign_revid[1], foreign_revid[2], mapping.name)).fetchone()
        if row is None:
            raise KeyError((foreign_revid, mapping))
        else:
            if row[3] is None:
                stored_lhs_parent_revid = None
            else:
                stored_lhs_parent_revid = row[3].encode("utf-8")
            if row[1] is None:
                stored_revid = mapping.revision_id_foreign_to_bzr(foreign_revid)
            else:
                stored_revid = row[1].encode("utf-8")
            return ((row[0], stored_revid, row[2]), stored_lhs_parent_revid)

    def get_original_mapping(self, foreign_revid):
        """Find the original mapping for a revision.

        :param foreign_revid: Foreign revision id
        :return: Mapping object or None
        """
        row = self.cachedb.execute("select original_mapping from original_mapping where path = ? and revnum = ?", (foreign_revid[1], foreign_revid[2])).fetchone()
        if row is None:
            raise KeyError(foreign_revid)
        if row[0] is None:
            return None
        else:
            return mapping_registry.parse_mapping_name("svn-" + row[0].encode("utf-8"))


class LogCache(CacheTable):
    """Log browser cache table manager. The methods of this class
    encapsulate the SQL commands used by CachingLogWalker to access
    the log cache tables."""

    def _create_table(self):
        self.cachedb.executescript("""
            create table if not exists changed_path(rev integer, action text, path text, copyfrom_path text, copyfrom_rev integer);
            create index if not exists path_rev on changed_path(rev);
            create unique index if not exists path_rev_path on changed_path(rev, path);
            create unique index if not exists path_rev_path_action on changed_path(rev, path, action);

            create table if not exists revprop(rev integer, name text, value text);
            create table if not exists revinfo(rev integer, all_revprops int);
            create index if not exists revprop_rev on revprop(rev);
            create unique index if not exists revprop_rev_name on revprop(rev, name);
            create unique index if not exists revinfo_rev on revinfo(rev);
        """)

    def find_latest_change(self, path, revnum):
        if path == "":
            return self.cachedb.execute("select max(rev) from changed_path where rev <= ?", (revnum,)).fetchone()[0]
        return self.cachedb.execute("""
            select max(rev) from changed_path
            where rev <= ?
            and (path=?
                 or path glob ?
                 or (? glob (path || '/*')
                     and action in ('A', 'R')))
        """, (revnum, path, path + "/*", path)).fetchone()[0]

    def get_revision_paths(self, revnum):
        """Return all history information for a given revision number.

        :param revnum: Revision number of revision.
        """
        result = self.cachedb.execute("select path, action, copyfrom_path, copyfrom_rev from changed_path where rev=?", (revnum,))
        paths = {}
        for p, act, cf, cr in result:
            if cf is not None:
                cf = cf.encode("utf-8")
            paths[p.encode("utf-8")] = (act, cf, cr)
        return paths

    def insert_paths(self, rev, orig_paths):
        """Insert new history information into the cache.

        :param rev: Revision number of the revision
        :param orig_paths: SVN-style changes dictionary
        """
        if orig_paths is None or orig_paths == {}:
            return
        new_paths = []
        for p in orig_paths:
            copyfrom_path = orig_paths[p][1]
            if copyfrom_path is not None:
                copyfrom_path = copyfrom_path.strip("/").decode("utf-8")

            new_paths.append((rev, p.strip("/").decode("utf-8"), orig_paths[p][0], copyfrom_path, orig_paths[p][2]))

        self.cachedb.executemany("replace into changed_path (rev, path, action, copyfrom_path, copyfrom_rev) values (?, ?, ?, ?, ?)", new_paths)

    def drop_revprops(self, revnum):
        self.cachedb.execute("update revinfo set all_revprops = 0 where rev = ?", (revnum,))

    def get_revprops(self, revnum):
        """Retrieve all the cached revision properties.

        :param revnum: Revision number of revision to retrieve revprops for.
        """
        result = self.cachedb.execute("select name, value from revprop where rev = ?", (revnum,))
        revprops = dict((k.encode("utf-8"), v.encode("utf-8")) for (k, v) in result)
        result = self.cachedb.execute("select all_revprops from revinfo where rev = ?", (revnum,)).fetchone()
        if result is None:
            all_revprops = False
        else:
            all_revprops = result[0]
        return (revprops, all_revprops)

    def insert_revprops(self, revision, revprops, all_revprops):
        if revprops is None:
            return
        self.cachedb.executemany("replace into revprop (rev, name, value) values (?, ?, ?)", [(revision, name.decode("utf-8", "replace"), value.decode("utf-8", "replace")) for (name, value) in revprops.iteritems()])
        self.cachedb.execute("""
            replace into revinfo (rev, all_revprops) values (?, ?)
        """, (revision, all_revprops))

    def has_all_revprops(self, revnum):
        """Check whether all revprops for a revision have been cached.

        :param revnum: Revision number of the revision.
        """

    def last_revnum(self):
        saved_revnum = self.cachedb.execute("SELECT MAX(rev) FROM changed_path").fetchone()[0]
        if saved_revnum is None:
            return 0
        return saved_revnum

    def min_revnum(self):
        return self.cachedb.execute("SELECT MIN(rev) FROM revprop").fetchone()[0]


class ParentsCache(CacheTable):

    def _create_table(self):
        self.cachedb.executescript("""
        create table if not exists parent (rev text, parent text, idx int);
        create unique index if not exists rev_parent_idx on parent (rev, idx);
        create unique index if not exists rev_parent on parent (rev, parent);
        """)
        self._commit_interval = 200

    def insert_parents(self, revid, parents):
        self.mutter('insert parents: %r -> %r', revid, parents)
        if len(parents) == 0:
            self.cachedb.execute("replace into parent (rev, parent, idx) values (?, NULL, -1)", (revid,))
        else:
            for i, p in enumerate(parents):
                self.cachedb.execute("replace into parent (rev, parent, idx) values (?, ?, ?)", (revid, p, i))

    def lookup_parents(self, revid):
        self.mutter('lookup parents: %r', revid)
        rows = self.cachedb.execute("select parent from parent where rev = ? order by idx", (revid, )).fetchall()
        if len(rows) == 0:
            return None
        return tuple([row[0].encode("utf-8") for row in rows if row[0] is not None])


class SqliteRepositoryCache(RepositoryCache):
    """Object that provides a cache related to a particular UUID."""

    def open_sqlite(self):
        cache_file = os.path.join(self.create_cache_dir(), 'cache-v%d' % CACHE_DB_VERSION)
        assert isinstance(cache_file, str)
        if not cachedbs().has_key(cache_file):
            cachedbs()[cache_file] = connect_cachefile(cache_file.decode(osutils._fs_enc).encode("utf-8"))
        return cachedbs()[cache_file]

    def open_revid_map(self):
        return RevisionIdMapCache(self.open_sqlite())

    def open_logwalker(self):
        return LogCache(self.open_sqlite())

    def open_revision_cache(self):
        return RevisionInfoCache(self.open_sqlite())

    def open_parents(self):
        return ParentsCache(self.open_sqlite())
