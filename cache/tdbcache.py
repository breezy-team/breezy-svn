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


"""TDB implementation of the bzr-svn cache."""

import os
import tdb
try:
    tdb.Tdb.get
except AttributeError:
    raise ImportError("tdb is out of date: doesn't have a Tdb.get attribute")

from breezy import (
    bencode,
    debug,
    errors,
    trace,
    )

from breezy.plugins.svn.cache import (
    RepositoryCache,
    )
from breezy.plugins.svn.mapping import (
    mapping_registry,
    )
from breezy.plugins.svn.revids import (
    RevisionIdMapCache,
    )
from breezy.plugins.svn.revmeta import (
    RevisionInfoCache,
    )
from breezy.plugins.svn.logwalker import (
    LogCache,
    )
from breezy.plugins.svn.parents import (
    ParentsCache,
    )

from subvertpy import NODE_UNKNOWN

from tdb import Tdb as tdb_open


CACHE_DB_VERSION = 1


class CacheTable(object):

    def __init__(self, db):
        self.db = db

    def commit(self):
        pass

    def mutter(self, text, *args, **kwargs):
        if "cache" in debug.debug_flags:
            trace.mutter(text, *args, **kwargs)


class TdbRevisionIdMapCache(RevisionIdMapCache, CacheTable):

    def set_last_revnum_checked(self, layout, revnum):
        """See RevisionIdMapCache.set_last_revnum_checked."""

        self.db["revidmap-last/%s" % str(layout)] = str(revnum)

    def last_revnum_checked(self, layout):
        """See RevisionIdMapCache.last_revnum_checked."""

        try:
            return int(self.db[b"revidmap-last/%s" % str(layout)].decode())
        except KeyError:
            return 0

    def lookup_revid(self, revid):
        """See RevisionIdMapCache.lookup_revid."""

        self.mutter('lookup-revid %s', revid)
        try:
            (min_revnum, max_revnum, mapping_name, path) = self.db[b"native-revid/" + revid].split(b" ", 3)
        except KeyError:
            raise errors.NoSuchRevision(self, revid)
        return (path.decode('utf-8'), int(min_revnum), int(max_revnum), mapping_name)

    def lookup_branch_revnum(self, revnum, path, mapping):
        """See RevisionIdMapCache.lookup_branch_revnum."""

        self.mutter('lookup-branch-revnum %s:%d', path, revnum)
        try:
            return self.db[b"foreign-revid/%d %s %s" % (revnum, getattr(mapping, "name", mapping), path.encode('utf-8'))]
        except KeyError:
            return None

    def insert_revid(self, revid, branch, min_revnum, max_revnum, mapping):
        """See RevisionIdMapCache.insert_revid."""

        mappingname = getattr(mapping, "name", mapping)
        encoded_branch_path = branch.encode('utf-8')
        self.db[b"native-revid/" + revid] = b"%d %d %s %s" % (min_revnum, max_revnum, mappingname, encoded_branch_path)
        if min_revnum == max_revnum:
            self.db[b"foreign-revid/%d %s %s" % (min_revnum, mappingname, encoded_branch_path)] = revid


class TdbRevisionInfoCache(RevisionInfoCache, CacheTable):

    def set_original_mapping(self, foreign_revid, original_mapping):
        """See RevisionInfoCache.set_original_mapping."""

        if original_mapping is not None:
            orig_mapping_name = original_mapping.name
        else:
            orig_mapping_name = ""
        encoded_branch_path = foreign_revid[1].encode('utf-8')
        self.db[b"original-mapping/%d %s" % (foreign_revid[2], encoded_branch_path)] = orig_mapping_name

    def insert_revision(self, foreign_revid, mapping, revdata,
            stored_lhs_parent_revid):
        """See RevisionInfoCache.insert_revision."""
        (revno, revid, hidden) = revdata
        if revid is None:
            revid = mapping.revision_id_foreign_to_bzr(foreign_revid)
        encoded_branch_path = foreign_revid[1].encode('utf-8')
        self.db[b"foreign-revid/%d %d %s %s" % (foreign_revid[2], foreign_revid[2], mapping.name, encoded_branch_path)] = revid
        basekey = b"%d %s %s" % (foreign_revid[2], mapping.name, encoded_branch_path)
        assert not hidden or revno is None
        if revno is not None:
            self.db[b"revno/%s" % basekey] = b"%d" % revno
        elif hidden:
            self.db[b"revno/%s" % basekey] = b""
        if stored_lhs_parent_revid is not None:
            self.db[b"lhs-parent-revid/%s" % basekey] = stored_lhs_parent_revid

    def get_revision(self, foreign_revid, mapping):
        """See RevisionInfoCache.get_revision."""

        self.mutter("get-revision %r,%r", foreign_revid, mapping)
        encoded_branch_path = foreign_revid[1].encode('utf-8')
        basekey = b"%d %s %s" % (foreign_revid[2], mapping.name, encoded_branch_path)
        revid = self.db[b"foreign-revid/%d %d %s %s" % (foreign_revid[2], foreign_revid[2], mapping.name, encoded_branch_path)]
        stored_lhs_parent_revid = self.db.get(b"lhs-parent-revid/%s" % basekey)
        try:
            revno = int(self.db[b"revno/%s" % basekey])
            hidden = False
        except KeyError:
            revno = None
            hidden = False
        except ValueError: # empty string
            hidden = True
            revno = None
        return ((revno, revid, hidden), stored_lhs_parent_revid)

    def get_original_mapping(self, foreign_revid):
        """See RevisionInfoCache.get_original_mapping."""

        self.mutter("get-original-mapping %r", foreign_revid)
        ret = self.db[b"original-mapping/%d %s" % (
            foreign_revid[2], foreign_revid[1].encode('utf-8'))]
        if ret == "":
            return None
        return mapping_registry.parse_mapping_name("svn-" + ret)


class TdbLogCache(LogCache, CacheTable):

    def find_latest_change(self, path, revnum):
        """See LogCache.find_latest_change."""

        raise NotImplementedError(self.find_latest_change)

    def get_revision_paths(self, revnum):
        """See LogCache.get_revision_paths."""

        self.mutter("get-revision-paths %d", revnum)
        ret = {}
        try:
            db = bencode.bdecode(self.db[b"paths/%d" % revnum])
        except KeyError:
            raise KeyError("missing revision paths for %d" % revnum)
        for key, v in db.iteritems():
            try:
                (action, cp, cr, kind) = v
            except ValueError:
                (action, cp, cr) = v
                kind = NODE_UNKNOWN
            if cp == b"" and cr == -1:
                cp = None
            else:
                cp = cp.decode('utf-8')
            ret[key.decode('utf-8')] = (action, cp, cr, kind)
        return ret

    def insert_paths(self, rev, orig_paths, revprops, all_revprops):
        """See LogCache.insert_paths."""
        self.db.transaction_start()
        try:
            self.insert_revprops(rev, revprops, all_revprops)
            if orig_paths is None:
                orig_paths = {}
            new_paths = {}
            for p in orig_paths:
                v = orig_paths[p]
                copyfrom_path = v[1]
                if copyfrom_path is not None:
                    copyfrom_path = copyfrom_path.strip("/").encode('utf-8')
                else:
                    copyfrom_path = b""
                    assert orig_paths[p][2] == -1
                try:
                    kind = v[3]
                except IndexError:
                    kind = NODE_UNKNOWN
                new_paths[p.strip(u"/").encode('utf-8')] = (v[0], copyfrom_path, v[2], kind)
            self.db[b"paths/%d" % rev] = bencode.bencode(new_paths)
            min_revnum = self.min_revnum()
            if min_revnum is None:
                min_revnum = rev
            else:
                min_revnum = min(min_revnum, rev)
            self.db[b"log-min"] = str(min_revnum)
            self.db[b"log-last"] = str(max(self.max_revnum(), rev))
        except:
            self.db.transaction_cancel()
            raise
        else:
            self.db.transaction_commit()

    def drop_revprops(self, revnum):
        """See LogCache.drop_revprops."""

        self.db[b"revprops/%d" % revnum] = bencode.bencode({})

    def get_revprops(self, revnum):
        """See LogCache.get_revprops."""

        self.mutter("get-revision-properties %d", revnum)
        ret = bencode.bdecode(self.db["revprops/%d" % revnum])
        return (ret[0], bool(ret[1]))

    def insert_revprops(self, revision, revprops, all_revprops):
        """See LogCache.insert_revprops."""

        if revprops is None:
            revprops = {}
        revprops = {key.encode('utf-8'): value for (key, value) in revprops.items()}
        self.db[b"revprops/%d" % revision] = bencode.bencode((revprops, all_revprops))

    def max_revnum(self):
        """See LogCache.last_revnum."""

        try:
            return int(self.db[b"log-last"].decode())
        except KeyError:
            return 0

    def min_revnum(self):
        """See LogCache.min_revnum."""

        try:
            return int(self.db[b"log-min"].decode())
        except KeyError:
            return None


class TdbParentsCache(ParentsCache, CacheTable):

    def insert_parents(self, revid, parents):
        """See ParentsCache.insert_parents."""

        self.db[b"parents/%s" % revid] = b" ".join(parents)

    def lookup_parents(self, revid):
        """See ParentsCache.lookup_parents."""

        self.mutter("lookup-parents %s", revid)
        try:
            return tuple(
                [p for p in self.db[b"parents/%s" % revid].split(b" ")
                    if p != b""])
        except KeyError:
            return None


TDB_HASH_SIZE = 10000


class TdbRepositoryCache(RepositoryCache):
    """Object that provides a cache related to a particular UUID."""

    def __init__(self, uuid):
        super(TdbRepositoryCache, self).__init__(uuid)
        cache_file = os.path.join(self.create_cache_dir(), 'cache.tdb')
        assert isinstance(cache_file, str), "expected str, got: %r" % cache_file
        db = tdb_open(cache_file, TDB_HASH_SIZE, tdb.DEFAULT,
                os.O_RDWR|os.O_CREAT)
        try:
            assert int(db[b"version"].decode()) == CACHE_DB_VERSION
        except KeyError:
            db[b"version"] = b"%d" % CACHE_DB_VERSION
        self._db = db

    def open_revid_map(self):
        return TdbRevisionIdMapCache(self._db)

    def open_logwalker(self):
        return TdbLogCache(self._db)

    def open_revision_cache(self):
        return TdbRevisionInfoCache(self._db)

    def open_parents(self):
        return TdbParentsCache(self._db)
