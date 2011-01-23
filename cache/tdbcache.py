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

from bzrlib import (
    bencode,
    debug,
    errors,
    trace,
    )

from bzrlib.plugins.svn.cache import (
    RepositoryCache,
    cachedbs,
    )
from bzrlib.plugins.svn.mapping import (
    mapping_registry,
    )
from bzrlib.plugins.svn.revids import (
    RevisionIdMapCache,
    )
from bzrlib.plugins.svn.revmeta import (
    RevisionInfoCache,
    )
from bzrlib.plugins.svn.logwalker import (
    LogCache,
    )
from bzrlib.plugins.svn.parents import (
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

    def commit_conditionally(self):
        pass


class TdbRevisionIdMapCache(RevisionIdMapCache, CacheTable):

    def set_last_revnum_checked(self, layout, revnum):
        self.db["revidmap-last/%s" % str(layout)] = str(revnum)

    def last_revnum_checked(self, layout):
        try:
            return int(self.db["revidmap-last/%s" % str(layout)])
        except KeyError:
            return 0

    def lookup_revid(self, revid):
        self.mutter('lookup-revid %s', revid)
        try:
            (min_revnum, max_revnum, mapping_name, path) = self.db["native-revid/" + revid].split(" ", 3)
        except KeyError:
            raise errors.NoSuchRevision(self, revid)
        return (path, int(min_revnum), int(max_revnum), mapping_name)

    def lookup_branch_revnum(self, revnum, path, mapping):
        self.mutter('lookup-branch-revnum %s:%d', path, revnum)
        try:
            return self.db["foreign-revid/%d %s %s" % (revnum, getattr(mapping, "name", mapping), path)]
        except KeyError:
            return None

    def insert_revid(self, revid, branch, min_revnum, max_revnum, mapping):
        mappingname = getattr(mapping, "name", mapping)
        self.db["native-revid/" + revid] = "%d %d %s %s" % (min_revnum, max_revnum, mappingname, branch)
        if min_revnum == max_revnum:
            self.db["foreign-revid/%d %s %s" % (min_revnum, mappingname, branch)] = revid


class TdbRevisionInfoCache(RevisionInfoCache, CacheTable):

    def set_original_mapping(self, foreign_revid, original_mapping):
        if original_mapping is not None:
            orig_mapping_name = original_mapping.name
        else:
            orig_mapping_name = ""
        self.db["original-mapping/%d %s" % (foreign_revid[2], foreign_revid[1])] = orig_mapping_name

    def insert_revision(self, foreign_revid, mapping, (revno, revid, hidden),
            stored_lhs_parent_revid):
        if revid is None:
            revid = mapping.revision_id_foreign_to_bzr(foreign_revid)
        self.db["foreign-revid/%d %d %s %s" % (foreign_revid[2], foreign_revid[2], mapping.name, foreign_revid[1])] = revid
        basekey = "%d %s %s" % (foreign_revid[2], mapping.name, foreign_revid[1])
        assert not hidden or revno is None
        if revno is not None:
            self.db["revno/%s" % basekey] = "%d" % revno
        elif hidden:
            self.db["revno/%s" % basekey] = ""
        if stored_lhs_parent_revid is not None:
            self.db["lhs-parent-revid/%s" % basekey] = stored_lhs_parent_revid

    def get_revision(self, foreign_revid, mapping):
        self.mutter("get-revision %r,%r", foreign_revid, mapping)
        basekey = "%d %s %s" % (foreign_revid[2], mapping.name, foreign_revid[1])
        revid = self.db["foreign-revid/%d %d %s %s" % (foreign_revid[2], foreign_revid[2], mapping.name, foreign_revid[1])]
        stored_lhs_parent_revid = self.db.get("lhs-parent-revid/%s" % basekey)
        try:
            revno = int(self.db["revno/%s" % basekey])
            hidden = False
        except KeyError:
            revno = None
            hidden = False
        except ValueError: # empty string
            hidden = True
            revno = None
        return ((revno, revid, hidden), stored_lhs_parent_revid)

    def get_original_mapping(self, foreign_revid):
        self.mutter("get-original-mapping %r", foreign_revid)
        ret = self.db["original-mapping/%d %s" % (foreign_revid[2], foreign_revid[1])]
        if ret == "":
            return None
        return mapping_registry.parse_mapping_name("svn-" + ret)


class TdbLogCache(LogCache, CacheTable):

    def find_latest_change(self, path, revnum):
        raise NotImplementedError(self.find_latest_change)

    def get_revision_paths(self, revnum):
        self.mutter("get-revision-paths %d", revnum)
        ret = {}
        db = bencode.bdecode(self.db["paths/%d" % revnum])
        for key, v in db.iteritems():
            try:
                (action, cp, cr, kind) = v
            except ValueError:
                (action, cp, cr) = v
                kind = NODE_UNKNOWN
            if cp == "" and cr == -1:
                cp = None
            ret[key] = (action, cp, cr, kind)
        return ret

    def insert_paths(self, rev, orig_paths):
        if orig_paths is None:
            orig_paths = {}
        new_paths = {}
        for p in orig_paths:
            v = orig_paths[p]
            copyfrom_path = v[1]
            if copyfrom_path is not None:
                copyfrom_path = copyfrom_path.strip("/")
            else:
                copyfrom_path = ""
                assert orig_paths[p][2] == -1
            try:
                kind = v[3]
            except IndexError:
                kind = NODE_UNKNOWN
            new_paths[p.strip("/")] = (v[0], copyfrom_path, v[2], kind)
        self.db["paths/%d" % rev] = bencode.bencode(new_paths)
        self.db["log-last"] = "%d" % max(self.last_revnum(), rev)

    def drop_revprops(self, revnum):
        self.db["revprops/%d" % revnum] = bencode.bencode({})

    def get_revprops(self, revnum):
        self.mutter("get-revision-properties %d", revnum)
        ret = bencode.bdecode(self.db["revprops/%d" % revnum])
        return (ret[0], bool(ret[1]))

    def insert_revprops(self, revision, revprops, all_revprops):
        if revprops is None:
            revprops = {}
        self.db["revprops/%d" % revision] = bencode.bencode((revprops, all_revprops))
        min_revnum = self.min_revnum()
        if min_revnum is not None:
            min_revnum = min(min_revnum, revision)
        else:
            min_revnum = revision
        self.db["log-min"] = str(min_revnum)

    def last_revnum(self):
        try:
            return int(self.db["log-last"])
        except KeyError:
            return 0

    def min_revnum(self):
        try:
            return int(self.db["log-min"])
        except KeyError:
            return None

class TdbParentsCache(ParentsCache, CacheTable):

    def insert_parents(self, revid, parents):
        self.db["parents/%s" % revid] = " ".join(parents)

    def lookup_parents(self, revid):
        self.mutter("lookup-parents %s", revid)
        try:
            return tuple([p for p in self.db["parents/%s" % revid].split(" ") if p != ""])
        except KeyError:
            return None


TDB_HASH_SIZE = 10000


class TdbRepositoryCache(RepositoryCache):
    """Object that provides a cache related to a particular UUID."""

    def open_tdb(self):
        cache_file = os.path.join(self.create_cache_dir(), 'cache.tdb')
        assert isinstance(cache_file, str)
        if not cachedbs().has_key(cache_file):
            cachedbs()[cache_file] = tdb_open(cache_file, TDB_HASH_SIZE,
                tdb.DEFAULT, os.O_RDWR|os.O_CREAT)
        db = cachedbs()[cache_file]
        try:
            assert int(db["version"]) == CACHE_DB_VERSION
        except KeyError:
            db["version"] = str(CACHE_DB_VERSION)
        return db

    def open_revid_map(self):
        return TdbRevisionIdMapCache(self.open_tdb())

    def open_logwalker(self):
        return TdbLogCache(self.open_tdb())

    def open_revision_cache(self):
        return TdbRevisionInfoCache(self.open_tdb())

    def open_parents(self):
        return TdbParentsCache(self.open_tdb())
