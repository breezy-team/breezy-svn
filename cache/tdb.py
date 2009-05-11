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

from bzrlib import (
    debug,
    errors,
    osutils,
    )
from bzrlib.util.bencode import (
    bdecode,
    bencode,
    )

from bzrlib.plugins.svn.cache import (
    RepositoryCache,
    cachedbs,
    )
from bzrlib.plugins.svn.mapping import (
    mapping_registry,
    )


CACHE_DB_VERSION = 1


class CacheTable(object):

    def __init__(self, db):
        self.db = db


class RevisionIdMapCache(CacheTable):
    """Revision id mapping store. 

    Stores mapping from revid -> (path, revnum, mapping)
    """

    def set_last_revnum_checked(self, layout, revnum):
        """Remember the latest revision number that has been checked
        for a particular layout.

        :param layout: Repository layout.
        :param revnum: Revision number.
        """
        self.db["revidmap-last/%s" % str(layout)] = str(revnum)

    def last_revnum_checked(self, layout):
        """Retrieve the latest revision number that has been checked 
        for revision ids for a particular layout.

        :param layout: Repository layout.
        :return: Last revision number checked or 0.
        """
        try:
            return int(self.db["revidmap-last/%s" % str(layout)])
        except KeyError:
            return 0
    
    def lookup_revid(self, revid):
        """Lookup the details for a particular revision id.

        :param revid: Revision id.
        :return: Tuple with path inside repository, minimum revision number, maximum revision number and 
            mapping.
        """
        (min_revnum, max_revnum, mapping_name, path) = self.db["native-revid/%s" % revid].split(" ", 4)
        return (path, int(min_revnum), int(max_revnum), mapping_registry.parse_mapping_name("svn-" + mapping_name))

    def lookup_branch_revnum(self, revnum, path, mapping):
        """Lookup a revision by revision number, branch path and mapping.

        :param revnum: Subversion revision number.
        :param path: Subversion branch path.
        :param mapping: Mapping
        """
        try:
            return self.db["foreign-revid/%d %s %s" % (revnum, mapping.name, path)]
        except KeyError:
            return None

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
        self.db["native-revid/%s" % revid] = "%d %d %s %s" % (min_revnum, max_revnum, mapping.name, branch)
        if min_revnum == max_revnum:
            self.db["foreign-revid/%d %s %s" % (min_revnum, mapping.name, branch)] = revid


class RevisionInfoCache(CacheTable):

    def insert_revision(self, foreign_revid, mapping, revid, revno, hidden, 
            original_mapping, stored_lhs_parent_revid):
        """Insert a revision to the cache.

        :param foreign_revid: Foreign revision id
        :param mapping: Mapping used
        :param revid: Revision id
        :param revno: Revision number
        :param hidden: Whether revision is hidden
        :param original_mapping: Original mapping used
        :param stored_lhs_parent_revid: Stored lhs parent revision
        """
        if orig_mapping is not None:
            self.db["original-mapping/%d %s" % (foreign_revid[2], foreign_revid[1])] = original_mapping.name
        basekey = "%d %s %s" % (foreign_revid[2], mapping.name, foreign_revid[1])
        self.db["revno/%s" % basekey] = str(revno)
        self.db["hidden/%s" % basekey] = str(hidden)
        if stored_lhs_parent_revid:
            self.db["lhs-parent-revid/%s" % basekey] = stored_lhs_parent_revid

    def get_revision(self, foreign_revid, mapping):
        """Get the revision metadata info for a (foreign_revid, mapping) tuple.

        :param foreign_revid: Foreign revision id
        :param mapping: Mapping
        :return: Tuple with revid, stored revno, hidden, original_mapping, 
            stored_lhs_parent_revid
        """
        basekey = "%d %s %s" % (foreign_revid[2], mapping.name, foreign_revid[1])
        revid = self.db["foreign-revid/%d %d %s %s" % (foreign_revid[2], foreign_revid[2], mapping.name, foreign_revid[1])]
        revno = int(self.db["revno/%s" % basekey])
        hidden = bool(self.db["hidden/%s" % basekey])
        original_mapping = self.get_original_mapping(foreign_revid)
        stored_lhs_parent_revid = self.db.get("lhs-parent-revid/%s" % basekey)
        return (revid, revno, hidden, original_mapping, stored_lhs_parent_revid)

    def get_original_mapping(self, foreign_revid):
        """Find the original mapping for a revision.

        :param foreign_revid: Foreign revision id
        :return: Mapping object or None
        """
        return mapping_registry.parse_mapping_name("svn-" + self.db["original-mapping/%d %s" % (foreign_revid[2], foreign_revid[1]))


class LogCache(CacheTable):
    """Log browser cache table manager. The methods of this class
    encapsulate the SQL commands used by CachingLogWalker to access
    the log cache tables."""
    
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
        db = bdecode(self.db["paths/%d" % revnum])
        for key, (action, cp, cr) in db.iteritems():
            if cp == "" and cr == -1:
                cp = None
            ret[key] = (action, cp, cr)
        return ret

    def insert_paths(self, rev, orig_paths):
        """Insert new history information into the cache.
        
        :param rev: Revision number of the revision
        :param orig_paths: SVN-style changes dictionary
        """
        if orig_paths is None:
            orig_paths = {}
        text = ""
        new_paths = {}
        for p in orig_paths:
            copyfrom_path = orig_paths[p][1]
            if copyfrom_path is not None:
                copyfrom_path = copyfrom_path.strip("/").decode("utf-8")
            else:
                copyfrom_path = ""
                assert orig_paths[p][2] == -1
            new_paths[p.strip("/")] = (orig_paths[p][0], copyfrom_path, orig_paths[p][2])
        self.db["paths/%d" % revnum] = bencode(new_paths)
    
    def drop_revprops(self, revnum):
        self.db["revprops/%d" % revnum] = bencode({})

    def get_revprops(self, revnum):
        """Retrieve all the cached revision properties.

        :param revnum: Revision number of revision to retrieve revprops for.
        """
        return bdecode(self.db["revprops/%d" % revnum])

    def insert_revprops(self, revision, revprops):
        self.db["revprops/%d" % revision] = bencode(revprops)

    def has_all_revprops(self, revnum):
        """Check whether all revprops for a revision have been cached.

        :param revnum: Revision number of the revision.
        """
        try:
            return bool(self.db["revprops-complete/%d" % revnum])
        except KeyError:
            return False

    def insert_revinfo(self, rev, all_revprops):
        """Insert metadata for a revision.

        :param rev: Revision number of the revision.
        :param all_revprops: Whether or not the full revprops have been stored.
        """
        self.db["revprops-complete/%d" % rev] = str(all_revprops)

    def last_revnum(self):
        saved_revnum = self.cachedb.execute("SELECT MAX(rev) FROM revinfo").fetchone()[0]
        if saved_revnum is None:
            return 0
        return saved_revnum


class ParentsCache(CacheTable):

    def insert_parents(self, revid, parents):
        self.db["parents/%s" % revid] = " ".join(parents)

    def lookup_parents(self, revid):
        try:
            return tuple(self.db["parents/%s" % revid].split(" "))
        except KeyError:
            return None


class TdbRepositoryCache(RepositoryCache):
    """Object that provides a cache related to a particular UUID."""

    def open_tdb(self):
        cache_file = os.path.join(self.create_cache_dir(), 'cache.tdb')
        assert isinstance(cache_file, str)
        if not cachedbs().has_key(cache_file):
            cachedbs()[cache_file] = tdb.open(cache_file, 0, tdb.DEFAULT, os.O_RDWR|os.O_CREAT)
        db = cachedbs()[cache_file]
        if not "version" in db:
            db["version"] = CACHE_DB_VERSION
        else:
            assert int(db["version"]) == CACHE_DB_VERSION
        return db

    def open_revid_map(self):
        return RevisionIdMapCache(self.open_tdb())

    def open_logwalker(self):
        return LogCache(self.open_tdb())

    def open_revision_cache(self):
        return RevisionInfoCache(self.open_tdb())

    def open_parents(self):
        return ParentsCache(self.open_tdb())
