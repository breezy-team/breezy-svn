# Copyright (C) 2006 Jelmer Vernooij <jelmer@samba.org>

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
"""Cache of the Subversion history log."""

from bzrlib import urlutils
from bzrlib.errors import NoSuchRevision
import bzrlib.ui as ui

import subvertpy

from bzrlib.plugins.svn import changes
from bzrlib.plugins.svn.cache import CacheTable
from bzrlib.plugins.svn.transport import SvnRaTransport

# Maximum number of extra revisions to fetch in caching logwalker
MAX_OVERHEAD_FETCH = 1000

class lazy_dict(object):
    def __init__(self, initial, create_fn, *args):
        self.initial = initial
        self.create_fn = create_fn
        self.args = args
        self.dict = None
        self.is_loaded = False

    def _ensure_init(self):
        if self.dict is None:
            self.dict = self.create_fn(*self.args)
            self.create_fn = None
            self.is_loaded = True

    def __len__(self):
        self._ensure_init()
        return len(self.dict)

    def __getitem__(self, key):
        if key in self.initial:
            return self.initial.__getitem__(key)
        self._ensure_init()
        return self.dict.__getitem__(key)

    def __delitem__(self, key):
        self._ensure_init()
        return self.dict.__delitem__(key)

    def __setitem__(self, key, value):
        self._ensure_init()
        return self.dict.__setitem__(key, value)

    def __contains__(self, key):
        if key in self.initial:
            return True
        self._ensure_init()
        return self.dict.__contains__(key)

    def get(self, key, default=None):
        if key in self.initial:
            return self.initial[key]
        self._ensure_init()
        return self.dict.get(key, default)

    def has_key(self, key):
        if self.initial.has_key(key):
            return True
        self._ensure_init()
        return self.dict.has_key(key)

    def keys(self):
        self._ensure_init()
        return self.dict.keys()

    def values(self):
        self._ensure_init()
        return self.dict.values()

    def items(self):
        self._ensure_init()
        return self.dict.items()

    def iteritems(self):
        self._ensure_init()
        return self.dict.iteritems()

    def __iter__(self):
        self._ensure_init()
        return self.dict.__iter__()

    def __repr__(self):
        self._ensure_init()
        return repr(self.dict)

    def __eq__(self, other):
        self._ensure_init()
        return self.dict.__eq__(other)

    def update(self, other):
        self._ensure_init()
        return self.dict.update(other)


class LogCache(CacheTable):
    """Log browser cache table manager. The methods of this class
    encapsulate the SQL commands used by CachingLogWalker to access
    the log cache tables."""
    
    def __init__(self, cache_db=None):
        CacheTable.__init__(self, cache_db)

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
        self._commit_interval = 5000
    
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
    
    def insert_path(self, rev, path, action, copyfrom_path=None, copyfrom_rev=-1):
        """Insert new history information into the cache.
        
        :param rev: Revision number of the revision
        :param path: Path that was changed
        :param action: Action on path (single-character)
        :param copyfrom_path: Optional original path this path was copied from.
        :param copyfrom_rev: Optional original rev this path was copied from.
        """
        assert action in ("A", "R", "D", "M")
        assert not path.startswith("/")
        self.cachedb.execute("replace into changed_path (rev, path, action, copyfrom_path, copyfrom_rev) values (?, ?, ?, ?, ?)", (rev, path, action, copyfrom_path, copyfrom_rev))

    def drop_revprops(self, revnum):
        self.cachedb.execute("update revinfo set all_revprops = 0 where rev = ?", (revnum,))

    def get_revprops(self, revnum):
        """Retrieve all the cached revision properties.

        :param revnum: Revision number of revision to retrieve revprops for.
        """
        result = self.cachedb.execute("select name, value from revprop where rev = ?", (revnum,))
        revprops = {}
        for k, v in result:
            revprops[k.encode("utf-8")] = v.encode("utf-8")
        return revprops

    def insert_revprop(self, rev, name, value):
        """Add a revision property.

        :param rev: Revision number of the revision.
        :param name: Name of the revision property.
        :param value: Contents of the revision property.
        """
        assert isinstance(name, str) and isinstance(value, str)
        self.cachedb.execute("replace into revprop (rev, name, value) values (?, ?, ?)", (rev, name.decode("utf-8", "replace"), value.decode("utf-8", "replace")))
    
    def insert_revprops(self, revision, revprops):
        if revprops is None:
            return
        for k, v in revprops.iteritems():
            self.insert_revprop(revision, k, v)

    def has_all_revprops(self, revnum):
        """Check whether all revprops for a revision have been cached.

        :param revnum: Revision number of the revision.
        """
        return self.cachedb.execute("select all_revprops from revinfo where rev = ?", (revnum,)).fetchone()[0]

    def insert_revinfo(self, rev, all_revprops):
        """Insert metadata for a revision.

        :param rev: Revision number of the revision.
        :param all_revprops: Whether or not the full revprops have been stored.
        """
        self.cachedb.execute("""
            replace into revinfo (rev, all_revprops) values (?, ?)
        """, (rev, all_revprops))

    def last_revnum(self):
        saved_revnum = self.cachedb.execute("SELECT MAX(rev) FROM revinfo").fetchone()[0]
        if saved_revnum is None:
            return 0
        return saved_revnum


class CachingLogWalker(CacheTable):
    """Subversion log browser."""
    def __init__(self, actual, cache_db=None):
        self.cache = LogCache(cache_db)
        self.actual = actual
        self.quick_revprops = actual.quick_revprops
        self._transport = actual._transport
        self.find_children = actual.find_children
        self.saved_revnum = self.cache.last_revnum()
        self._latest_revnum = None

    def find_latest_change(self, path, revnum):
        """Find latest revision that touched path.

        :param path: Path to check for changes
        :param revnum: First revision to check
        """
        assert isinstance(path, str)
        assert isinstance(revnum, int) and revnum >= 0
        self._fetch_revisions(revnum)

        self.mutter("latest change: %r:%r", path, revnum)
        revnum = self.cache.find_latest_change(path.strip("/"), revnum)
        if revnum is None and path == "":
            return 0
        
        return revnum

    def iter_changes(self, paths, from_revnum, to_revnum=0, limit=0, pb=None):
        """Return iterator over all the revisions between from_revnum and to_revnum named path or inside path.

        :param paths:    Paths to report about.
        :param from_revnum:  Start revision.
        :param to_revnum: End revision.
        :return: An iterator that yields tuples with (paths, revnum, revprops)
            where paths is a dictionary with all changes that happened 
            in revnum.
        """
        assert from_revnum >= 0 and to_revnum >= 0

        ascending = (to_revnum > from_revnum)

        revnum = from_revnum

        self.mutter("iter changes %r->%r (%r)", from_revnum, to_revnum, paths)

        if paths is None:
            path = ""
        else:
            assert len(paths) == 1
            path = paths[0].strip("/")

        assert from_revnum >= to_revnum or path == ""

        self._fetch_revisions(max(from_revnum, to_revnum), pb=pb)
        i = 0

        while ((not ascending and revnum >= to_revnum) or
               (ascending and revnum <= to_revnum)):
            if pb is not None:
                if ascending:
                    pb.update("determining changes", revnum, to_revnum)
                else:
                    pb.update("determining changes", from_revnum-revnum, from_revnum-to_revnum)
            assert revnum > 0 or path == "", "Inconsistent path,revnum: %r,%r" % (revnum, path)
            revpaths = self.get_revision_paths(revnum)

            if ascending:
                next = (path, revnum+1)
            else:
                next = changes.find_prev_location(revpaths, path, revnum)

            revprops = lazy_dict({}, self.revprop_list, revnum)

            if path == "" or changes.changes_path(revpaths, path, True):
                assert isinstance(revnum, int)
                yield (revpaths, revnum, revprops)
                i += 1
                if limit != 0 and i == limit:
                    break

            if next is None:
                break

            assert (ascending and next[1] > revnum) or \
                   (not ascending and next[1] < revnum)
            (path, revnum) = next
            assert isinstance(path, str)
            assert isinstance(revnum, int)

    def get_revision_paths(self, revnum):
        if revnum == 0:
            return changes.REV0_CHANGES
        self._fetch_revisions(revnum)

        return self.cache.get_revision_paths(revnum)

    def revprop_list(self, revnum):
        self.mutter('revprop list: %d' % revnum)
        self._fetch_revisions(revnum)

        if revnum > 0:
            has_all_revprops = self.cache.has_all_revprops(revnum)
            known_revprops = self.cache.get_revprops(revnum)
        else:
            has_all_revprops = False
            known_revprops = {}

        if has_all_revprops:
            return known_revprops

        return lazy_dict(known_revprops, self._caching_revprop_list, revnum)

    def _caching_revprop_list(self, revnum):
        revprops = self._transport.revprop_list(revnum)
        self.cache.insert_revprops(revnum, revprops)
        self.cache.insert_revinfo(revnum, True)
        self.cache.commit()
        return revprops

    def _fetch_revisions(self, to_revnum=None, pb=None):
        """Fetch information about all revisions in the remote repository
        until to_revnum.

        :param to_revnum: End of range to fetch information for
        """
        assert isinstance(self.saved_revnum, int)
        if to_revnum <= self.saved_revnum:
            return

        # Try to fetch log data in lumps, if possible.
        if self._latest_revnum is None:
            self._latest_revnum = self.actual._transport.get_latest_revnum()
        assert isinstance(self._latest_revnum, int)
        to_revnum = max(min(self._latest_revnum, to_revnum+MAX_OVERHEAD_FETCH), to_revnum)

        # Subversion 1.4 clients and servers can only deliver a limited set of revprops
        if self._transport.has_capability("log-revprops"):
            todo_revprops = None
        else:
            todo_revprops = ["svn:author", "svn:log", "svn:date"]

        def rcvr(orig_paths, revision, revprops, has_children=None):
            nested_pb.update('fetching svn revision info', revision, to_revnum)
            if orig_paths is None:
                orig_paths = {}
            for p in orig_paths:
                copyfrom_path = orig_paths[p][1]
                if copyfrom_path is not None:
                    copyfrom_path = copyfrom_path.strip("/")

                self.cache.insert_path(revision, p.strip("/"), orig_paths[p][0], copyfrom_path, orig_paths[p][2])
            self.cache.insert_revprops(revision, revprops)
            self.cache.insert_revinfo(revision, todo_revprops is None)
            self.saved_revnum = revision
            self.cache.commit_conditionally()

        self.mutter("get_log %d->%d", self.saved_revnum, to_revnum)

        if pb is None:
            nested_pb = ui.ui_factory.nested_progress_bar()
        else:
            nested_pb = pb

        try:
            nested_pb.update('fetching svn revision info', 0, to_revnum)
            try:
                self.actual._transport.get_log(rcvr, [""], self.saved_revnum, to_revnum, 0, True, True, False, todo_revprops)
            except subvertpy.SubversionException, (_, num):
                if num == subvertpy.ERR_FS_NO_SUCH_REVISION:
                    raise NoSuchRevision(branch=self, 
                        revision="Revision number %d" % to_revnum)
                raise
        finally:
            if pb is None:
                nested_pb.finished()
        self.cache.commit()


def strip_slashes(changed_paths):
    if changed_paths is None:
        return {}
    assert isinstance(changed_paths, dict)
    revpaths = {}
    for k, (action, copyfrom_path, copyfrom_rev) in changed_paths.iteritems():
        if copyfrom_path is None:
            copyfrom_path = None
        else:
            copyfrom_path = copyfrom_path.strip("/")
        revpaths[k.strip("/")] = (action, copyfrom_path, copyfrom_rev)
    return revpaths


class LogWalker(object):
    """Easy way to access the history of a Subversion repository."""
    def __init__(self, transport, limit=None):
        """Create a new instance.

        :param transport:   SvnRaTransport to use to access the repository.
        """
        assert isinstance(transport, SvnRaTransport)

        self._transport = transport
        self.quick_revprops = (self._transport.has_capability("log-revprops") == True)

    def find_latest_change(self, path, revnum):
        """Find latest revision that touched path.

        :param path: Path to check for changes
        :param revnum: First revision to check
        """
        assert isinstance(path, str)
        assert isinstance(revnum, int) and revnum >= 0

        try:
            return self._transport.iter_log([path], revnum, 0, 2, True, False, False, []).next()[1]
        except subvertpy.SubversionException, (_, num):
            if num == subvertpy.ERR_FS_NO_SUCH_REVISION:
                raise NoSuchRevision(branch=self, 
                    revision="Revision number %d" % revnum)
            if num == subvertpy.ERR_FS_NOT_FOUND:
                return None
            raise

    def revprop_list(self, revnum):
        return lazy_dict({}, self._transport.revprop_list, revnum)

    def iter_changes(self, paths, from_revnum, to_revnum=0, limit=0, pb=None):
        """Return iterator over all the revisions between revnum and 0 named path or inside path.

        :param paths:    Paths report about (in revnum)
        :param from_revnum:  Start revision.
        :param to_revnum: End revision.
        :return: An iterator that yields tuples with (paths, revnum, revprops)
            where paths is a dictionary with all changes that happened in revnum.
        """
        assert from_revnum >= 0 and to_revnum >= 0

        try:
            # Subversion 1.4 clients and servers can only deliver a limited set of revprops
            if self._transport.has_capability("log-revprops"):
                todo_revprops = None
            else:
                todo_revprops = ["svn:author", "svn:log", "svn:date"]

            iterator = self._transport.iter_log(paths, from_revnum, to_revnum, limit, 
                                                    True, False, False, revprops=todo_revprops)

            for (changed_paths, revnum, known_revprops, has_children) in iterator:
                if pb is not None:
                    pb.update("determining changes", from_revnum-revnum, from_revnum-to_revnum)
                if revnum == 0 and changed_paths is None:
                    revpaths = changes.REV0_CHANGES
                elif isinstance(changed_paths, dict):
                    revpaths = strip_slashes(changed_paths)
                else:
                    revpaths = {}
                if todo_revprops is None:
                    revprops = known_revprops
                else:
                    revprops = lazy_dict(known_revprops, self._transport.revprop_list, revnum)
                yield (revpaths, revnum, revprops)
        except subvertpy.SubversionException, (_, num):
            if num == subvertpy.ERR_FS_NO_SUCH_REVISION:
                raise NoSuchRevision(branch=self, 
                    revision="Revision number %d" % from_revnum)
            raise

    def get_revision_paths(self, revnum):
        """Obtain dictionary with all the changes in a particular revision.

        :param revnum: Subversion revision number
        :returns: dictionary with paths as keys and 
                  (action, copyfrom_path, copyfrom_rev) as values.
        """
        # To make the existing code happy:
        if revnum == 0:
            return changes.REV0_CHANGES

        try:
            return strip_slashes(
                self._transport.iter_log([""], revnum, revnum, 1, True, True, False, []).next()[0])
        except subvertpy.SubversionException, (_, num):
            if num == subvertpy.ERR_FS_NO_SUCH_REVISION:
                raise NoSuchRevision(branch=self, 
                    revision="Revision number %d" % revnum)
            raise

    def find_children(self, path, revnum, pb=None):
        """Find all children of path in revnum.

        :param path:  Path to check
        :param revnum:  Revision to check
        """
        assert isinstance(path, str), "invalid path"
        path = path.strip("/")
        conn = self._transport.connections.get(self._transport.get_svn_repos_root())
        results = []
        unchecked_dirs = set([path])
        num_checked = 0
        try:
            while len(unchecked_dirs) > 0:
                if pb is not None:
                    pb.update("listing branch contents", num_checked, num_checked+len(unchecked_dirs))
                nextp = unchecked_dirs.pop()
                num_checked += 1
                try:
                    dirents = conn.get_dir(nextp, revnum, subvertpy.ra.DIRENT_KIND)[0]
                except subvertpy.SubversionException, (_, num):
                    if num == subvertpy.ERR_FS_NOT_DIRECTORY:
                        continue
                    raise
                for k, v in dirents.iteritems():
                    childp = urlutils.join(nextp, k)
                    if v['kind'] == subvertpy.NODE_DIR:
                        unchecked_dirs.add(childp)
                    results.append(childp)
        finally:
            self._transport.connections.add(conn)
        return results
