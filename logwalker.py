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


"""Cache of the Subversion history log."""


import subvertpy

from bzrlib import (
    debug,
    ui,
    trace,
    urlutils,
    )
from bzrlib.errors import (
    NoSuchRevision,
    )

from bzrlib.plugins.svn import (
    changes,
    )
from bzrlib.plugins.svn.transport import (
    SvnRaTransport,
    )
from bzrlib.plugins.svn.util import (
    lazy_dict,
    )


# Maximum number of extra revisions to fetch in caching logwalker
MAX_OVERHEAD_FETCH = 1000

def iter_changes(paths, from_revnum, to_revnum, get_revision_paths, 
    revprop_list, limit=0):
    ascending = (to_revnum > from_revnum)

    revnum = from_revnum

    if paths is None:
        path = ""
    else:
        assert len(paths) == 1
        path = paths[0].strip("/")

    assert from_revnum >= to_revnum or path == ""

    i = 0

    while ((not ascending and revnum >= to_revnum) or
           (ascending and revnum <= to_revnum)):
        if not (revnum > 0 or path == ""):
            raise AssertionError("Inconsistent path,revnum: %r,%r" % (revnum, path))
        revpaths = get_revision_paths(revnum)

        if ascending:
            next = (path, revnum+1)
        else:
            next = changes.find_prev_location(revpaths, path, revnum)

        revprops = revprop_list(revnum)

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


class LogCache(object):
    """Log browser cache. """
    
    def find_latest_change(self, path, revnum):
        raise NotImplementedError(self.find_latest_change)

    def get_revision_paths(self, revnum):
        """Return all history information for a given revision number.
        
        :param revnum: Revision number of revision.
        """
        raise NotImplementedError(self.get_revision_paths)

    def insert_paths(self, rev, orig_paths):
        """Insert new history information into the cache.
        
        :param rev: Revision number of the revision
        :param orig_paths: SVN-style changes dictionary
        """
        raise NotImplementedError(self.insert_paths)
    
    def drop_revprops(self, revnum):
        raise NotImplementedError(self.drop_revprops)

    def get_revprops(self, revnum):
        """Retrieve all the cached revision properties.

        :param revnum: Revision number of revision to retrieve revprops for.
        """
        raise NotImplementedError(self.get_revprops)

    def insert_revprops(self, revision, revprops, all):
        raise NotImplementedError(self.insert_revprops)

    def last_revnum(self):
        raise NotImplementedError(self.last_revnum)


class CachingLogWalker(object):
    """Subversion log browser."""

    def __init__(self, actual, cache):
        self.cache = cache
        self.actual = actual
        self.quick_revprops = actual.quick_revprops
        self._transport = actual._transport
        self.saved_revnum = self.cache.last_revnum()
        self._latest_revnum = None

    def mutter(self, text, *args, **kwargs):
        if "logwalker" in debug.debug_flags:
            trace.mutter(text, *args, **kwargs)

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
        assert from_revnum >= 0 and to_revnum >= 0, "%r -> %r" % (from_revnum, to_revnum)
        self.mutter("iter changes %r->%r (%r)", from_revnum, to_revnum, paths)
        self._fetch_revisions(max(from_revnum, to_revnum), pb=pb)

        return iter(iter_changes(paths, from_revnum, to_revnum, 
            self.get_revision_paths, self.revprop_list, limit))

    def get_revision_paths(self, revnum):
        if revnum == 0:
            return changes.REV0_CHANGES
        self._fetch_revisions(revnum)

        return self.cache.get_revision_paths(revnum)

    def revprop_list(self, revnum):
        self.mutter('revprop list: %d' % revnum)
        self._fetch_revisions(revnum)

        if revnum > 0:
            (known_revprops, has_all_revprops) = self.cache.get_revprops(revnum)
        else:
            has_all_revprops = False
            known_revprops = {}

        if has_all_revprops:
            return known_revprops

        return lazy_dict(known_revprops, self._caching_revprop_list, revnum)

    def _caching_revprop_list(self, revnum):
        revprops = self._transport.revprop_list(revnum)
        self.cache.insert_revprops(revnum, revprops, True)
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
            self.cache.insert_paths(revision, orig_paths)
            self.cache.insert_revprops(revision, revprops, 
                                       todo_revprops is None)
            self.saved_revnum = revision

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
            self.cache.commit()
            if pb is None:
                nested_pb.finished()


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


class DictBasedLogWalker(object):

    def __init__(self, paths, revprops):
        self.paths = paths
        self.revprops = revprops

    def iter_changes(self, paths, from_revnum, to_revnum=0, limit=0, pb=None):
        return iter(iter_changes(paths, from_revnum, to_revnum, 
            self.get_revision_paths, self.revprop_list, limit))

    def get_revision_paths(self, revnum):
        if revnum == 0:
            return changes.REV0_CHANGES

        return self.paths.get(revnum, {})

    def revprop_list(self, revnum):
        if revnum == 0:
            return {}
        return self.revprops.get(revnum, {})

