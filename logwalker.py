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

from subvertpy import NODE_UNKNOWN


# Maximum number of extra revisions to fetch in caching logwalker
MAX_OVERHEAD_FETCH = 1000

def iter_all_changes(from_revnum, to_revnum, get_revision_paths,
    revprop_list, limit=0):
    revnum = from_revnum
    ascending = (to_revnum > from_revnum)
    i = 0
    while ((not ascending and revnum >= to_revnum) or
           (ascending and revnum <= to_revnum)):
        revpaths = get_revision_paths(revnum)

        # Report this revision if any affected paths are changed
        yield (revpaths, revnum, revprop_list(revnum))
        i += 1
        if limit != 0 and i == limit:
            break

        if ascending:
            revnum += 1
        else:
            revnum -= 1


def iter_changes(prefixes, from_revnum, to_revnum, get_revision_paths,
    revprop_list, limit=0):
    """Iter changes.

    :param prefix: Sequence of paths
    :param from_revnum: Start revision number
    :param to_revnum: End revision number
    :param get_revision_paths: Callback to retrieve the paths
        for a particular revision
    :param revprop_list: Callback to retrieve the revision properties
        for a particular revision
    :param limit: Maximum number of revisions to yield, 0 for all
    :return: Iterator over (revpaths, revnum, revprops)
    """
    revnum = from_revnum

    if prefixes is not None:
        prefixes = set(prefixes)

    if prefixes in (None, set([""]), set()):
        return iter_all_changes(from_revnum, to_revnum, get_revision_paths,
                revprop_list, limit)
    else:
        return iter_prefixes_changes(prefixes, from_revnum, to_revnum,
            get_revision_paths, revprop_list, limit)


def iter_prefixes_changes(from_prefixes, from_revnum, to_revnum,
    get_revision_paths, revprop_list, limit=0):
    assert type(from_prefixes) is set
    from_prefixes = set([p.strip("/") for p in from_prefixes])

    assert from_revnum >= to_revnum, "path: %r, %d >= %d" % (
            from_prefixes, from_revnum, to_revnum)

    revnum = from_revnum
    todo_prefixes = { revnum: from_prefixes }

    i = 0
    while revnum >= to_revnum:
        prefixes = todo_prefixes.pop(revnum)
        revpaths = get_revision_paths(revnum)

        # Report this revision if any affected paths are changed
        if any(changes.changes_path(revpaths, prefix, True) for prefix in prefixes):
            assert type(revnum) is int
            yield (revpaths, revnum, revprop_list(revnum))
            i += 1
            if limit != 0 and i == limit:
                break

        next = {}
        # Find the location of each prefix for the next iteration
        for prefix in prefixes:
            assert type(prefix) is str, "%r" % prefix
            try:
                (p, r) = changes.find_prev_location(revpaths, prefix,
                        revnum)
            except TypeError:
                pass # didn't exist here
            else:
                assert r < revnum
                todo_prefixes.setdefault(r, set()).add(p)

        if todo_prefixes == {}:
            break
        revnum = max(todo_prefixes)


class LogCache(object):
    """Log browser cache. """

    def find_latest_change(self, path, revnum):
        """Find the last revision in which a particular path
        was changed.

        :param path: Path to check
        :param revnum: Revision in which to check
        """
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


class CachingLogWalkerUpdater(object):
    """Function that can update a logwalker."""

    def __init__(self, logwalker, all_revprops):
        self.count = 0
        self.total = None
        self.pb = ui.ui_factory.nested_progress_bar()
        self.all_revprops = all_revprops
        self.logwalker = logwalker

    def __call__(self, orig_paths, revision, revprops, has_children=None):
        self.count += 1
        self.pb.update('fetching svn revision info', self.count, self.total)
        self.logwalker.cache.insert_paths(revision, orig_paths)
        self.logwalker.cache.insert_revprops(revision, revprops,
                                             self.all_revprops)
        self.logwalker.saved_revnum = max(revision, self.logwalker.saved_revnum)
        self.logwalker.saved_minrevnum = min(revision,
            self.logwalker.saved_minrevnum)

    def finished(self):
        self.pb.finished()


class CachingLogWalker(object):
    """Subversion log browser."""

    def __init__(self, actual, cache):
        self.cache = cache
        self.actual = actual
        self.quick_revprops = actual.quick_revprops
        self._transport = actual._transport
        self.saved_revnum = self.cache.last_revnum()
        self.saved_minrevnum = self.cache.min_revnum()
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
        try:
            revnum = self.cache.find_latest_change(path.strip("/"), revnum)
        except NotImplementedError:
            return self.actual.find_latest_change(path, revnum)
        if revnum is None and path == "":
            return 0

        return revnum

    def iter_changes(self, prefixes, from_revnum, to_revnum=0, limit=0, pb=None):
        """Return iterator over all the revisions between from_revnum and to_revnum named path or inside path.

        :param prefixes:    Prefixes of paths to report about
        :param from_revnum:  Start revision.
        :param to_revnum: End revision.
        :return: An iterator that yields tuples with (paths, revnum, revprops)
            where paths is a dictionary with all changes that happened
            in revnum.
        """
        assert from_revnum >= 0 and to_revnum >= 0, "%r -> %r" % (from_revnum, to_revnum)
        self.mutter("iter changes %r->%r (%r)", from_revnum, to_revnum, prefixes)
        self._fetch_revisions(max(from_revnum, to_revnum), pb=pb)
        return iter(iter_changes(prefixes, from_revnum, to_revnum,
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

    def _fetch_revisions(self, to_revnum, pb=None):
        """Fetch information about all revisions in the remote repository
        until to_revnum.

        :param to_revnum: End of range to fetch information for
        """
        assert isinstance(self.saved_revnum, int)

        if to_revnum <= self.saved_revnum and self.saved_minrevnum == 0:
            return

        # Try to fetch log data in lumps, if possible.
        if self._latest_revnum is None:
            self._latest_revnum = self.actual._transport.get_latest_revnum()
        assert isinstance(self._latest_revnum, int)
        to_revnum = max(min(self._latest_revnum, to_revnum+MAX_OVERHEAD_FETCH),
                        to_revnum)

        if to_revnum <= self.saved_revnum:
            return

        # Subversion 1.4 clients and servers can only deliver a limited set of
        # revprops
        if self._transport.has_capability("log-revprops"):
            todo_revprops = None
        else:
            todo_revprops = ["svn:author", "svn:log", "svn:date"]

        rcvr = CachingLogWalkerUpdater(self, todo_revprops is None)
        try:
            try:
                # The get_log bounds are inclusive at both ends, so the total
                # number of revisions requested is A - B.
                rcvr.total = to_revnum - self.saved_revnum
                # Try to keep the cache consistent by closing any holes early
                # in the history
                if self.saved_minrevnum:
                    rcvr.total += self.saved_minrevnum
                    self.mutter("get_log %d->%d", self.saved_minrevnum - 1, 0)
                    self.actual._transport.get_log(rcvr, [""],
                        self.saved_minrevnum - 1, 0, 0, True, True, False,
                        todo_revprops)
                self.mutter("get_log %d->%d", to_revnum, self.saved_revnum+1)
                self.actual._transport.get_log(rcvr, [""], to_revnum,
                    self.saved_revnum+1, 0, True, True, False, todo_revprops)
            except subvertpy.SubversionException, (_, num):
                if num == subvertpy.ERR_FS_NO_SUCH_REVISION:
                    raise NoSuchRevision(branch=self,
                        revision="Revision number %d" % to_revnum)
                raise
        finally:
            rcvr.finished()
            self.cache.commit()


def strip_slashes(changed_paths):
    """Strip the leading and trailing slashes in paths.

    :param changed_paths: Changed paths dictionary
    :return: Update paths dictionary
    """
    if changed_paths is None:
        return {}
    assert type(changed_paths) is dict
    revpaths = {}
    for k, v in changed_paths.iteritems():
        try:
            kind = v[3]
        except IndexError:
            kind = NODE_UNKNOWN
        if v[1] is None:
            copyfrom_path = None
        else:
            copyfrom_path = v[1].strip("/")
        revpaths[k.strip("/")] = (v[0], copyfrom_path, v[2], kind)
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
            return self._transport.iter_log([path], revnum, 0, 2, True, False,
                    False, []).next()[1]
        except subvertpy.SubversionException, (_, num):
            if num == subvertpy.ERR_FS_NO_SUCH_REVISION:
                raise NoSuchRevision(branch=self,
                    revision="Revision number %d" % revnum)
            if num == subvertpy.ERR_FS_NOT_FOUND:
                return None
            raise

    def revprop_list(self, revnum):
        return lazy_dict({}, self._transport.revprop_list, revnum)

    def iter_changes(self, prefixes, from_revnum, to_revnum=0, limit=0, pb=None):
        """Return iterator over all the revisions between revnum and 0 named
        path or inside path.

        :param prefixes:    Prefixes to report about (in from_revnum)
        :param from_revnum:  Start revision.
        :param to_revnum: End revision.
        :return: An iterator that yields tuples with (paths, revnum, revprops)
            where paths is a dictionary with all changes that happened in revnum.
        """
        assert from_revnum >= 0 and to_revnum >= 0

        # Subversion 1.4 clients and servers can only deliver a limited set of revprops
        if self._transport.has_capability("log-revprops"):
            todo_revprops = None
        else:
            todo_revprops = ["svn:author", "svn:log", "svn:date"]

        try:
            iterator = self._transport.iter_log(prefixes, from_revnum,
                to_revnum, limit, True, False, False, revprops=todo_revprops)

            for (changed_paths, revnum, known_revprops, has_children) in iterator:
                if revnum == 0 and changed_paths is None:
                    revpaths = changes.REV0_CHANGES
                elif isinstance(changed_paths, dict):
                    revpaths = strip_slashes(changed_paths)
                else:
                    revpaths = {}
                if todo_revprops is None:
                    revprops = known_revprops
                    if revprops is None:
                        revprops = {}
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

    def iter_changes(self, prefixes, from_revnum, to_revnum=0, limit=0,
            pb=None):
        return iter(iter_changes(prefixes, from_revnum, to_revnum,
            self.get_revision_paths, self.revprop_list, limit))

    def get_revision_paths(self, revnum):
        if revnum == 0:
            return changes.REV0_CHANGES

        return self.paths.get(revnum, {})

    def revprop_list(self, revnum):
        if revnum == 0:
            return {}
        return self.revprops.get(revnum, {})

