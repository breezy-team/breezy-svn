# Copyright (C) 2005-2011 Jelmer Vernooij <jelmer@samba.org>

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Subversion Meta-Revisions. This is where all the magic happens. """

from __future__ import absolute_import

import bisect
from collections import defaultdict

from subvertpy import (
    NODE_DIR,
    )

from breezy.sixish import (
    text_type,
    )

from breezy.plugins.svn import (
    changes,
    errors as svn_errors,
    )

from breezy.plugins.svn.util import (
    ListBuildingIterator,
    )


class MetaHistoryIncomplete(Exception):
    """No revision metadata branch: %(msg)s"""

    def __init__(self, msg):
        self.msg = msg


def filter_revisions(it):
    """Filter out all revisions out of a stream with changes."""
    for kind, rev in it:
        if kind == "revision":
            yield rev


def restrict_prefixes(prefixes, prefix):
    """Trim a list of prefixes down as much as possible.

    :param prefixes: List of prefixes to check
    :param prefix: Prefix to restrict to
    :return: Set with the remaining prefixes
    """
    ret = set()
    for p in prefixes:
        if prefix == u"" or p == prefix or p.startswith(prefix+u"/"):
            ret.add(p)
        elif prefix.startswith(p+u"/") or p == u"":
            ret.add(prefix)
    return ret


class MetaRevision(object):
    """Object describing a revision in a Subversion repository.

    Tries to be as lazy as possible - data is not retrieved or calculated
    from other known data before contacting the Subversions server.
    """

    __slots__ = ('branch_path', 'revnum', 'uuid',
                 '_paths', '_revprops', '_graph', '_lhs_parent_known',
                 '_lhs_parent', 'children', 'metaiterators')

    def __init__(self, graph, uuid, branch_path, revnum, paths=None,
            revprops=None):
        self._lhs_parent_known = False
        self._lhs_parent = None
        if not isinstance(branch_path, text_type):
            raise TypeError(branch_path)
        self.branch_path = branch_path
        self.revnum = revnum
        self.uuid = uuid
        self._graph = graph
        self._paths = paths
        self._revprops = revprops
        self.children = set()
        self.metaiterators = []

    def __eq__(self, other):
        return (isinstance(other, MetaRevision) and
                self.revnum == other.revnum and
                self.branch_path == other.branch_path and
                self.uuid == other.uuid)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __cmp__(self, other):
        return cmp((self.uuid, self.revnum, self.branch_path),
                   (other.uuid, other.revnum, other.branch_path))

    def __repr__(self):
        return "<%s for revision %d, path %s in repository %r>" % (
            self.__class__.__name__, self.revnum, self.branch_path, self.uuid)

    def refresh_revprops(self):
        self._revprops = self._graph._log._transport.revprop_list(self.revnum)

    @property
    def paths(self):
        """Fetch the changed paths dictionary for this revision.
        """
        if self._paths is None:
            self._paths = self._graph._log.get_revision_paths(self.revnum)
        return self._paths

    @property
    def revprops(self):
        """Get the revision properties set on the revision."""
        if self._revprops is None:
            self._revprops = self._graph._log.revprop_list(self.revnum)
        return self._revprops

    def knows_revprops(self):
        """Check whether all revision properties can be cheaply retrieved."""
        if self._revprops is None:
            return False
        revprops = self.revprops
        return isinstance(revprops, dict) or revprops.is_loaded

    def changes_outside_root(self):
        """Check if there are any changes in this svn revision not under
        this metarev's root."""
        return changes.changes_outside_branch_path(self.branch_path,
            self.paths.keys())

    def is_changes_root(self):
        """Check whether this revisions root is the root of the changes
        in this svn revision.

        This is a requirement for revisions pushed with bzr-svn using
        file properties.
        """
        return changes.changes_root(self.paths.keys()) == self.branch_path

    def __hash__(self):
        return hash((self.__class__, self.get_foreign_revid()))

    def get_foreign_revid(self):
        """Return the foreign revision id for this revision.

        :return: Tuple with uuid, branch path and revision number.
        """
        return (self.uuid, self.branch_path, self.revnum)

    def _set_lhs_parent(self, parent_metarev):
        """Set the left-hand side parent.

        :note: Should only be called once, later callers are only allowed
            to specify the same lhs parent.
        """
        assert parent_metarev is None or isinstance(parent_metarev, MetaRevision)
        if (self._lhs_parent_known and
            self._lhs_parent != parent_metarev):
            raise AssertionError("Tried registering %r as parent while %r already was parent for %r" % (
                parent_metarev, self._lhs_parent, self))
        self._lhs_parent_known = True
        self._lhs_parent = parent_metarev
        if parent_metarev is not None:
            parent_metarev.children.add(self)

    def get_lhs_parent(self):
        """Find the left hand side parent of this revision.
        """
        if self._lhs_parent_known:
            return self._lhs_parent
        for metaiterator in self.metaiterators:
            # Perhaps the metaiterator already has the parent?
            try:
                self._lhs_parent = metaiterator.get_lhs_parent(self)
                self._lhs_parent_known = True
                return self._lhs_parent
            except StopIteration:
                self._lhs_parent = None
                self._lhs_parent_known = True
                return self._lhs_parent
            except MetaHistoryIncomplete:
                pass
        iterator = self._graph.iter_reverse_branch_changes(
                self.branch_path, self.revnum, to_revnum=0, limit=0)
        firstrevmeta = iterator.next()
        assert self == firstrevmeta, "Expected %r got %r" % (self, firstrevmeta)
        try:
            self._lhs_parent = iterator.next()
        except StopIteration:
            self._lhs_parent = None
        self._lhs_parent_known = True
        return self._lhs_parent


class RevisionMetadataBranch(object):
    """Describes a Bazaar-like branch in a Subversion repository."""

    __slots__ = ('_revs', '_revnums', '_history_limit', '_graph',
                 '_history_iter')

    def __init__(self, history_iter, graph, history_limit=None):
        self._revs = []
        self._revnums = []
        self._history_iter = history_iter
        self._history_limit = history_limit
        self._graph = graph

    def _get_next(self):
        (bp, paths, revnum, revprops) = self._history_iter.next()
        ret = MetaRevision(self._graph,
            self._graph._log._transport.get_uuid(), bp, revnum, paths=paths,
            revprops=revprops)
        if self._revs:
            self._revs[-1]._set_lhs_parent(ret)
        self.append(ret)
        return ret

    def __eq__(self, other):
        return (type(self) == type(other) and
                self._history_limit == other._history_limit and
                ((self._revs == [] and other._revs == []) or
                 (self._revs != [] and other._revs != [] and
                  self._revs[0] == other._revs[0])))

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        if len(self._revs) == 0:
            return hash((type(self), self._history_limit))
        return hash((type(self), self._history_limit, self._revs[0]))

    def __repr__(self):
        if len(self._revs) == 0:
            return "<Empty %s>" % (self.__class__.__name__)
        else:
            return "<%s starting at %s revision %d>" % (
                self.__class__.__name__, self._revs[0].branch_path,
                self._revs[0].revnum)

    def __iter__(self):
        return ListBuildingIterator(self._revs, self.next)

    def fetch_until(self, revnum):
        """Fetch at least all revisions until revnum."""
        while len(self._revnums) == 0 or self._revnums[0] > revnum:
            try:
                self.next()
            except MetaHistoryIncomplete:
                return
            except StopIteration:
                return

    def next(self):
        if self._history_limit and len(self._revs) >= self._history_limit:
            raise MetaHistoryIncomplete("Limited to %d revisions" % self._history_limit)
        return self._get_next()

    def _index(self, metarev):
        """Find the location of a metarev object, counted from the
        most recent revision."""
        i = len(self._revs) - bisect.bisect_right(self._revnums, metarev.revnum)
        assert i == len(self._revs) or self._revs[i] == metarev
        return i

    def get_lhs_parent(self, metarev):
        """Find the left hand side of a revision using revision metadata.

        :note: Will return None if no LHS parent can be found, this
            doesn't necessarily mean there is no LHS parent.
        """
        assert isinstance(metarev, MetaRevision)
        i = self._index(metarev)
        try:
            return self._revs[i+1]
        except IndexError:
            return self.next()

    def append(self, metarev):
        """Append a revision metadata object to this branch."""
        assert len(self._revs) == 0 or self._revs[-1].revnum > metarev.revnum,\
                "%r > %r" % (self._revs[-1].revnum, metarev.revnum)
        self._revs.append(metarev)
        self._revnums.insert(0, metarev.revnum)


class RevisionMetadataBrowser(object):
    """Object can that can iterate over the meta revisions in a
    revision range, under a specific path.

    """

    def __init__(self, prefixes, from_revnum, to_revnum, layout, graph,
            project=None, pb=None):
        """Create a new browser

        :param prefixes: Prefixes of branches over which to iterate
        :param from_revnum: Start side of revnum
        :param to_revnum: Stop side of revnum
        :param layout: Layout
        :param graph: Object with get_revision and iter_changes functions
        :param project: Project name
        :param pb: Optional progress bar
        """
        if prefixes in ([""], None):
            self.from_prefixes = None
            self._pending_prefixes = None
            self._prefixes = None
        else:
            self.from_prefixes = [prefix.strip("/") for prefix in prefixes]
            self._pending_prefixes = defaultdict(set)
            self._prefixes = set(self.from_prefixes)
        self.from_revnum = from_revnum
        self.to_revnum = to_revnum
        self._last_revnum = None
        self.layout = layout
        # Two-dimensional dictionary for each set of revision meta
        # branches that exist *after* a revision
        self._pending_ancestors = defaultdict(lambda: defaultdict(set))
        self._ancestors = defaultdict(set)
        self._unusual = set()
        self._unusual_history = defaultdict(set)
        self._graph = graph
        self._actions = []
        self._iter_log = self._graph._log.iter_changes(self.from_prefixes,
                self.from_revnum, self.to_revnum, pb=pb)
        self._project = project
        self._pb = pb
        self._iter = self.do()

    def __iter__(self):
        return ListBuildingIterator(self._actions, self.next)

    def __repr__(self):
        return "<%s from %d to %d, layout: %r>" % (
                self.__class__.__name__, self.from_revnum, self.to_revnum,
                self.layout)

    def __eq__(self, other):
        return (type(self) == type(other) and
                self.from_revnum == other.from_revnum and
                self.to_revnum == other.to_revnum and
                self.from_prefixes == other.from_prefixes and
                self.layout == other.layout)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        if self.from_prefixes is None:
            prefixes = None
        else:
            prefixes = tuple(self.from_prefixes)
        return hash((type(self), self.from_revnum, self.to_revnum, prefixes,
            hash(self.layout)))

    def get_lhs_parent(self, metarev):
        """Find the left hand side parent of a metarevision object.

        :param metarev: RevisionMetadata object
        :return: MetaRevision object for the left hand side parent
        """
        assert isinstance(metarev, MetaRevision)
        while not metarev._lhs_parent_known:
            try:
                self.next()
            except StopIteration:
                if self.to_revnum > 0 or self.from_prefixes:
                    raise MetaHistoryIncomplete(
                        "Reached revision 0 or outside of prefixes.")
                raise AssertionError("Unable to find lhs parent for %r" % metarev)
        return metarev._lhs_parent

    def fetch_until(self, revnum):
        """Fetch at least all revisions until revnum.

        :param revnum: Revision until which to fetch.
        """
        try:
            while self._last_revnum is None or self._last_revnum > revnum:
                self.next()
        except StopIteration:
            return

    def next(self):
        """Return the next action.

        :return: Tuple with action string and object.
        """
        ret = self._iter.next()
        self._actions.append(ret)
        return ret

    def do(self):
        """Yield revisions and deleted branches.

        This is where the *real* magic happens.
        """
        assert self.from_revnum >= self.to_revnum
        count = self.from_revnum-self.to_revnum
        for (paths, revnum, revprops) in self._iter_log:
            assert revnum <= self.from_revnum
            if self._pb:
                self._pb.update("discovering revisions",
                        abs(self.from_revnum-revnum), count)

            if self._last_revnum is not None:
                # Import all metabranches_history where key > revnum
                for x in xrange(self._last_revnum, revnum-1, -1):
                    for bp in self._pending_ancestors[x].keys():
                        self._ancestors[bp].update(self._pending_ancestors[x][bp])
                    del self._pending_ancestors[x]
                    self._unusual.update(self._unusual_history[x])
                    del self._unusual_history[x]
                    if self._prefixes is not None:
                        self._prefixes.update(self._pending_prefixes[x])

            # Eliminate anything that's not under prefixes/
            if self._prefixes is not None:
                for bp in self._ancestors.keys():
                    if not changes.under_prefixes(bp, self._prefixes):
                        del self._ancestors[bp]
                        self._unusual.discard(bp)

            changed_bps = set()
            deletes = []

            if paths == {}:
                paths = {"": ("M", None, -1, NODE_DIR)}

            # Find out what branches have changed
            for p in sorted(paths):
                if (self._prefixes is not None and
                    not changes.under_prefixes(p, self._prefixes)):
                    continue
                action = paths[p][0]
                try:
                    (_, bp, ip) = self.layout.split_project_path(p, self._project)
                except svn_errors.NotSvnBranchPath:
                    pass
                else:
                    # Did something change inside a branch?
                    if action != 'D' or ip != "":
                        changed_bps.add(bp)
                for u in self._unusual:
                    if (p == u and not action in ('D', 'R')) or changes.path_is_child(u, p):
                        changed_bps.add(u)
                if action in ('R', 'D') and (
                    self.layout.is_branch_or_tag(p, self._project) or
                    self.layout.is_branch_or_tag_parent(p, self._project)):
                    deletes.append(p)

            # Mention deletes
            for d in deletes:
                yield ("delete", (p, revnum))

            # Dictionary with the last revision known for each branch
            # Report the new revisions
            for bp in changed_bps:
                metarev = MetaRevision(self._graph,
                    self._graph._log._transport.get_uuid(), bp, revnum, paths=paths,
                    revprops=revprops)
                assert metarev is not None
                for c in self._ancestors[bp]:
                    c._set_lhs_parent(metarev)
                del self._ancestors[bp]
                self._ancestors[bp] = set([metarev])
                # If this branch was started here, terminate it
                if (bp in paths and paths[bp][0] in ('A', 'R') and
                    paths[bp][1] is None):
                    metarev._set_lhs_parent(None)
                    del self._ancestors[bp]
                yield "revision", metarev

            # Apply reverse renames and the like for the next round
            for new_name, old_name, old_rev, kind in changes.apply_reverse_changes(
                self._ancestors.keys(), paths):
                self._unusual.discard(new_name)
                if old_name is None:
                    # Didn't exist previously, mark as beginning and remove.
                    for rev in self._ancestors[new_name]:
                        if rev.branch_path != new_name:
                            raise AssertionError("Revision %d has invalid branch path %s, expected %s" % (revnum, rev.branch_path, new_name))
                        rev._set_lhs_parent(None)
                    del self._ancestors[new_name]
                else:
                    # was originated somewhere else
                    data = self._ancestors[new_name]
                    del self._ancestors[new_name]
                    self._pending_ancestors[old_rev][old_name].update(data)
                    if not self.layout.is_branch_or_tag(old_name, self._project):
                        self._unusual_history[old_rev].add(old_name)

            # Update the prefixes, if necessary
            if self._prefixes:
                for new_name, old_name, old_rev, kind in changes.apply_reverse_changes(
                    self._prefixes, paths):
                    if old_name is None:
                        # Didn't exist previously, terminate prefix
                        self._prefixes.discard(new_name)
                        if len(self._prefixes) == 0:
                            return
                    else:
                        self._prefixes.discard(new_name)
                        self._pending_prefixes[old_rev].add(old_name)

            self._last_revnum = revnum


class MetaRevisionGraph(object):
    """Meta revision graph."""

    def __init__(self, logwalker):
        self._log = logwalker
        self._open_metaiterators = []

    def iter_changes(self, branch_path, from_revnum, to_revnum, pb=None,
                     limit=0):
        """Iterate over all revisions backwards.

        :return: iterator that returns tuples with branch path,
            changed paths, revision number, changed file properties and
        """
        assert from_revnum >= to_revnum
        assert isinstance(branch_path, text_type)

        bp = branch_path
        i = 0

        # Limit can't be passed on directly to LogWalker.iter_changes()
        # because we're skipping some revs
        # TODO: Rather than fetching everything if limit == 2, maybe just
        # set specify an extra X revs just to be sure?
        for (paths, revnum, revprops) in self._log.iter_changes([branch_path],
            from_revnum, to_revnum, pb=pb):
            assert bp is not None
            next = changes.find_prev_location(paths, bp, revnum)
            assert revnum > 0 or bp == u""

            if changes.changes_path(paths, bp, False):
                yield (bp, paths, revnum, revprops)
                i += 1

            if next is None:
                bp = None
            else:
                bp = next[0]

    def add_metaiterator(self, iterator):
        self._open_metaiterators.append(iterator)

    def finish_metaiterators(self):
        for mb in self._open_metaiterators:
            mb.fetch_until(0)

    def iter_reverse_branch_changes(self, branch_path, from_revnum, to_revnum,
                                    pb=None, limit=0):
        """Return all the changes that happened in a branch
        until branch_path,revnum.

        :return: iterator that returns RevisionMetadata objects.
        """
        if not isinstance(branch_path, text_type):
            raise TypeError(branch_path)
        history_iter = self.iter_changes(branch_path, from_revnum,
                                         to_revnum, pb=pb, limit=limit)

        metabranch = RevisionMetadataBranch(history_iter, self, limit)
        self.add_metaiterator(metabranch)
        return metabranch

    def iter_all_revisions(self, layout, check_unusual_path, from_revnum,
                           to_revnum=0, project=None, pb=None):
        """Iterate over all RevisionMetadata objects in a repository.

        :param layout: Repository layout to use
        :param check_unusual_path: Check whether to keep branch

        Layout decides which ones to pick up.
        """
        return filter_revisions(self.iter_all_changes(layout,
            check_unusual_path, from_revnum, to_revnum, project, pb))

    def iter_all_changes(self, layout, check_unusual_path, from_revnum,
                         to_revnum=0, project=None, pb=None, prefix=None):
        """Iterate over all RevisionMetadata objects and branch removals
        in a repository.

        :param layout: Repository layout to use
        :param check_unusual_path: Check whether to keep branch

        Layout decides which ones to pick up.
        """
        assert from_revnum >= to_revnum
        if check_unusual_path is None:
            check_unusual_path = lambda x: True
        if project is not None:
            prefixes = filter(self._log._transport.has,
                              layout.get_project_prefixes(project))
        else:
            prefixes = [u""]

        if prefix is not None:
            prefixes = list(restrict_prefixes(prefixes, prefix))

        browser = RevisionMetadataBrowser(prefixes, from_revnum, to_revnum,
                                          layout, self, project, pb=pb)
        self.add_metaiterator(browser)
        for kind, item in browser:
            if kind != "revision" or check_unusual_path(item.branch_path):
                yield kind, item
