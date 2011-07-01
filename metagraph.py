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

from bzrlib.plugins.svn import (
    changes,
    )


class MetaRevision(object):
    """Object describing a revision in a Subversion repository.

    Tries to be as lazy as possible - data is not retrieved or calculated
    from other known data before contacting the Subversions server.
    """

    __slots__ = ('branch_path', 'revnum', 'uuid',
                 '_paths', '_revprops', '_log')

    def __init__(self, logwalker, uuid, branch_path, revnum, paths=None,
            revprops=None):
        self.branch_path = branch_path
        self.revnum = revnum
        self.uuid = uuid
        self._log = logwalker
        self._paths = paths
        self._revprops = revprops

    def __eq__(self, other):
        return (type(self) == type(other) and
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
        self._revprops = self._log._transport.revprop_list(self.revnum)

    @property
    def paths(self):
        """Fetch the changed paths dictionary for this revision.
        """
        if self._paths is None:
            self._paths = self._log.get_revision_paths(self.revnum)
        return self._paths

    @property
    def revprops(self):
        """Get the revision properties set on the revision."""
        if self._revprops is None:
            self._revprops = self._log.revprop_list(self.revnum)
        return self._revprops

    def knows_revprops(self):
        """Check whether all revision properties can be cheaply retrieved."""
        if self._revprops is None:
            return False
        revprops = self.revprops
        return isinstance(revprops, dict) or revprops.is_loaded

    def changes_outside_root(self):
        """Check if there are any changes in this svn revision not under
        this revmeta's root."""
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


class MetaRevisionGraph(object):

    def __init__(self, logwalker):
        self._log = logwalker

    def iter_changes(self, branch_path, from_revnum, to_revnum, pb=None,
                     limit=0):
        """Iterate over all revisions backwards.

        :return: iterator that returns tuples with branch path,
            changed paths, revision number, changed file properties and
        """
        assert isinstance(branch_path, str)
        assert from_revnum >= to_revnum

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
            assert revnum > 0 or bp == ""

            if changes.changes_path(paths, bp, False):
                yield (bp, paths, revnum, revprops)
                i += 1

            if next is None:
                bp = None
            else:
                bp = next[0]
