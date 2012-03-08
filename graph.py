# Copyright (C) 2011 Jelmer Vernooij <jelmer@samba.org>

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

"""Subversion revision graph access."""

from __future__ import absolute_import

from bzrlib import (
    errors as _mod_errors,
    graph as _mod_graph,
    revision as _mod_revision,
    )


class SubversionGraph(_mod_graph.Graph):

    def __init__(self, repository, parents_provider):
        super(SubversionGraph, self).__init__(parents_provider)
        self._revmeta_provider = repository._revmeta_provider

    def iter_lefthand_ancestry(self, start_key, stop_keys=None):
        """Iterate backwards through revision ids in the lefthand history

        :param start_key: The revision id to start with.  All its lefthand
            ancestors will be traversed.
        """
        if stop_keys is None:
            stop_keys = ()
        if _mod_revision.is_null(start_key):
            return
        try:
            foreign_revid, mapping = self._revmeta_provider.lookup_bzr_revision_id(start_key)
        except _mod_errors.NoSuchRevision:
            for key in super(SubversionGraph, self).iter_lefthand_ancestry(start_key,
                    stop_keys):
                yield key
            return
        (uuid, branch_path, revnum) = foreign_revid
        for revmeta, hidden, mapping in self._revmeta_provider._iter_reverse_revmeta_mapping_history(
                branch_path, revnum, to_revnum=0, mapping=mapping):
            if hidden:
                continue
            key = revmeta.get_revision_id(mapping)
            if key in stop_keys:
                return
            yield key
