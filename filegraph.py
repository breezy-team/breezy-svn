# Copyright (C) 2005-2009 Jelmer Vernooij <jelmer@samba.org>

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

"""Per-file graph for Subversion."""

from bzrlib.revision import NULL_REVISION


class PerFileParentProvider(object):
    """Subversion texts backend."""

    def __init__(self, repository):
        self.repository = repository

    def _get_parent(self, fileid, revid):
        revmeta, mapping = self.repository._get_revmeta(revid)
        parentrevmeta = revmeta.get_lhs_parent_revmeta(mapping)
        fileidmap = self.repository.get_fileid_map(revmeta, mapping)
        try:
            path = fileidmap.reverse_lookup(mapping, fileid)
        except KeyError:
            return None

        text_parents = revmeta.get_text_parents(mapping)
        if path in text_parents:
            return tuple([
                (fileid, tp) for tp in text_parents[path]
                if tp != NULL_REVISION])

        # Not explicitly recorded - so just return the text revisions 
        # present in the parents of the mentioned revision.
        ret = []
        rev_parent_revids = revmeta.get_parent_ids(mapping, parentrevmeta)
        for (revid, revmeta, mapping) in self.repository._iter_revmetas(rev_parent_revids):
            if revid == NULL_REVISION:
                continue # Nothing exists in NULL_REVISION
            fileidmap = self.repository.get_fileid_map(revmeta, mapping)
            try:
                path = fileidmap.reverse_lookup(mapping, fileid)
            except KeyError:
                pass # File didn't exist here
            else:
                text_parent = fileidmap.lookup(mapping, path)[:2]
                assert len(text_parent) == 2
                if text_parent not in ret and text_parent[1] != NULL_REVISION:
                    ret.append(text_parent)

        return tuple(ret)

    def get_parent_map(self, keys):
        invs = {}

        # First, figure out the revision number/path
        ret = {}
        for k in keys:
            if k == NULL_REVISION:
                ret[k] = ()
            elif len(k) == 2: # We only know how to handle this
                if k[1] == NULL_REVISION:
                    ret[k] = None
                else:
                    ret[k] = self._get_parent(*k)
            else:
                ret[k] = None
        return ret
