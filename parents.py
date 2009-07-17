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

from bzrlib.revision import NULL_REVISION

class DiskCachingParentsProvider(object):
    """Parents provider that caches parents in a SQLite database."""

    def __init__(self, actual, cache):
        self._cache = cache
        self.actual = actual

    def get_parent_map(self, keys):
        ret = {}
        todo = set()
        for k in keys:
            parents = self._cache.lookup_parents(k)
            if parents is None:
                todo.add(k)
            else:
                ret[k] = parents
        if len(todo) > 0:
            newfound = self.actual.get_parent_map(todo)
            for revid, parents in newfound.iteritems():
                if revid == NULL_REVISION:
                    continue
                self._cache.insert_parents(revid, parents)
            ret.update(newfound)
        return ret


class ParentsCache(object):

    def insert_parents(self, revid, parents):
        raise NotImplementedError(self.insert_parents)

    def lookup_parents(self, revid):
        raise NotImplementedError(self.lookup_parents)
