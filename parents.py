# Copyright (C) 2005-2009 Jelmer Vernooij <jelmer@samba.org>
 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from bzrlib import debug
from bzrlib.knit import make_file_factory
from bzrlib.trace import mutter
from bzrlib.revision import NULL_REVISION
from bzrlib.versionedfile import ConstantMapper

from bzrlib.plugins.svn.cache import CacheTable

class DiskCachingParentsProvider(object):
    """Parents provider that caches parents in a SQLite database."""

    def __init__(self, actual, cachedb=None):
        self._cache = ParentsCache(cachedb)
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


class ParentsCache(CacheTable):

    def _create_table(self):
        self.cachedb.executescript("""
        create table if not exists parent (rev text, parent text, idx int);
        create unique index if not exists rev_parent_idx on parent (rev, idx);
        create unique index if not exists rev_parent on parent (rev, parent);
        """)
        self._commit_interval = 1000

    def insert_parents(self, revid, parents):
        if "cache" in debug.debug_flags:
            mutter('insert parents: %r -> %r', revid, parents)
        if len(parents) == 0:
            self.cachedb.execute("replace into parent (rev, parent, idx) values (?, NULL, -1)", (revid,))
        else:
            for i, p in enumerate(parents):
                self.cachedb.execute("replace into parent (rev, parent, idx) values (?, ?, ?)", (revid, p, i))

    def lookup_parents(self, revid):
        if "cache" in debug.debug_flags:
            mutter('lookup parents: %r', revid)
        rows = self.cachedb.execute("select parent from parent where rev = ? order by idx", (revid, )).fetchall()
        if len(rows) == 0:
            return None
        return tuple([row[0] for row in rows if row[0] is not None])
