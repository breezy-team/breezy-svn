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
"""Subversion cache directory access."""

import os
import sys
import threading

import bzrlib
from bzrlib import (
    debug, 
    osutils,
    trace,
    )
from bzrlib.config import (
    config_dir,
    ensure_config_dir_exists,
    )
from bzrlib.transport import (
    get_transport,
    )

from bzrlib.plugins.svn import version_info

def create_cache_dir():
    """Create the top-level bzr-svn cache directory.
    
    :return: Path to cache directory.
    """
    ensure_config_dir_exists()
    if version_info[3] == 'exp':
        name = 'svn-cache-exp'
        extra = "This is the directory used by the experimental version of bzr-svn.\n"
    else:
        name = 'svn-cache'
        extra = ""
    if sys.platform in ("nt", "win32"):
        from bzrlib.win32utils import get_local_appdata_location
        s = get_local_appdata_location()
        assert s is not None
        # This can return a unicode string or a plain string in 
        # user encoding
        if type(s) == str:
            s = s.decode(bzrlib.user_encoding)
        base_cache_dir = s.encode(osutils._fs_enc)
    else:
        base_cache_dir = config_dir()
    cache_dir = os.path.join(base_cache_dir, name)

    if not os.path.exists(cache_dir):
        os.mkdir(cache_dir)

        open(os.path.join(cache_dir, "README"), 'w').write(
"""This directory contains information cached by the bzr-svn plugin.

It is used for performance reasons only and can be removed 
without losing data.

See http://bazaar-vcs.org/BzrForeignBranches/Subversion for details.
""" + extra)
    return cache_dir


def check_pysqlite_version(sqlite3):
    """Check that sqlite library is compatible.

    """
    if (sqlite3.sqlite_version_info[0] < 3 or 
            (sqlite3.sqlite_version_info[0] == 3 and 
             sqlite3.sqlite_version_info[1] < 3)):
        trace.warning('Needs at least sqlite 3.3.x')
        raise bzrlib.errors.BzrError("incompatible sqlite library")

try:
    try:
        import sqlite3
        check_pysqlite_version(sqlite3)
    except (ImportError, bzrlib.errors.BzrError), e: 
        from pysqlite2 import dbapi2 as sqlite3
        check_pysqlite_version(sqlite3)
except:
    trace.warning('Needs at least Python2.5 or Python2.4 with the pysqlite2 '
            'module')
    raise bzrlib.errors.BzrError("missing sqlite library")

connect_cachefile = sqlite3.connect

class CacheTable(object):
    """Simple base class for SQLite-based caches."""
    def __init__(self, cache_db=None):
        if cache_db is None:
            self.cachedb = sqlite3.connect(":memory:")
        else:
            self.cachedb = cache_db
        self._commit_interval = 500
        self._create_table()
        self.cachedb.commit()
        self._commit_countdown = self._commit_interval

    def commit(self):
        """Commit the changes to the database."""
        self.cachedb.commit()
        self._commit_countdown = self._commit_interval

    def commit_conditionally(self):
        self._commit_countdown -= 1
        if self._commit_countdown <= 0:
            self.commit()

    def _create_table(self):
        pass

    @staticmethod
    def mutter(text, *args):
        if "cache" in debug.debug_flags:
            trace.mutter(text, *args)


CACHE_DB_VERSION = 5


_cachedbs = threading.local()
def cachedbs():
    """Get a cache for this thread's db connections."""
    try:
        return _cachedbs.cache
    except AttributeError:
        _cachedbs.cache = {}
        return _cachedbs.cache


class RepositoryCache(object):
    """Object that provides a cache related to a particular UUID."""

    def __init__(self, uuid):
        self.uuid = uuid

    def create_cache_dir(self):
        cache_dir = create_cache_dir()
        dir = os.path.join(cache_dir, self.uuid)
        if not os.path.exists(dir):
            trace.info("Initialising Subversion metadata cache in %s",
                       dir.decode(osutils._fs_enc))
            os.mkdir(dir)
        return dir

    def open_sqlite(self):
        cache_file = os.path.join(self.create_cache_dir(), 'cache-v%d' % CACHE_DB_VERSION)
        assert isinstance(cache_file, str)
        if not cachedbs().has_key(cache_file):
            cachedbs()[cache_file] = connect_cachefile(cache_file.decode(osutils._fs_enc).encode("utf-8"))
        return cachedbs()[cache_file]

    def open_transport(self):
        return get_transport(self.create_cache_dir().decode(osutils._fs_enc))

    def open_fileid_map(self):
        from bzrlib.plugins.svn.fileids import FileIdMapCache
        return FileIdMapCache(self.open_transport())

    def open_revid_map(self):
        from bzrlib.plugins.svn.revids import RevisionIdMapCache
        return RevisionIdMapCache(self.open_sqlite())
