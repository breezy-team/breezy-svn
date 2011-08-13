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
import weakref

import bzrlib
from bzrlib import (
    osutils,
    trace,
    transport as _mod_transport,
    )
from bzrlib.config import (
    config_dir,
    ensure_config_dir_exists,
    )

# Compatibility with bzr < 2.5
get_transport_from_path = getattr(_mod_transport, "get_transport_from_path",
        _mod_transport.get_transport)

from bzrlib.plugins.svn import version_info


def write_cache_readme(path):
    f = open(path, 'w')
    try:
        f.write(
"""This directory contains information cached by the bzr-svn plugin.

It is used for performance reasons only and can be removed
without losing data.

See http://bazaar-vcs.org/BzrForeignBranches/Subversion for details.
""")
        if version_info[3] == 'exp':
            f.write("This is the directory used by the experimental version of bzr-svn.\n")
    finally:
        f.close()


def create_cache_dir():
    """Create the top-level bzr-svn cache directory.

    :return: Path to cache directory, as byte string.
    """
    ensure_config_dir_exists()
    if version_info[3] == 'exp':
        name = 'svn-cache-exp'
    else:
        name = 'svn-cache'
    if sys.platform in ("nt", "win32"):
        from bzrlib.win32utils import get_local_appdata_location
        s = get_local_appdata_location()
        assert s is not None
        # This can return a unicode string or a plain string in
        # user encoding
        if type(s) == str:
            s = s.decode(bzrlib.user_encoding)
        base_cache_dir = s.encode(osutils._fs_enc)
        cache_dir = os.path.join(base_cache_dir, name)
    else:
        base_cache_dir = config_dir()
        cache_dir = os.path.join(base_cache_dir, name)
        # Check and use old location if possible
        if not os.path.exists(cache_dir):
            try:
                from xdg.BaseDirectory import xdg_cache_home
            except ImportError:
                pass
            else:
                cache_dir = os.path.join(xdg_cache_home, "bazaar", "svn")
        if type(cache_dir) == unicode:
            cache_dir = cache_dir.encode(osutils._fs_enc)

    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
        write_cache_readme(os.path.join(cache_dir, "README"))

    return cache_dir


_cachedbs = threading.local()
def cachedbs():
    """Get a cache for this thread's db connections."""
    try:
        return _cachedbs.cache
    except AttributeError:
        _cachedbs.cache = weakref.WeakValueDictionary()
        return _cachedbs.cache

class RepositoryCache(object):
    """Object that provides a cache related to a particular UUID."""

    def __init__(self, uuid):
        self.uuid = uuid

    def create_cache_dir(self):
        cache_dir = create_cache_dir()
        assert isinstance(cache_dir, str)
        dir = os.path.join(cache_dir, self.uuid)
        if not os.path.exists(dir):
            trace.note("Initialising Subversion metadata cache in %s.",
                       dir.decode(osutils._fs_enc))
            os.mkdir(dir)
        return dir

    def open_transport(self):
        return get_transport_from_path(self.create_cache_dir().decode(osutils._fs_enc))

    def open_fileid_map(self):
        from bzrlib.plugins.svn.fileids import FileIdMapCache
        return FileIdMapCache(self.open_transport())

    def open_revid_map(self):
        raise NotImplementedError(self.open_revid_map)

    def open_logwalker(self):
        raise NotImplementedError(self.open_logwalker)

    def open_revision_cache(self):
        raise NotImplementedError(self.open_revision_cache)

    def open_parents(self):
        raise NotImplementedError(self.open_parents)

    def commit(self):
        pass

try:
    from bzrlib.plugins.svn.cache.tdbcache import TdbRepositoryCache
    cache_cls = TdbRepositoryCache
except ImportError:
    from bzrlib.plugins.svn.cache.sqlitecache import SqliteRepositoryCache
    cache_cls = SqliteRepositoryCache

def get_cache(uuid):
    try:
        return cachedbs()[uuid]
    except KeyError:
        db = cache_cls(uuid)
        cachedbs()[uuid] = db
        return db
