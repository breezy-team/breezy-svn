# Copyright (C) 2006 Jelmer Vernooij <jelmer@samba.org>
# -*- coding: utf-8 -*-

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
"""Simple transport for accessing Subversion smart servers."""

import bzrlib
from bzrlib import (
        debug,
        urlutils,
        )
from bzrlib.errors import (
        NoSuchFile,
        TransportNotPossible, 
        FileExists,
        NotLocalUrl,
        InvalidURL,
        RedirectRequested,
        )
from bzrlib.trace import mutter, warning
from bzrlib.transport import Transport

import subvertpy
from subvertpy.client import get_config
import urlparse
import urllib

import bzrlib.plugins.svn
from bzrlib.plugins.svn.auth import create_auth_baton
from bzrlib.plugins.svn.errors import convert_svn_error, NoSvnRepositoryPresent

svn_config = get_config()


# Don't run any tests on SvnTransport as it is not intended to be 
# a full implementation of Transport
def get_test_permutations():
    return []


def get_svn_ra_transport(bzr_transport):
    """Obtain corresponding SvnRaTransport for a stock Bazaar transport."""
    if isinstance(bzr_transport, SvnRaTransport):
        return bzr_transport

    ra_transport = getattr(bzr_transport, "_svn_ra", None)
    if ra_transport is not None:
        return ra_transport

    # Save _svn_ra transport here so we don't have to connect again next time
    # we try to use bzr svn on this transport
    ra_transport = SvnRaTransport(bzr_transport.base)
    bzr_transport._svn_ra = ra_transport
    return ra_transport


def _url_unescape_uri(url):
    (scheme, netloc, path, query, fragment) = urlparse.urlsplit(url)
    if scheme in ("http", "https"):
        # Without this, URLs with + in them break
        path = urllib.unquote(path)
    return urlparse.urlunsplit((scheme, netloc, path, query, fragment))


def _url_escape_uri(url):
    (scheme, netloc, path, query, fragment) = urlparse.urlsplit(url)
    if scheme in ("http", "https"):
        # Without this, URLs with + in them break
        path = urllib.quote(path)
    return urlparse.urlunsplit((scheme, netloc, path, query, fragment))


svnplus_warning_showed = False

def warn_svnplus(url):
    global svnplus_warning_showed
    if not svnplus_warning_showed:
        warning("The svn+ syntax is deprecated, use %s instead.", url)
        svnplus_warning_showed = True


def bzr_to_svn_url(url):
    """Convert a Bazaar URL to a URL understood by Subversion.

    This will possibly remove the svn+ prefix.
    """
    if (url.startswith("svn+http://") or 
        url.startswith("svn+file://") or
        url.startswith("svn+https://")):
        url = url[len("svn+"):] # Skip svn+
        warn_svnplus(url)

    url = _url_unescape_uri(url)

    # The SVN libraries don't like trailing slashes...
    url = url.rstrip('/')

    return url


def Connection(url):
    try:
        mutter('opening SVN RA connection to %r' % url)
        ret = subvertpy.ra.RemoteAccess(url.encode('utf8'), 
                auth=create_auth_baton(url),
                client_string_func=bzrlib.plugins.svn.get_client_string)
        if 'transport' in debug.debug_flags:
            ret = MutteringRemoteAccess(ret)
    except subvertpy.SubversionException, (msg, num):
        if num in (subvertpy.ERR_RA_SVN_REPOS_NOT_FOUND,):
            raise NoSvnRepositoryPresent(url=url)
        if num == subvertpy.ERR_BAD_URL:
            raise InvalidURL(url)
        if num == subvertpy.ERR_RA_DAV_PATH_NOT_FOUND:
            raise NoSuchFile(url)
        if num == subvertpy.ERR_RA_DAV_RELOCATED:
            # Try to guess the new url
            if "'" in msg:
                new_url = msg.split("'")[1]
            elif "«" in msg:
                new_url = msg[msg.index("»")+2:msg.index("«")]
            else:
                raise AssertionError("Unable to parse error message: %s" % msg)
            raise RedirectRequested(source=url, target=new_url, 
                                    is_permanent=True)
        raise

    from bzrlib.plugins.svn import lazy_check_versions
    lazy_check_versions()

    return ret


class ConnectionPool(object):
    """Collection of connections to a Subversion repository."""
    def __init__(self):
        self.connections = set()

    def get(self, url):
        # Check if there is an existing connection we can use
        for c in self.connections:
            assert not c.busy, "busy connection in pool"
            if c.url == url:
                self.connections.remove(c)
                return c
        # Nothing available? Just pick an existing one and reparent:
        if len(self.connections) == 0:
            return Connection(url)
        c = self.connections.pop()
        try:
            c.reparent(_url_escape_uri(url))
            return c
        except NotImplementedError:
            self.connections.add(c)
            return Connection(url)
        except:
            self.connections.add(c)
            raise

    def add(self, connection):
        assert not connection.busy, "adding busy connection in pool"
        self.connections.add(connection)
    

class SvnRaTransport(Transport):
    """Fake transport for Subversion-related namespaces.
    
    This implements just as much of Transport as is necessary 
    to fool Bazaar. """
    @convert_svn_error
    def __init__(self, url="", pool=None, _uuid=None, _repos_root=None):
        bzr_url = url
        self.svn_url = bzr_to_svn_url(url)
        Transport.__init__(self, bzr_url)

        if pool is None:
            self.connections = ConnectionPool()

            # Make sure that the URL is valid by connecting to it.
            self.connections.add(self.connections.get(self.svn_url))
        else:
            self.connections = pool

        self._repos_root = _repos_root
        self._uuid = _uuid
        self.capabilities = {}

        from bzrlib.plugins.svn import lazy_check_versions
        lazy_check_versions()

    def get_connection(self, repos_path=None):
        if repos_path is not None:
            return self.connections.get(urlutils.join(self.get_svn_repos_root(), 
                                        repos_path))
        else:
            return self.connections.get(self.svn_url)

    def add_connection(self, conn):
        self.connections.add(conn)

    def has(self, relpath):
        """See Transport.has()."""
        # TODO: Raise TransportNotPossible here instead and 
        # catch it in bzrdir.py
        return False

    def get(self, relpath):
        """See Transport.get()."""
        # TODO: Raise TransportNotPossible here instead and 
        # catch it in bzrdir.py
        raise NoSuchFile(path=relpath)

    def stat(self, relpath):
        """See Transport.stat()."""
        raise TransportNotPossible('stat not supported on Subversion')

    def put_file(self, name, file, mode=0):
        raise TransportNotPossible("put_file not supported on Subversion")

    def get_uuid(self):
        if self._uuid is None:
            conn = self.get_connection()
            try:
                return conn.get_uuid()
            finally:
                self.add_connection(conn)
        return self._uuid

    def get_repos_root(self):
        root = self.get_svn_repos_root()
        if (self.base.startswith("svn+http:") or 
            self.base.startswith("svn+https:")):
            return "svn+%s" % root
        return root

    def get_svn_repos_root(self):
        if self._repos_root is None:
            conn = self.get_connection()
            try:
                self._repos_root = conn.get_repos_root()
            finally:
                self.add_connection(conn)
        return self._repos_root

    def get_latest_revnum(self):
        conn = self.get_connection()
        try:
            return conn.get_latest_revnum()
        finally:
            self.add_connection(conn)

    def iter_log(self, paths, from_revnum, to_revnum, limit, discover_changed_paths, 
                 strict_node_history, include_merged_revisions, revprops):
        assert paths is None or isinstance(paths, list)
        assert isinstance(from_revnum, int) and isinstance(to_revnum, int)
        assert isinstance(limit, int)
        from threading import Thread, Semaphore

        class logfetcher(Thread):
            def __init__(self, transport, *args, **kwargs):
                Thread.__init__(self)
                self.setDaemon(True)
                self.transport = transport
                self.args = args
                self.kwargs = kwargs
                self.pending = []
                self.conn = self.transport.get_connection()
                self.semaphore = Semaphore(0)
                self.busy = False

            def next(self):
                self.semaphore.acquire()
                ret = self.pending.pop(0)
                if isinstance(ret, Exception):
                    raise ret
                return ret

            def run(self):
                assert not self.busy, "already running"
                self.busy = True
                def rcvr(orig_paths, revision, revprops, has_children=None):
                    self.pending.append((orig_paths, revision, revprops, has_children))
                    self.semaphore.release()
                try:
                    try:
                        self.conn.get_log(rcvr, *self.args, **self.kwargs)
                        self.pending.append(None)
                    except Exception, e:
                        self.pending.append(e)
                finally:
                    self.pending.append(Exception("Some exception was not handled"))
                    self.semaphore.release()
                    self.transport.add_connection(self.conn)

        if paths is None:
            newpaths = None
        else:
            newpaths = [p.rstrip("/") for p in paths]

        fetcher = logfetcher(self, newpaths, from_revnum, to_revnum, limit, discover_changed_paths=discover_changed_paths, strict_node_history=strict_node_history, include_merged_revisions=include_merged_revisions, revprops=revprops)
        fetcher.start()
        return iter(fetcher.next, None)

    def get_log(self, rcvr, paths, from_revnum, to_revnum, limit, discover_changed_paths, 
                strict_node_history, include_merged_revisions, revprops):
        assert paths is None or isinstance(paths, list), "Invalid paths"

        all_true = True
        for item in [isinstance(x, str) for x in paths]:
            if not item:
                all_true = False
                break
        
        assert paths is None or all_true

        if paths is None:
            newpaths = None
        else:
            newpaths = [p.rstrip("/") for p in paths]

        conn = self.get_connection()
        try:
            return conn.get_log(rcvr, newpaths, 
                    from_revnum, to_revnum,
                    limit, discover_changed_paths, strict_node_history, 
                    include_merged_revisions,
                    revprops)
        finally:
            self.add_connection(conn)

    def change_rev_prop(self, revnum, name, value):
        conn = self.get_connection()
        try:
            return conn.change_rev_prop(revnum, name, value)
        finally:
            self.add_connection(conn)

    def get_dir(self, path, revnum, kind=False):
        conn = self.get_connection()
        try:
            return conn.get_dir(path, revnum, kind)
        finally:
            self.add_connection(conn)

    def get_file(self, path, stream, revnum):
        conn = self.get_connection()
        try:
            return conn.get_file(path, stream, revnum)
        finally:
            self.add_connection(conn)

    def list_dir(self, relpath):
        assert len(relpath) == 0 or relpath[0] != "/"
        if relpath == ".":
            relpath = ""
        try:
            (dirents, _, _) = self.get_dir(relpath, self.get_latest_revnum())
        except subvertpy.SubversionException, (msg, num):
            if num == subvertpy.ERR_FS_NOT_DIRECTORY:
                raise NoSuchFile(relpath)
            raise
        return dirents.keys()

    def check_path(self, path, revnum):
        conn = self.get_connection()
        try:
            return conn.check_path(path, revnum)
        finally:
            self.add_connection(conn)

    @convert_svn_error
    def mkdir(self, relpath, message="Creating directory"):
        conn = self.get_connection()
        try:
            ce = conn.get_commit_editor({"svn:log": message})
            try:
                node = ce.open_root(-1)
                batons = relpath.split("/")
                toclose = [node]
                for i in range(len(batons)):
                    node = node.open_directory("/".join(batons[:i]), -1)
                    toclose.append(node)
                toclose.append(node.add_directory(relpath, None, -1))
                for c in reversed(toclose):
                    c.close()
                ce.close()
            except subvertpy.SubversionException, (msg, num):
                ce.abort()
                if num == subvertpy.ERR_FS_NOT_DIRECTORY:
                    raise NoSuchFile(msg)
                if num == subvertpy.ERR_FS_ALREADY_EXISTS:
                    raise FileExists(msg)
                raise
        finally:
            self.add_connection(conn)

    def has_capability(self, cap):
        if cap in self.capabilities:
            return self.capabilities[cap]
        conn = self.get_connection()
        try:
            try:
                self.capabilities[cap] = conn.has_capability(cap)
            except subvertpy.SubversionException, (msg, num):
                if num != subvertpy.ERR_UNKNOWN_CAPABILITY:
                    raise
                self.capabilities[cap] = None
            except NotImplementedError:
                self.capabilities[cap] = None # None for unknown
            return self.capabilities[cap]
        finally:
            self.add_connection(conn)

    def revprop_list(self, revnum):
        conn = self.get_connection()
        try:
            return conn.rev_proplist(revnum)
        finally:
            self.add_connection(conn)

    def get_locations(self, path, peg_revnum, revnums):
        conn = self.get_connection()
        try:
            return conn.get_locations(path, peg_revnum, revnums)
        finally:
            self.add_connection(conn)

    def listable(self):
        """See Transport.listable().
        """
        return True

    # There is no real way to do locking directly on the transport 
    # nor is there a need to as the remote server will take care of 
    # locking
    class PhonyLock(object):
        def unlock(self):
            pass

    def lock_read(self, relpath):
        """See Transport.lock_read()."""
        return self.PhonyLock()

    def lock_write(self, path_revs, comment=None, steal_lock=False):
        return self.PhonyLock() # FIXME

    def _is_http_transport(self):
        return False
        return (self.svn_url.startswith("http://") or 
                self.svn_url.startswith("https://"))

    def clone_root(self):
        if self._is_http_transport():
            return SvnRaTransport(self.get_repos_root(), 
                                  bzr_to_svn_url(self.base),
                                  pool=self.connections)
        return SvnRaTransport(self.get_repos_root(),
                              pool=self.connections)

    def clone(self, offset=None):
        """See Transport.clone()."""
        if offset is None:
            newurl = self.base
        else:
            newurl = urlutils.join(self.base, offset)

        return SvnRaTransport(newurl, pool=self.connections)

    def local_abspath(self, relpath):
        """See Transport.local_abspath()."""
        absurl = self.abspath(relpath)
        if self.base.startswith("file:///"):
            return urlutils.local_path_from_url(absurl)
        raise NotLocalUrl(absurl)

    def abspath(self, relpath):
        """See Transport.abspath()."""
        return urlutils.join(self.base, relpath)


class MutteringRemoteAccess(object):

    busy = property(lambda self: self.actual.busy)
    url = property(lambda self: self.actual.url)

    def __init__(self, actual):
        self.actual = actual

    def check_path(self, path, revnum):
        mutter('svn check-path -r%d %s' % (revnum, path))
        return self.actual.check_path(path, revnum)

    def has_capability(self, cap):
        mutter('svn has-capability %s' % (cap,))
        return self.actual.has_capability(cap)

    def get_uuid(self):
        mutter('svn get-uuid')
        return self.actual.get_uuid()

    def get_repos_root(self):
        mutter('svn get-repos-root')
        return self.actual.get_repos_root()

    def get_latest_revnum(self):
        mutter('svn get-latest-revnum')
        return self.actual.get_latest_revnum()

    def get_log(self, callback, paths, from_revnum, to_revnum, *args, **kwargs):
        mutter('svn log -r%d:%d %r' % (from_revnum, to_revnum, paths))
        return self.actual.get_log(callback, paths, 
                    from_revnum, to_revnum, *args, **kwargs)

    def change_rev_prop(self, revnum, name, value):
        mutter('svn change-revprop -r%d %s=%s' % (revnum, name, value))
        return self.actual.change_rev_prop(revnum, name, value)

    def get_dir(self, path, revnum=-1, fields=0):
        mutter('svn get-dir -r%d %s' % (revnum, path))
        return self.actual.get_dir(path, revnum, fields)

    def get_file(self, path, revnum):
        mutter('svn get-file -r%d %s' % (revnum, path))
        return self.actual.get_file(path, revnum)

    def revprop_list(self, revnum):
        mutter('svn revprop-list -r%d' % (revnum,))
        return self.actual.revprop_list(revnum)

    def get_locations(self, path, peg_revnum, revnums):
        mutter('svn get_locations -r%d %s (%r)' % (peg_revnum, path, revnums))
        return self.actual.get_locations(path, peg_revnum, revnums)

    def do_update(self, revnum, path, start_empty, editor):
        mutter("svn update -r%d %s" % (revnum, path))
        return self.actual.do_update(revnum, path, start_empty, editor)

    def do_switch(self, revnum, path, start_empty, to_url, editor):
        mutter("svn switch -r%d %s -> %s" % (revnum, path, to_url))
        return self.actual.do_switch(revnum, path, start_empty, to_url, editor)

    def reparent(self, url):
        mutter("svn reparent %s" % url)
        return self.actual.reparent(url)

    def get_commit_editor(self, *args, **kwargs):
        mutter("svn commit")
        return self.actual.get_commit_editor(*args, **kwargs)

    def rev_proplist(self, revnum):
        mutter("svn rev-proplist -r%d" % revnum)
        return self.actual.rev_proplist(revnum)
