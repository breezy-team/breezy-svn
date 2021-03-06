# Copyright (C) 2006-2009 Jelmer Vernooij <jelmer@samba.org>
# -*- coding: utf-8 -*-

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
"""Simple transport for accessing Subversion smart servers."""

from __future__ import absolute_import

import stat
import subvertpy
from subvertpy import (
    AUTH_PARAM_DEFAULT_USERNAME,
    AUTH_PARAM_DEFAULT_PASSWORD,
    ERR_FS_NOT_DIRECTORY,
    ERR_FS_NOT_FOUND,
    )
from subvertpy.client import get_config
from subvertpy.ra import RemoteAccess
from subvertpy.subr import (
    uri_canonicalize as svn_uri_canonicalize,
    )
import sys
try:
    import urllib.parse as urlparse
except ImportError:  # Python < 3.7
    import urlparse

import breezy
from breezy import (
    debug,
    ui,
    urlutils,
    )
from breezy.errors import (
    BzrError,
    FileExists,
    NoSuchFile,
    NotLocalUrl,
    RedirectRequested,
    TransportNotPossible,
    )
from six import (
    text_type,
    )
from breezy.trace import (
    mutter,
    warning,
    )
from breezy.transport import (
    Transport,
    register_urlparse_netloc_protocol,
    )

register_urlparse_netloc_protocol('svn+http')
register_urlparse_netloc_protocol('svn+https')
import breezy.plugins.svn
from .auth import (
    create_auth_baton,
    )
from .changes import (
    common_prefix,
    )
from .errors import (
    convert_svn_error,
    DavRequestFailed,
    NoSvnRepositoryPresent,
    convert_error,
    )


try:
    svn_config = get_config()
except subvertpy.SubversionException as e:
    msg, num = e.args
    if num == subvertpy.ERR_MALFORMED_FILE:
        raise BzrError(msg)
    raise


# This variable is here to allow tests to temporarily disable features
# to see how bzr-svn copes with that
disabled_capabilities = set()


# Don't run any tests on SvnTransport as it is not intended to be
# a full implementation of Transport
def get_test_permutations():
    return []


_warned_codeplex = False
def warn_codeplex(host):
    global _warned_codeplex
    if not _warned_codeplex:
        warning("Please note %s is hosted on Codeplex which runs a broken "
                "Subversion server. Please consider using the bzr-tfs plugin, "
                "which provides support for CodePlex' native Team Foundation Server." % host)
        _warned_codeplex = True


def get_svn_ra_transport(bzr_transport):
    """Obtain corresponding SvnRaTransport for a stock Bazaar transport."""
    if isinstance(bzr_transport, SvnRaTransport):
        return bzr_transport

    ra_transport = getattr(bzr_transport, "_svn_ra", None)
    if ra_transport is not None:
        return ra_transport

    # Save _svn_ra transport here so we don't have to connect again next time
    # we try to use bzr svn on this transport
    shared_connection = getattr(bzr_transport, "_shared_connection", None)
    if (shared_connection is not None and
        shared_connection.credentials is not None):
        if isinstance(shared_connection.credentials, dict):
            creds = shared_connection.credentials
        elif isinstance(shared_connection.credentials[0], dict):
            creds = shared_connection.credentials[0]
        else:
            creds = None
    else:
        creds = None
    ra_transport = SvnRaTransport(bzr_transport.external_url(),
        credentials=creds)
    bzr_transport._svn_ra = ra_transport
    return ra_transport


def _url_unescape_uri(url):
    (scheme, netloc, path, query, fragment) = urlparse.urlsplit(url)
    if scheme in ("http", "https"):
        # Without this, URLs with + in them break
        path = urlutils.quote(urlutils.unquote(path), safe="/+%")
    while "//" in path:
        path = path.replace("//", "/")
    return urlparse.urlunsplit((scheme, netloc, path, query, fragment))


def _url_escape_uri(url):
    (scheme, netloc, path, query, fragment) = urlparse.urlsplit(url)
    if scheme in ("http", "https"):
        # Without this, URLs with + in them break
        path = urlutils.quote(path, safe="/+%")
    return urlparse.urlunsplit((scheme, netloc, path, query, fragment))


def url_join_unescaped_path(url, path):
    (scheme, netloc, basepath, query, fragment) = urlparse.urlsplit(url)
    path = urlutils.join(urlutils.unquote(basepath), path)
    if scheme in ("http", "https"):
        # Without this, URLs with + in them break
        path = urlutils.quote(path, safe="/+%")
    return urlparse.urlunsplit((scheme, netloc, path, query, fragment))


def bzr_to_svn_url(url):
    """Convert a Bazaar URL to a URL understood by Subversion.

    This will possibly remove the svn+ prefix.
    """
    url = urlutils.split_segment_parameters(url.strip("/"))[0]
    if url.startswith("readonly+"):
        url = url[len("readonly+"):]
        readonly = True
    else:
        readonly = False

    if (url.startswith("svn+http://") or
        url.startswith("svn+https://")):
        url = url[len("svn+"):] # Skip svn+

    url = svn_uri_canonicalize(url)

    # The SVN libraries don't like trailing slashes...
    url = url.rstrip('/')

    return url, readonly


def svn_to_bzr_url(url, readonly=False):
    """Convert a Subversion URL to a URL understood by Bazaar.

    This mainly involves fixing file:// URLs on Windows.
    """
    if sys.platform == "win32":
        # Subversion URLs have only two // after file: on Windows
        if url.startswith("file://"):
            url = "file:///" + url[len("file://"):]
    if readonly:
        url = "readonly+" + url
    return url


class SubversionProgressReporter(object):

    def __init__(self, url):
        self._scheme = urlparse.urlsplit(url)[0]
        self._last_progress = 0
        # This variable isn't used yet as of bzr 1.12, and finding
        # the right Transport object will be tricky in bzr-svn's case
        # so just setting it to None for now.

    def update(self, progress, total):
        # The counter seems to reset sometimes
        if self._last_progress > progress:
            assert progress < 100000
            self._last_progress = 0
        changed = progress - self._last_progress
        if changed < 0:
            raise AssertionError("changed was %d (%d -> %d)" % (changed, self._last_progress, progress))
        self._last_progress = progress
        ui.ui_factory.report_transport_activity(self, changed, None)


def convert_relocate_error(url, num, msg):
    """Convert a permanently moved error."""
    # Try to guess the new url
    if "'" in msg:
        new_url = msg.split("'")[1]
    elif "«" in msg:
        new_url = msg[msg.index("»")+2:msg.index("«")]
    else:
        raise AssertionError("Unable to parse error message: %s" % msg)
    raise RedirectRequested(source=_url_escape_uri(url),
        target=_url_escape_uri(urlutils.join(url, new_url)),
        is_permanent=True)


def Connection(url, auth=None, config=None, readonly=False):
    from . import get_client_string
    progress_cb = SubversionProgressReporter(url).update
    try:
        ret = RemoteAccess(
            _url_escape_uri(url), auth=auth,
            client_string_func=get_client_string,
            progress_cb=progress_cb,
            config=config)
        if 'transport' in debug.debug_flags:
            ret = MutteringRemoteAccess(ret)
        if readonly:
            ret = ReadonlyRemoteAccess(ret)
    except subvertpy.SubversionException as e:
        msg, num = e.args
        if num in (subvertpy.ERR_RA_SVN_REPOS_NOT_FOUND,):
            raise NoSvnRepositoryPresent(url=url)
        if num == subvertpy.ERR_BAD_URL:
            raise urlutils.InvalidURL(url)
        if num in (subvertpy.ERR_RA_DAV_PATH_NOT_FOUND, subvertpy.ERR_FS_NOT_FOUND):
            raise NoSuchFile(url)
        if num == subvertpy.ERR_RA_ILLEGAL_URL:
            raise urlutils.InvalidURL(url, msg)
        if num == subvertpy.ERR_RA_DAV_RELOCATED:
            raise convert_relocate_error(url, num, msg)
        raise convert_error(e)
    from . import lazy_check_versions
    lazy_check_versions()
    return ret


class ConnectionPool(object):
    """Collection of connections to a Subversion repository."""

    def __init__(self, url, readonly=False):
        self.start_url = url
        self.connections = set()
        self.readonly = readonly
        self.auth_baton = create_auth_baton(url)

    def set_credentials(self, credentials):
        if isinstance(credentials, dict):
            user = credentials.get('user')
            password = credentials.get('password')
        else:
            user, password = credentials[:2]
        if user is not None:
            self.auth_baton.set_parameter(AUTH_PARAM_DEFAULT_USERNAME, user)
        if password is not None:
            self.auth_baton.set_parameter(AUTH_PARAM_DEFAULT_PASSWORD, password)

    def get_any(self):
        try:
            c = self.connections.pop()
        except KeyError:
            return Connection(self.start_url, self.auth_baton,
                readonly=self.readonly)
        else:
            assert not c.busy, "busy connection in pool"
            return c

    def new(self, url):
        # Nothing available? Just pick an existing one and reparent:
        if len(self.connections) == 0:
            return Connection(url, self.auth_baton, readonly=self.readonly)
        c = self.connections.pop()
        try:
            c.reparent(_url_escape_uri(url))
            return c
        except NotImplementedError:
            self.connections.add(c)
            return Connection(url, self.auth_baton, readonly=self.readonly)
        except:
            self.connections.add(c)
            raise

    def get_parent(self, url):
        # Check if there is an existing connection we can use
        for c in self.connections:
            assert not c.busy, "busy connection in pool"
            assert not c.url.endswith("/"), "%r ends with a /" % c.url
            assert isinstance(url, text_type)
            if url == c.url or url.startswith(c.url+"/"):
                self.connections.remove(c)
                relpath = urlutils.relative_url(c.url+"/", url.rstrip("/")+"/")
                if relpath == ".":
                    relpath = ""
                return c, relpath.rstrip("/")
        return self.new(url), ""

    def get(self, url):
        # Check if there is an existing connection we can use
        for c in self.connections:
            assert not c.busy, "busy connection in pool"
            if c.url == url:
                self.connections.remove(c)
                return c
        return self.new(url)

    def add(self, connection):
        assert not connection.busy, "adding busy connection in pool"
        self.connections.add(connection)


class SvnRaTransport(Transport):
    """Fake transport for Subversion-related namespaces.

    This implements just as much of Transport as is necessary
    to fool Bazaar. """

    @convert_svn_error
    def __init__(self, url, from_transport=None, credentials=None):
        self.svn_url, readonly = bzr_to_svn_url(url)
        Transport.__init__(self, url)
        if not isinstance(url, str):
            url = url.encode()
        host = urlutils.parse_url(url)[3]
        if host.endswith(".codeplex.com"):
            warn_codeplex(host)
        if from_transport is None:
            self.connections = ConnectionPool(
                    self.svn_url, readonly=readonly)
            if credentials is not None:
                assert isinstance(credentials, dict)
                self.connections.set_credentials(credentials)
            # Make sure that the URL is valid by connecting to it.
            self.connections.add(self.connections.get(self.svn_url))
        else:
            if readonly != from_transport.is_readonly():
                raise AssertionError("readonly %r != %r (%r)" % (readonly,
                        from_transport.is_readonly(), from_transport))
            self.connections = from_transport.connections
            if credentials is not None:
                assert isinstance(credentials, dict)
                self.connections.set_credentials(credentials)
        self._repos_root = None
        self._uuid = None
        self.capabilities = {}
        from . import lazy_check_versions
        lazy_check_versions()

    def is_readonly(self):
        return self.connections.readonly

    def get_any_connection(self):
        return self.connections.get_any()

    def get_connection(self, repos_path=None):
        if repos_path is not None:
            return self.connections.get(urlutils.join(self.get_svn_repos_root(),
                                        repos_path))
        else:
            return self.connections.get(self.svn_url)

    def get_path_connection(self, path):
        return self.connections.get_parent(
            urlutils.join(self.svn_url, path))

    def get_paths_connection(self, paths):
        paths = [p.strip(u"/") for p in paths]
        prefix = common_prefix(paths)
        subpaths = [urlutils.determine_relative_path(prefix, p) for p in paths]
        conn, relprefix = self.get_path_connection(prefix)
        if relprefix == u"":
            relsubpaths = subpaths
        else:
            relsubpaths = [urlutils.join(relprefix, p) for p in subpaths]
        return (conn, relsubpaths)

    def external_url(self):
        return self.base

    def add_connection(self, conn):
        self.connections.add(conn)

    def has(self, relpath):
        """See Transport.has()."""
        return self.check_path(relpath, -1) != subvertpy.NODE_NONE

    def get(self, relpath):
        """See Transport.get()."""
        # TODO: Raise TransportNotPossible here instead and
        # catch it in bzrdir.py
        raise NoSuchFile(path=relpath)

    def stat(self, path):
        """See Transport.stat()."""
        class StatResult(object):
            def __init__(self, svn_stat):
                if svn_stat['kind'] == subvertpy.NODE_DIR:
                    self.st_mode = stat.S_IFDIR
                elif svn_stat['kind'] == subvertpy.NODE_FILE:
                    self.st_mode = stat.S_IFREG
                self.st_size = svn_stat['size']
                self.st_mtime = svn_stat['time']
        conn, relpath = self.get_path_connection(path)
        try:
            try:
                return StatResult(conn.stat(relpath, -1))
            except NotImplementedError:
                raise TransportNotPossible("stat not supported in this version")
        finally:
            self.add_connection(conn)

    def put_file(self, name, file, mode=0):
        raise TransportNotPossible("put_file not supported on Subversion")

    def get_uuid(self):
        if self._uuid is None:
            conn = self.get_any_connection()
            try:
                return str(conn.get_uuid())
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
            conn = self.get_any_connection()
            try:
                self._repos_root = conn.get_repos_root()
            finally:
                self.add_connection(conn)
        return self._repos_root

    def get_latest_revnum(self):
        conn = self.get_any_connection()
        try:
            return conn.get_latest_revnum()
        finally:
            self.add_connection(conn)

    def iter_log(self, paths, from_revnum, to_revnum, limit,
                 discover_changed_paths, strict_node_history,
                 include_merged_revisions, revprops):
        assert paths is None or isinstance(paths, list)
        assert isinstance(from_revnum, int) and isinstance(to_revnum, int)
        assert isinstance(limit, int)

        if paths is not None:
            conn, paths = self.get_paths_connection(paths)
        else:
            conn = self.get_connection()

        return conn.iter_log(paths, from_revnum, to_revnum, limit,
            discover_changed_paths=discover_changed_paths,
            strict_node_history=strict_node_history,
            include_merged_revisions=include_merged_revisions,
            revprops=revprops)

    def get_log(self, rcvr, paths, from_revnum, to_revnum, limit,
            discover_changed_paths, strict_node_history,
            include_merged_revisions, revprops):

        if paths is not None:
            conn, paths = self.get_paths_connection(paths)
        else:
            conn = self.get_connection()
        try:
            try:
                return conn.get_log(rcvr, paths,
                        from_revnum, to_revnum,
                        limit, discover_changed_paths, strict_node_history,
                        include_merged_revisions,
                        revprops)
            except subvertpy.SubversionException as e:
                msg, num = e.args
                if num == subvertpy.ERR_RA_DAV_REQUEST_FAILED:
                    raise DavRequestFailed(msg)
                elif num == subvertpy.ERR_RA_DAV_RELOCATED:
                    raise convert_relocate_error(conn.url, num, msg)
                else:
                    raise e
        finally:
            self.add_connection(conn)

    def change_rev_prop(self, revnum, name, value):
        conn = self.get_any_connection()
        try:
            return conn.change_rev_prop(revnum, name, value)
        finally:
            self.add_connection(conn)

    @convert_svn_error
    def get_dir(self, path, revnum, fields=0):
        conn, relpath = self.get_path_connection(path)
        try:
            return conn.get_dir(relpath, revnum, fields)
        finally:
            self.add_connection(conn)

    def get_file(self, path, stream, revnum):
        conn, relpath = self.get_path_connection(path)
        try:
            return conn.get_file(relpath, stream, revnum)
        finally:
            self.add_connection(conn)

    def get_file_revs(self, path, start_revnum, end_revnum, handler,
                      include_merged_revisions=False):
        conn, relpath = self.get_path_connection(path)
        try:
            return conn.get_file_revs(relpath, start_revnum, end_revnum, handler,
                                      include_merged_revisions)
        finally:
            self.add_connection(conn)

    def list_dir(self, relpath):
        assert len(relpath) == 0 or relpath[0] != "/"
        try:
            (dirents, _, _) = self.get_dir(relpath, self.get_latest_revnum())
        except subvertpy.SubversionException as e:
            msg, num = e.args
            if num == ERR_FS_NOT_DIRECTORY:
                raise NoSuchFile(relpath)
            raise
        return dirents.keys()

    def check_path(self, path, revnum):
        conn, relpath = self.get_path_connection(path)
        try:
            return conn.check_path(relpath, revnum)
        finally:
            self.add_connection(conn)

    @property
    def repos_path(self):
        return self.svn_url[len(self.get_repos_root()):].strip("/")

    @convert_svn_error
    def create_prefix(self, mode=None):
        create_branch_prefix(self.clone_root(),
                {"svn:log": "Creating prefix"},
                self.repos_path.split("/")[:-1])

    @convert_svn_error
    def mkdir(self, relpath, mode=None, message=u"Creating directory"):
        relpath = urlutils.join(self.repos_path, relpath)
        dirname, basename = urlutils.split(relpath)
        conn = self.get_connection(dirname.strip("/"))
        try:
            with conn.get_commit_editor({"svn:log": message}) as ce:
                try:
                    with ce.open_root(-1) as node:
                        node.add_directory(relpath, None, -1).close()
                except subvertpy.SubversionException as e:
                    msg, num = e.args
                    if num == ERR_FS_NOT_FOUND:
                        raise NoSuchFile(msg)
                    if num == ERR_FS_NOT_DIRECTORY:
                        raise NoSuchFile(msg)
                    if num == subvertpy.ERR_FS_ALREADY_EXISTS:
                        raise FileExists(msg)
                    raise
        finally:
            self.add_connection(conn)

    def has_capability(self, cap):
        if cap in disabled_capabilities:
            return False
        if cap in self.capabilities:
            return self.capabilities[cap]
        conn = self.get_any_connection()
        try:
            try:
                self.capabilities[cap] = conn.has_capability(cap)
            except subvertpy.SubversionException as e:
                msg, num = e.args
                if num != subvertpy.ERR_UNKNOWN_CAPABILITY:
                    raise
                self.capabilities[cap] = None
            except NotImplementedError:
                self.capabilities[cap] = None # None for unknown
            return self.capabilities[cap]
        finally:
            self.add_connection(conn)

    def revprop_list(self, revnum):
        conn = self.get_any_connection()
        try:
            return conn.rev_proplist(revnum)
        finally:
            self.add_connection(conn)

    def get_locations(self, path, peg_revnum, revnums):
        conn, relpath = self.get_path_connection(path)
        try:
            return conn.get_locations(relpath, peg_revnum, revnums)
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

    def clone_root(self):
        url = str(self.get_repos_root())
        if self.is_readonly():
            url = "readonly+" + url
        return SvnRaTransport(url, self)

    def clone(self, offset=None):
        """See Transport.clone()."""
        if offset is None:
            newurl = self.base
        else:
            newurl = urlutils.join(self.base, offset)

        return SvnRaTransport(newurl, self)

    def local_abspath(self, relpath):
        """See Transport.local_abspath()."""
        absurl = self.abspath(relpath)
        if self.base.startswith("file:///"):
            return urlutils.local_path_from_url(absurl)
        raise NotLocalUrl(absurl)

    def abspath(self, relpath):
        """See Transport.abspath()."""
        return urlutils.join(self.base, relpath)


class ReadonlyRemoteAccess(object):

    busy = property(lambda self: self.actual.busy)
    url = property(lambda self: self.actual.url)

    def __init__(self, actual):
        self.actual = actual

    def check_path(self, path, revnum):
        return self.actual.check_path(path, revnum)

    def stat(self, path, revnum):
        return self.actual.stat(path, revnum)

    def has_capability(self, cap):
        return self.actual.has_capability(cap)

    def get_uuid(self):
        return self.actual.get_uuid()

    def get_repos_root(self):
        return self.actual.get_repos_root()

    def get_latest_revnum(self):
        return self.actual.get_latest_revnum()

    def get_log(self, *args, **kwargs):
        return self.actual.get_log(*args, **kwargs)

    def iter_log(self, *args, **kwargs):
        return self.actual.iter_log(*args, **kwargs)

    def change_rev_prop(self, revnum, name, value):
        raise TransportNotPossible('readonly transport')

    def get_dir(self, *args, **kwargs):
        return self.actual.get_dir(*args, **kwargs)

    def get_file(self, *args, **kwargs):
        return self.actual.get_file(*args, **kwargs)

    def get_file_revs(self, *args, **kwargs):
        return self.actual.get_file_revs(*args, **kwargs)

    def revprop_list(self, revnum):
        return self.actual.revprop_list(revnum)

    def get_locations(self, path, peg_revnum, revnums):
        return self.actual.get_locations(path, peg_revnum, revnums)

    def do_update(self, revnum, path, start_empty, editor):
        return self.actual.do_update(revnum, path, start_empty, editor)

    def do_diff(self, *args, **kwargs):
        return self.actual.do_diff(*args, **kwargs)

    def do_switch(self, *args, **kwargs):
        return self.actual.do_switch(*args, **kwargs)

    def reparent(self, url):
        return self.actual.reparent(url)

    def get_commit_editor(self, *args, **kwargs):
        raise TransportNotPossible('readonly transport')

    def rev_proplist(self, revnum):
        return self.actual.rev_proplist(revnum)

    def replay_range(self, *args, **kwargs):
        return self.actual.replay_range(*args, **kwargs)

    def replay(self, *args, **kwargs):
        return self.actual.replay(*args, **kwargs)


class MutteringRemoteAccess(object):
    """Trivial RemoteAccess wrapper that mutters all activity."""

    busy = property(lambda self: self.actual.busy)
    url = property(lambda self: self.actual.url)

    def __init__(self, actual):
        self.actual = actual

    def check_path(self, path, revnum):
        mutter('svn check-path -r%d %s' % (revnum, path))
        return self.actual.check_path(path, revnum)

    def stat(self, path, revnum):
        mutter('svn stat -r%d %s' % (revnum, path))
        return self.actual.stat(path, revnum)

    def has_capability(self, cap):
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

    def get_log(self, callback, paths, from_revnum, to_revnum, limit, *args, **kwargs):
        mutter('svn log -r%d:%d %r (limit: %r)' % (from_revnum, to_revnum, paths, limit))
        return self.actual.get_log(callback, paths,
                    from_revnum, to_revnum, limit, *args, **kwargs)

    def iter_log(self, paths, from_revnum, to_revnum, limit, *args, **kwargs):
        mutter('svn iter-log -r%d:%d %r (limit: %r)' % (from_revnum, to_revnum, paths, limit))
        return self.actual.iter_log(paths,
                    from_revnum, to_revnum, limit, *args, **kwargs)

    def change_rev_prop(self, revnum, name, value):
        mutter('svn change-revprop -r%d %s=%s' % (revnum, name, value))
        return self.actual.change_rev_prop(revnum, name, value)

    def get_dir(self, path, revnum=-1, fields=0):
        mutter('svn get-dir -r%d %s' % (revnum, path))
        return self.actual.get_dir(path, revnum, fields)

    def get_file(self, path, stream, revnum):
        mutter('svn get-file -r%d %s' % (revnum, path))
        return self.actual.get_file(path, stream, revnum)

    def get_file_revs(self, path, start_revnum, end_revnum, handler):
        mutter('svn get-file-revs -r%d:%d %s' % (start_revnum, end_revnum, path))
        return self.actual.get_file_revs(path, start_revnum, end_revnum,
            handler)

    def revprop_list(self, revnum):
        mutter('svn revprop-list -r%d' % (revnum,))
        return self.actual.revprop_list(revnum)

    def get_locations(self, path, peg_revnum, revnums):
        mutter('svn get_locations -r%d %s (%r)' % (peg_revnum, path, revnums))
        return self.actual.get_locations(path, peg_revnum, revnums)

    def do_update(self, revnum, path, start_empty, editor):
        mutter("svn update -r%d %s" % (revnum, path))
        return self.actual.do_update(revnum, path, start_empty, editor)

    def do_diff(self, revision_to_update, diff_target, versus_url,
                diff_editor, recurse=True, ignore_ancestry=False, text_deltas=False,
                depth=None):
        mutter("svn diff -r%d %s -> %s" % (revision_to_update, diff_target, versus_url))
        return self.actual.do_diff(revision_to_update, diff_target, versus_url,
                diff_editor, recurse, ignore_ancestry, text_deltas)

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

    def replay_range(self, start_revision, end_revision, low_water_mark, cbs,
                     send_deltas=True):
        mutter("svn replay-range %d -> %d (low water mark: %d)" % (start_revision, end_revision, low_water_mark))
        return self.actual.replay_range(start_revision, end_revision, low_water_mark, cbs,
                   send_deltas)

    def replay(self, revision, low_water_mark, editor, send_deltas=True):
        mutter("svn replay %d (low water mark: %d)" % (revision, low_water_mark))
        return self.actual.replay(revision, low_water_mark, editor, send_deltas)


def create_branch_prefix(transport, revprops, bp_parts, existing_bp_parts=None):
    """Create a branch prefixes (e.g. "branches")

    :param transport: Subversion transport
    :param revprops: Revision properties to set
    :param bp_parts: Branch path elements that should be created (list of names,
        ["branches", "foo"] for "branches/foo")
    :param existing_bp_parts: Branch path elements that already exist.
    """
    conn = transport.get_connection()
    if existing_bp_parts is None:
        existing_bp_parts = check_dirs_exist(conn, bp_parts, -1)
    try:
        with convert_svn_error(conn.get_commit_editor)(revprops) as ci:
            root = ci.open_root()
            name = None
            batons = [root]
            for p in existing_bp_parts:
                if name is None:
                    name = p
                else:
                    name += "/" + p
                batons.append(batons[-1].open_directory(name))
            for p in bp_parts[len(existing_bp_parts):]:
                if name is None:
                    name = p
                else:
                    name += "/" + p
                batons.append(batons[-1].add_directory(name))
            for baton in reversed(batons):
                baton.close()
    finally:
        transport.add_connection(conn)


def check_dirs_exist(transport, bp_parts, base_rev):
    """Make sure that the specified directories exist.

    :param transport: SvnRaTransport to use.
    :param bp_parts: List of directory names in the format returned by
        os.path.split()
    :param base_rev: Base revision to check.
    :return: List of the directories that exists in base_rev.
    """
    for i in range(len(bp_parts), 0, -1):
        current = bp_parts[:i]
        path = "/".join(current).strip("/")
        if transport.check_path(path, base_rev) == subvertpy.NODE_DIR:
            return current
    return []
