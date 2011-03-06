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


"""Subversion ControlDir formats."""


from bzrlib import (
    errors as bzr_errors,
    osutils,
    trace,
    )
from bzrlib.lockable_files import (
    TransportLock,
    )

from bzrlib.controldir import (
    ControlDirFormat,
    Prober,
    format_registry,
    )
from bzrlib.transport import (
    do_catching_redirections,
    )


class SvnProber(Prober):

    @classmethod
    def _check_versions(cls):
        from bzrlib.plugins.svn import lazy_check_versions
        lazy_check_versions()


class SvnControlFormat(ControlDirFormat):
    """Format for a Subversion control dir."""
    _lock_class = TransportLock

    def __init__(self):
        super(SvnControlFormat, self).__init__()
        self.__repository_format = None

    @property
    def repository_format(self):
        if self.__repository_format is None:
            from bzrlib.plugins.svn.repository import SvnRepositoryFormat
            self.__repository_format = SvnRepositoryFormat()
        return self.__repository_format

    def is_supported(self):
        """See ControlDirFormat.is_supported()."""
        return True


class SvnRemoteProber(SvnProber):

    _supported_schemes = ["http", "https", "file", "svn"]

    def probe_transport(self, transport):
        from bzrlib.transport.local import LocalTransport

        if isinstance(transport, LocalTransport):
            # Cheaper way to figure out if there is a svn repo
            maybe = False
            subtransport = transport
            while subtransport:
                try:
                    if subtransport.has("format"):
                        maybe = True
                        break
                except UnicodeEncodeError:
                    pass
                prevsubtransport = subtransport
                subtransport = prevsubtransport.clone("..")
                if subtransport.base == prevsubtransport.base:
                    break
            if not maybe:
                raise bzr_errors.NotBranchError(path=transport.base)

        try:
            scheme = transport.external_url().split(":")[0]
        except bzr_errors.InProcessTransport:
            # bzr-svn not supported on MemoryTransport
            raise bzr_errors.NotBranchError(path=transport.base)
        if (not scheme.startswith("svn+") and
            not scheme in self._supported_schemes):
            raise bzr_errors.NotBranchError(path=transport.base)

        self._check_versions()
        from bzrlib.plugins.svn.transport import get_svn_ra_transport
        from bzrlib.plugins.svn.errors import DavRequestFailed
        import subvertpy
        try:
            transport = get_svn_ra_transport(transport)
        except subvertpy.SubversionException, (msg, num):
            if num == subvertpy.ERR_RA_DAV_NOT_VCC:
                raise bzr_errors.NotBranchError(path=transport.base)
            if num in (subvertpy.ERR_RA_ILLEGAL_URL, \
                       subvertpy.ERR_RA_LOCAL_REPOS_OPEN_FAILED, \
                       subvertpy.ERR_BAD_URL):
                trace.mutter("Unable to open %r with Subversion: %s",
                    transport, msg)
                raise bzr_errors.NotBranchError(path=transport.base)
            raise
        except bzr_errors.InProcessTransport:
            raise bzr_errors.NotBranchError(path=transport.base)
        except bzr_errors.NoSuchFile:
            raise bzr_errors.NotBranchError(path=transport.base)
        except bzr_errors.InvalidURL:
            raise bzr_errors.NotBranchError(path=transport.base)
        except bzr_errors.InvalidHttpResponse:
            raise bzr_errors.NotBranchError(path=transport.base)
        except DavRequestFailed, e:
            if "501 Unsupported method" in e.msg:
                raise bzr_errors.NotBranchError(path=transport.base)
            else:
                raise

        return SvnRemoteFormat()


class SvnRemoteFormat(SvnControlFormat):
    """Format for the Subversion smart server."""

    supports_workingtrees = False

    def __init__(self):
        super(SvnRemoteFormat, self).__init__()

    def get_branch_format(self):
        from bzrlib.plugins.svn.branch import SvnBranchFormat
        return SvnBranchFormat()

    def open(self, transport, _found=False):
        import subvertpy
        from bzrlib.plugins.svn import remote
        try:
            return remote.SvnRemoteAccess(transport, self)
        except subvertpy.SubversionException, (_, num):
            if num in (subvertpy.ERR_RA_DAV_REQUEST_FAILED,
                       subvertpy.ERR_RA_DAV_NOT_VCC):
                raise bzr_errors.NotBranchError(transport.base)
            if num == subvertpy.ERR_XML_MALFORMED:
                # This *could* be an indication of an actual corrupt
                # svn server, but usually it just means a broken
                # xml page
                raise bzr_errors.NotBranchError(transport.base)
            raise

    def network_name(self):
        return "subversion"

    def get_format_string(self):
        raise NotImplementedError(self.get_format_string)

    def get_format_description(self):
        return 'Subversion Smart Server'

    def initialize_on_transport_ex(self, transport, use_existing_dir=False,
        create_prefix=False, force_new_repo=False, stacked_on=None,
        stack_on_pwd=None, repo_format_name=None, make_working_trees=None,
        shared_repo=False, vfs_only=False):
        from bzrlib.bzrdir import CreateRepository
        def make_directory(transport):
            transport.mkdir('.')
            return transport
        def redirected(transport, e, redirection_notice):
            trace.note(redirection_notice)
            return transport._redirected_to(e.source, e.target)
        try:
            transport = do_catching_redirections(make_directory, transport,
                redirected)
        except bzr_errors.FileExists:
            if not use_existing_dir:
                raise
        except bzr_errors.NoSuchFile:
            if not create_prefix:
                raise
            transport.create_prefix()

        controldir = self.initialize_on_transport(transport)
        repository = controldir.open_repository()
        repository.lock_write()
        return (repository, controldir, None, CreateRepository(controldir))

    def initialize_on_transport(self, transport):
        """See ControlDir.initialize_on_transport()."""
        from bzrlib.plugins.svn import lazy_check_versions
        lazy_check_versions()
        from bzrlib.plugins.svn.transport import get_svn_ra_transport
        from bzrlib.transport.local import LocalTransport
        import os
        from subvertpy import repos

        if not isinstance(transport, LocalTransport):
            raise NotImplementedError(self.initialize,
                "Can't create Subversion Repositories/branches on "
                "non-local transports")

        local_path = transport.local_abspath(".").rstrip("/").encode(osutils._fs_enc)
        assert type(local_path) == str
        repos.create(local_path)
        # All revision property changes
        revprop_hook = os.path.join(local_path, "hooks", "pre-revprop-change")
        open(revprop_hook, 'w').write("#!/bin/sh")
        os.chmod(revprop_hook, os.stat(revprop_hook).st_mode | 0111)
        return self.open(get_svn_ra_transport(transport), _found=True)


class SvnWorkingTreeProber(SvnProber):

    def probe_transport(self, transport):
        from bzrlib.transport.local import LocalTransport

        if isinstance(transport, LocalTransport) and transport.has(".svn"):
            self._check_versions()
            return SvnWorkingTreeDirFormat()

        raise bzr_errors.NotBranchError(path=transport.base)


class SvnWorkingTreeDirFormat(SvnControlFormat):
    """Working Tree implementation that uses Subversion working copies."""

    def open(self, transport, _found=False):
        from bzrlib.plugins.svn.workingtree import SvnCheckout
        from bzrlib.plugins.svn import errors
        import subvertpy
        try:
            return SvnCheckout(transport, self)
        except subvertpy.SubversionException, (_, num):
            if num in (subvertpy.ERR_RA_LOCAL_REPOS_OPEN_FAILED,):
                raise errors.NoSvnRepositoryPresent(transport.base)
            raise

    def get_format_string(self):
        raise NotImplementedError(self.get_format_string)

    def get_format_description(self):
        return 'Subversion Local Checkout'

    def initialize_on_transport(self, transport):
        raise bzr_errors.UninitializableFormat(self)

    def initialize_on_transport_ex(self, transport, use_existing_dir=False,
        create_prefix=False, force_new_repo=False, stacked_on=None,
        stack_on_pwd=None, repo_format_name=None, make_working_trees=None,
        shared_repo=False, vfs_only=False):
        raise bzr_errors.UninitializableFormat(self)

    def get_converter(self, format=None):
        """See ControlDirFormat.get_converter()."""
        if format is None:
            format = format_registry.make_bzrdir('default')
        from bzrlib.plugins.svn.workingtree import SvnCheckoutConverter
        return SvnCheckoutConverter(format)
