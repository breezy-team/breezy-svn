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


"""Subversion remote ControlDir formats."""

import urllib


from bzrlib import (
    errors,
    trace,
    version_info as bzrlib_version,
    )
from bzrlib.revision import (
    NULL_REVISION,
    )
from bzrlib.push import (
    PushResult,
    )

from bzrlib.plugins.svn.errors import (
    NoSvnRepositoryPresent,
    NotSvnBranchPath,
    )
from bzrlib.plugins.svn.format import (
    SvnRemoteFormat,
    )
from bzrlib.plugins.svn.repository import (
    SvnRepository,
    )
from bzrlib.plugins.svn.transport import (
    bzr_to_svn_url,
    get_svn_ra_transport,
    )


from bzrlib.controldir import (
    ControlDirFormat,
    ControlDir,
    format_registry,
    )


class SubversionPushResult(PushResult):

    def report(self, to_file):
        """Write a human-readable description of the result."""
        if self.branch_push_result is None:
            trace.note('Created new branch at %s.', self.target_branch_path)
        else:
            self.branch_push_result.report(to_file)

    @property
    def old_revno(self):
        if self.branch_push_result is None:
            return 0
        return self.branch_push_result.old_revno

    @property
    def old_revid(self):
        if self.branch_push_result is None:
            return NULL_REVISION
        return self.branch_push_result.old_revid


class SvnRemoteAccess(ControlDir):
    """ControlDir implementation for Subversion connections.

    This is used for all non-checkout connections
    to Subversion repositories.
    """

    @property
    def user_transport(self):
        return self.root_transport

    @property
    def control_transport(self):
        return self.root_transport

    def __init__(self, _transport, _format=None):
        """See ControlDir.__init__()."""
        _transport = get_svn_ra_transport(_transport)
        if _format is None:
            _format = SvnRemoteFormat()
        self._format = _format
        self.transport = None
        self.root_transport = _transport

        svn_url = bzr_to_svn_url(self.root_transport.base)
        self.svn_root_url = _transport.get_svn_repos_root()
        self.root_url = _transport.get_repos_root()

        assert svn_url.lower().startswith(self.svn_root_url.lower())
        self.branch_path = urllib.unquote(svn_url[len(self.svn_root_url):])

    def break_lock(self):
        pass

    def clone(self, url, revision_id=None, force_new_repo=False):
        """See ControlDir.clone().

        Not supported on Subversion connections.
        """
        raise NotImplementedError(SvnRemoteAccess.clone)

    def sprout(self, *args, **kwargs):
        if self.branch_path == "":
            guessed_layout = self.find_repository().get_guessed_layout()
            if guessed_layout is not None and not guessed_layout.is_branch(""):
                trace.warning('Cloning Subversion repository as branch. '
                        'To import the individual branches in the repository, '
                        'use "bzr svn-import".')
        return super(SvnRemoteAccess, self).sprout(*args, **kwargs)

    def is_control_filename(self, path):
        # Bare, so anything is a control file
        return True

    def open_repository(self, _unsupported=False):
        """Open the repository associated with this ControlDir.

        :return: instance of SvnRepository.
        """
        if self.branch_path == "":
            return SvnRepository(self, self.root_transport)
        raise NoSvnRepositoryPresent(self.root_transport.base)

    def find_repository(self, _ignore_branch_path=False):
        """Open the repository associated with this ControlDir.

        :return: instance of SvnRepository.
        """
        transport = self.root_transport
        if self.root_url != transport.base:
            transport = transport.clone_root()
        if _ignore_branch_path:
            return SvnRepository(self, transport)
        else:
            return SvnRepository(self, transport, self.branch_path)

    def cloning_metadir(self, require_stacking=False):
        """Produce a metadir suitable for cloning with."""
        return format_registry.make_bzrdir('default')

    def open_workingtree(self, _unsupported=False,
            recommend_upgrade=True):
        """See ControlDir.open_workingtree().

        Will always raise NotLocalUrl as this
        ControlDir can not be associated with working trees.
        """
        # Working trees never exist on remote Subversion repositories
        raise errors.NoWorkingTree(self.root_transport.base)

    def create_workingtree(self, revision_id=None, hardlink=None):
        """See ControlDir.create_workingtree().

        Will always raise NotLocalUrl as this
        ControlDir can not be associated with working trees.
        """
        raise errors.NotLocalUrl(self.root_transport.base)

    def needs_format_conversion(self, format=None):
        """See ControlDir.needs_format_conversion()."""
        # if the format is not the same as the system default,
        # an upgrade is needed.
        if format is None:
            format = ControlDirFormat.get_default_format()
        return not isinstance(self._format, format.__class__)

    def import_branch(self, source, stop_revision=None, overwrite=False,
                      _push_merged=None, _override_svn_revprops=None):
        """Create a new branch in this repository, possibly
        with the specified history, optionally importing revisions.

        :param source: Source branch
        :param stop_revision: Tip of new branch
        :return: Branch object
        """
        from bzrlib.plugins.svn.push import InterToSvnRepository
        source.lock_read()
        try:
            if stop_revision is None:
                stop_revision = source.last_revision()
            target_branch_path = self.branch_path.strip("/")
            repos = self.find_repository()
            repos.lock_write()
            try:
                inter = InterToSvnRepository(source.repository, repos)
                layout = repos.get_layout()
                try:
                    (type, project, _, ip) = layout.parse(target_branch_path)
                except NotSvnBranchPath:
                    raise errors.NotBranchError(target_branch_path)
                if type not in ('branch', 'tag') or ip != '':
                    raise errors.NotBranchError(target_branch_path)
                inter.push_new_branch(layout, project, target_branch_path,
                        stop_revision,
                        override_svn_revprops=_override_svn_revprops,
                        push_merged=_push_merged, overwrite=overwrite)
                return self.open_branch()
            finally:
                repos.unlock()
        finally:
            source.unlock()

    def create_branch(self, branch_name=None):
        """See ControlDir.create_branch()."""
        if branch_name is not None:
            from bzrlib.errors import NoColocatedBranchSupport
            raise NoColocatedBranchSupport(self)
        from bzrlib.plugins.svn.branch import SvnBranch
        repos = self.find_repository()

        if self.branch_path != "":
            # TODO: Set NULL_REVISION in SVN_PROP_BZR_BRANCHING_SCHEME
            repos.transport.mkdir(self.branch_path.strip("/"))
        elif repos.get_latest_revnum() > 0:
            # Bail out if there are already revisions in this repository
            raise errors.AlreadyBranchError(self.root_transport.base)
        branch = SvnBranch(repos, self.branch_path)
        branch.bzrdir = self
        return branch

    if bzrlib_version >= (2, 2):
        def open_branch(self, name=None, unsupported=False, 
            ignore_fallbacks=None):
            return self._open_branch(name=name,
                ignore_fallbacks=ignore_fallbacks, unsupported=unsupported)
    else:
        def open_branch(self, ignore_fallbacks=None, unsupported=False):
            return self._open_branch(name=None,
                ignore_fallbacks=ignore_fallbacks, unsupported=unsupported)

    def _open_branch(self, name=None, unsupported=True, ignore_fallbacks=False):
        """See ControlDir.open_branch()."""
        from bzrlib.plugins.svn.branch import SvnBranch
        if name is not None:
            raise errors.NoColocatedBranchSupport(self)
        repos = self.find_repository()
        branch = SvnBranch(repos, self.branch_path)
        branch.bzrdir = self
        return branch

    def create_repository(self, shared=False, format=None):
        """See ControlDir.create_repository."""
        return self.open_repository()

    def push_branch(self, source, revision_id=None, overwrite=False,
        remember=False, create_prefix=False):
        ret = SubversionPushResult()
        ret.source_branch = source
        ret.workingtree_updated = None
        ret.stacked_on = None
        ret.master_branch = None
        try:
            target_branch = self.open_branch()
            if source.get_push_location() is None or remember:
                source.set_push_location(target_branch.base)
            ret.target_branch = target_branch
            target_branch.lock_write()
            try:
                ret.branch_push_result = source.push(
                    target_branch, stop_revision=revision_id,
                    overwrite=overwrite)
            finally:
                target_branch.unlock()
        except errors.NotBranchError:
            ret.target_branch_path = "/%s" % self.branch_path.lstrip("/")
            if create_prefix:
                self.root_transport.create_prefix()
            ret.target_branch = self.import_branch(source, revision_id,
                overwrite=overwrite)
            ret.tag_conflicts = source.tags.merge_to(ret.target_branch.tags,
                overwrite)
            if source.get_push_location() is None or remember:
                source.set_push_location(ret.target_branch.base)
        return ret

    def destroy_branch(self, branch_name=None):
        if branch_name is not None:
            from bzrlib.errors import NoColocatedBranchSupport
            raise NoColocatedBranchSupport(self)
        if self.branch_path == "":
            raise errors.BzrError("Branch at root not removable.")
        raise NotImplementedError(self.destroy_branch)

    def destroy_repository(self):
        raise errors.UnsupportedOperation(self.destroy_repository, self)

    def can_convert_format(self):
        return False

    def get_config(self):
        from bzrlib.plugins.svn.config import SubversionControlDirConfig
        return SubversionControlDirConfig()
