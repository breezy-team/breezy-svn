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


"""Subversion BzrDir formats."""


import subvertpy

import bzrlib
from bzrlib import (
    trace,
    )
from bzrlib.bzrdir import (
    BzrDirFormat,
    BzrDir,
    )
from bzrlib.errors import (
    AlreadyBranchError,
    NotBranchError,
    NotLocalUrl,
    NoWorkingTree,
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
    get_rich_root_format,
    )
from bzrlib.plugins.svn.repository import (
    SvnRepository,
    )
from bzrlib.plugins.svn.transport import (
    bzr_to_svn_url,
    get_svn_ra_transport,
    )


class SubversionPushResult(PushResult):

    def report(self, to_file):
        """Write a human-readable description of the result."""
        if self.branch_push_result is None:
            trace.note('Created new branch at %s.', self.target_branch_path)
        else:
            self.branch_push_result.report(to_file)


class SvnRemoteAccess(BzrDir):
    """BzrDir implementation for Subversion connections.
    
    This is used for all non-checkout connections 
    to Subversion repositories.
    """

    def __init__(self, _transport, _format=None):
        """See BzrDir.__init__()."""
        _transport = get_svn_ra_transport(_transport)
        if _format is None:
            _format = SvnRemoteFormat()
        self._format = _format
        self.transport = None
        self.root_transport = _transport

        svn_url = bzr_to_svn_url(self.root_transport.base)
        self.svn_root_url = _transport.get_svn_repos_root()
        self.root_url = _transport.get_repos_root()

        assert svn_url.startswith(self.svn_root_url)
        self.branch_path = svn_url[len(self.svn_root_url):]

    def clone(self, url, revision_id=None, force_new_repo=False):
        """See BzrDir.clone().

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

    def open_repository(self, _unsupported=False):
        """Open the repository associated with this BzrDir.
        
        :return: instance of SvnRepository.
        """
        if self.branch_path == "":
            return SvnRepository(self, self.root_transport)
        raise NoSvnRepositoryPresent(self.root_transport.base)

    def break_lock(self):
        pass

    def find_repository(self, _ignore_branch_path=False):
        """Open the repository associated with this BzrDir.
        
        :return: instance of SvnRepository.
        """
        transport = self.root_transport
        if self.root_url != transport.base:
            transport = transport.clone_root()
        if _ignore_branch_path:
            return SvnRepository(self, transport)
        else:
            return SvnRepository(self, transport, self.branch_path)

    def cloning_metadir(self, stacked=False):
        """Produce a metadir suitable for cloning with."""
        return get_rich_root_format(stacked)

    def open_workingtree(self, _unsupported=False,
            recommend_upgrade=True):
        """See BzrDir.open_workingtree().

        Will always raise NotLocalUrl as this 
        BzrDir can not be associated with working trees.
        """
        # Working trees never exist on remote Subversion repositories
        raise NoWorkingTree(self.root_transport.base)

    def create_workingtree(self, revision_id=None, hardlink=None):
        """See BzrDir.create_workingtree().

        Will always raise NotLocalUrl as this 
        BzrDir can not be associated with working trees.
        """
        raise NotLocalUrl(self.root_transport.base)

    def needs_format_conversion(self, format=None):
        """See BzrDir.needs_format_conversion()."""
        # if the format is not the same as the system default,
        # an upgrade is needed.
        if format is None:
            format = BzrDirFormat.get_default_format()
        return not isinstance(self._format, format.__class__)

    def import_branch(self, source, stop_revision=None, _push_merged=None,
                      _override_svn_revprops=None):
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
            inter = InterToSvnRepository(source.repository, repos)
            layout = repos.get_layout()
            try:
                (type, project, _, ip) = layout.parse(target_branch_path)
            except NotSvnBranchPath:
                raise NotBranchError(branch_path)
            if type not in ('branch', 'tag') or ip != '':
                raise NotBranchError(branch_path)
            inter.push_new_branch(layout, project, target_branch_path, 
                    stop_revision, override_svn_revprops=_override_svn_revprops,
                    push_merged=_push_merged)
            return self.open_branch()
        finally:
            source.unlock()

    def create_branch(self):
        """See BzrDir.create_branch()."""
        from bzrlib.plugins.svn.branch import SvnBranch
        repos = self.find_repository()

        if self.branch_path != "":
            # TODO: Set NULL_REVISION in SVN_PROP_BZR_BRANCHING_SCHEME
            repos.transport.mkdir(self.branch_path.strip("/"))
        elif repos.get_latest_revnum() > 0:
            # Bail out if there are already revisions in this repository
            raise AlreadyBranchError(self.root_transport.base)
        branch = SvnBranch(repos, self.branch_path)
        branch.bzrdir = self
        return branch

    def open_branch(self, unsupported=True):
        """See BzrDir.open_branch()."""
        from bzrlib.plugins.svn.branch import SvnBranch
        repos = self.find_repository()
        branch = SvnBranch(repos, self.branch_path)
        branch.bzrdir = self
        return branch

    def create_repository(self, shared=False, format=None):
        """See BzrDir.create_repository."""
        return self.open_repository()

    def push_branch(self, source, revision_id=None, overwrite=False,
        remember=False):
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
        except NotBranchError:
            ret.old_revno = 0
            ret.old_revid = NULL_REVISION
            ret.target_branch_path = "/%s" % self.branch_path.lstrip("/")
            ret.target_branch = self.import_branch(source, revision_id)
            if source.get_push_location() is None or remember:
                source.set_push_location(ret.target_branch.base)
        return ret
