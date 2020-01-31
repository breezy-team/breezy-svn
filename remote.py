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

from __future__ import absolute_import

import urllib

from breezy import (
    errors,
    osutils,
    trace,
    urlutils,
    )
from breezy.branch import (
    InterBranch,
    UnstackableBranchFormat,
    )
from breezy.bzr.bzrdir import CreateRepository
from breezy.controldir import (
    ControlDirFormat,
    ControlDir,
    format_registry,
    )
from breezy.lockable_files import (
    TransportLock,
    )
from breezy.revision import (
    NULL_REVISION,
    )
from breezy.push import (
    PushResult,
    )
from breezy.transport import (
    do_catching_redirections,
    get_transport,
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


class UninitializableOnRemoteTransports(errors.UninitializableFormat):

    _fmt = "Format %(format)s can not be initialised on non-local transports."


class SvnRemoteFormat(ControlDirFormat):
    """Format for the Subversion smart server."""

    colocated_branches = True
    supports_workingtrees = False
    fixed_components = True
    _lock_class = TransportLock

    def __eq__(self, other):
        return isinstance(other, SvnRemoteFormat)

    @property
    def repository_format(self):
        from .repository import SvnRepositoryFormat
        return SvnRepositoryFormat()

    def is_supported(self):
        """See ControlDirFormat.is_supported()."""
        return True

    def get_branch_format(self):
        from .branch import SvnBranchFormat
        return SvnBranchFormat()

    def open(self, transport, _found=False):
        import subvertpy
        try:
            return SvnRemoteAccess(transport, self)
        except subvertpy.SubversionException as e:
            if e.args[1] in (
                    subvertpy.ERR_RA_DAV_REQUEST_FAILED,
                    subvertpy.ERR_RA_DAV_NOT_VCC,
                    subvertpy.ERR_RA_LOCAL_REPOS_OPEN_FAILED):
                raise errors.NotBranchError(transport.base)
            if e.args[1] == subvertpy.ERR_XML_MALFORMED:
                # This *could* be an indication of an actual corrupt
                # svn server, but usually it just means a broken
                # xml page
                raise errors.NotBranchError(transport.base)
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
        def make_directory(transport):
            transport.mkdir('.')
            return transport
        def redirected(transport, e, redirection_notice):
            trace.note(redirection_notice)
            return transport._redirected_to(e.source, e.target)
        try:
            transport = do_catching_redirections(make_directory, transport,
                redirected)
        except errors.FileExists:
            if not use_existing_dir:
                raise
        except errors.NoSuchFile:
            if not create_prefix:
                raise
            transport.create_prefix()

        controldir = self.initialize_on_transport(transport)
        if repo_format_name is not None:
            repository = controldir.open_repository()
            repository.lock_write()
        else:
            repository = None
        return (repository, controldir, None, CreateRepository(controldir))

    def initialize_on_transport(self, transport):
        """See ControlDir.initialize_on_transport()."""
        from . import lazy_check_versions
        lazy_check_versions()
        from breezy.transport.local import LocalTransport
        import os
        import subvertpy
        from subvertpy import repos
        # For subvertpy < 0.8.6
        ERR_REPOS_BAD_ARGS = getattr(subvertpy, "ERR_REPOS_BAD_ARGS", 165002)

        if not isinstance(transport, LocalTransport):
            raise UninitializableOnRemoteTransports(self)

        local_path = transport.local_abspath(".").rstrip("/").encode(
            osutils._fs_enc)
        try:
            repos.create(local_path)
        except subvertpy.SubversionException as e:
            if e.args[1] == subvertpy.ERR_DIR_NOT_EMPTY:
                raise errors.BzrError("Directory is not empty")
            if e.args[1] == ERR_REPOS_BAD_ARGS:
                raise errors.AlreadyControlDirError(local_path)
            raise
        # All revision property changes
        revprop_hook = os.path.join(
            local_path, b"hooks", b"pre-revprop-change")
        open(revprop_hook, 'w').write("#!/bin/sh")
        os.chmod(revprop_hook, os.stat(revprop_hook).st_mode | 0o111)
        return self.open(transport, _found=True)

    def supports_transport(self, transport):
        try:
            url = transport.external_url()
        except errors.InProcessTransport:
            return False
        if url.startswith("file:") or url.startswith("svn+"):
            return True
        if url.startswith("http:") or url.startswith("https:"):
            # FIXME: Check that the server is running SVN
            return False
        return False


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
        from .transport import bzr_to_svn_url, get_svn_ra_transport
        svn_transport = get_svn_ra_transport(_transport)
        if _format is None:
            _format = SvnRemoteFormat()
        self._format = _format
        self._config = None
        self.svn_transport = svn_transport
        self.transport = None
        self.root_transport = _transport

        self.svn_url, readonly = bzr_to_svn_url(self.root_transport.base)
        self.svn_root_url = svn_transport.get_svn_repos_root()
        self.root_url = svn_transport.get_repos_root()

        if not self.svn_url.lower().startswith(self.svn_root_url.lower()):
            raise AssertionError("SVN URL %r does not start with root %r" %
                (self.svn_url, self.svn_root_url))

        self._branch_path = urllib.unquote(self.svn_url[len(self.svn_root_url):])

    def break_lock(self):
        pass

    def clone_on_transport(self, transport, revision_id=None,
        force_new_repo=False, preserve_stacking=False, stacked_on=None,
        create_prefix=False, use_existing_dir=True, no_tree=False):
        """Clone this controldir and its contents to transport verbatim.

        :param transport: The transport for the location to produce the clone
            at.  If the target directory does not exist, it will be created.
        :param revision_id: The tip revision-id to use for any branch or
            working tree.  If not None, then the clone operation may tune
            itself to download less data.
        :param force_new_repo: Do not use a shared repository for the target,
                               even if one is available.
        :param preserve_stacking: When cloning a stacked branch, stack the
            new branch on top of the other branch's stacked-on branch.
        :param create_prefix: Create any missing directories leading up to
            to_transport.
        :param use_existing_dir: Use an existing directory if one exists.
        :param no_tree: If set to true prevents creation of a working tree.
        """
        if stacked_on is not None:
            raise UnstackableBranchFormat(
                self._format.get_branch_format(), self.user_url)
        if create_prefix:
            transport.create_prefix()
        elif not use_existing_dir:
            transport.mkdir(".")
        target = SvnRemoteFormat().initialize_on_transport(transport)
        target_repo = target.open_repository()
        source_repo = self.find_repository()
        # FIXME: This should ideally use the same mechanism as svnsync,
        # or at least copy all colocated branches, too.
        try:
            branch = self.open_branch()
        except errors.NotBranchError:
            target_repo.fetch(source_repo)
        else:
            if revision_id is None:
                revision_id = branch.last_revision()
            target_repo.fetch(source_repo, revision_id=revision_id)
            target.push_branch(branch, revision_id=revision_id)
        return target

    def sprout(self, url, revision_id=None, force_new_repo=False,
               recurse='down', possible_transports=None,
               accelerator_tree=None, hardlink=False, stacked=False,
               source_branch=None, create_tree_if_local=True):
        from breezy.repository import InterRepository
        from breezy.transport.local import LocalTransport
        relpath = self._determine_relpath(None)
        if relpath == u"":
            guessed_layout = self.find_repository().get_guessed_layout()
            if guessed_layout is not None and not guessed_layout.is_branch(u""):
                trace.warning('Cloning Subversion repository as branch. '
                        'To import the individual branches in the repository, '
                        'use "bzr svn-import".')
        target_transport = get_transport(url, possible_transports)
        target_transport.ensure_base()
        require_colocated = ("branch" in target_transport.get_segment_parameters())
        cloning_format = self.cloning_metadir(require_colocated=require_colocated)
        # Create/update the result branch
        result = cloning_format.initialize_on_transport(target_transport)

        source_repository = self.find_repository()
        if force_new_repo:
            result_repo = result.create_repository()
            target_is_empty = True
        else:
            try:
                result_repo = result.find_repository()
            except errors.NoRepositoryPresent:
                result_repo = result.create_repository()
                target_is_empty = True
            else:
                target_is_empty = None # Unknown
        if stacked:
            raise UnstackableBranchFormat(self._format.get_branch_format(),
                self.root_transport.base)
        interrepo = InterRepository.get(source_repository, result_repo)
        try:
            source_branch = self.open_branch()
        except errors.NotBranchError:
            source_branch = None
            project = None
            mapping = None
        else:
            project = source_branch.project
            mapping = source_branch.mapping
        interrepo.fetch(revision_id=revision_id,
            project=project, mapping=mapping,
            target_is_empty=target_is_empty,
            exclude_non_mainline=False)
        if source_branch is not None:
            if revision_id is None:
                revision_id = source_branch.last_revision()
            result_branch = source_branch.sprout(result,
                revision_id=revision_id, repository=result_repo)
            interbranch = InterBranch.get(source_branch, result_branch)
            interbranch.fetch(stop_revision=revision_id,
                    exclude_non_mainline=False) # For the tags
        else:
            result_branch = result.create_branch()
        if (create_tree_if_local and isinstance(target_transport, LocalTransport)
            and (result_repo is None or result_repo.make_working_trees())):
            result.create_workingtree(accelerator_tree=accelerator_tree,
                hardlink=hardlink, from_branch=result_branch)
        return result

    def is_control_filename(self, path):
        # Bare, so anything is a control file
        return True

    def open_repository(self, _unsupported=False):
        """Open the repository associated with this ControlDir.

        :return: instance of SvnRepository.
        """
        from .errors import NoSvnRepositoryPresent
        from .repository import SvnRepository
        if self._branch_path == "":
            return SvnRepository(self, self.root_transport, self.svn_transport)
        raise NoSvnRepositoryPresent(self.root_transport.base)

    def _find_or_create_repository(self, force_new_repo=False):
        return self.find_repository()

    def find_repository(self, _ignore_branch_path=False):
        """Open the repository associated with this ControlDir.

        :return: instance of SvnRepository.
        """
        from .repository import SvnRepository
        from .transport import get_svn_ra_transport
        if self.root_url != self.root_transport.base:
            transport = get_transport(self.root_url, possible_transports=[self.root_transport])
            svn_transport = get_svn_ra_transport(transport)
        else:
            transport = self.root_transport
            svn_transport = self.svn_transport
        if _ignore_branch_path:
            return SvnRepository(self, transport, svn_transport)
        else:
            return SvnRepository(self, transport, svn_transport, self._branch_path)

    def cloning_metadir(self, require_stacking=False, require_colocated=False):
        """Produce a metadir suitable for cloning with."""
        ret = format_registry.make_controldir('default')
        if require_colocated and not ret.colocated_branches:
            ret = format_registry.make_controldir('development-colo')
        return ret

    def open_workingtree(self, unsupported=False,
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
        raise errors.UnsupportedOperation(self.create_workingtree, self)

    def needs_format_conversion(self, format):
        """See ControlDir.needs_format_conversion()."""
        return not isinstance(self._format, format.__class__)

    def import_branch(self, source, stop_revision=None, overwrite=False,
            name=None, lossy=False):
        """Create a new branch in this repository, possibly
        with the specified history, optionally importing revisions.

        :param source: Source branch
        :param stop_revision: Tip of new branch
        :return: Branch object
        """
        from .errors import NotSvnBranchPath
        from .push import InterToSvnRepository
        with source.lock_read():
            if stop_revision is None:
                stop_revision = source.last_revision()
            if stop_revision == NULL_REVISION:
                return self.create_branch()
            relpath = self._determine_relpath(name)
            target_branch_path = relpath.lstrip("/")
            repos = self.find_repository()
            with repos.lock_write():
                inter = InterToSvnRepository(source.repository, repos)
                layout = repos.get_layout()
                try:
                    project = layout.get_branch_project(target_branch_path)
                except NotSvnBranchPath:
                    raise errors.NotBranchError(target_branch_path)
                inter.push_new_branch(layout, project, target_branch_path,
                        stop_revision, push_metadata=(not lossy), overwrite=overwrite)
                return self.open_branch(name)

    def _determine_relpath(self, branch_name):
        from .errors import NoCustomBranchPaths
        repos = self.find_repository()
        layout = repos.get_layout()
        if branch_name is None and getattr(self, "_get_selected_branch", False):
            branch_name = self._get_selected_branch()
        if branch_name == "" and layout.is_branch_or_tag(self._branch_path):
            return self._branch_path
        try:
            return layout.get_branch_path(branch_name, self._branch_path)
        except NoCustomBranchPaths:
            if branch_name == "":
                return self._branch_path
            else:
                raise errors.NoColocatedBranchSupport(layout)

    def create_branch(self, name=None, repository=None, mapping=None,
            lossy=False, append_revisions_only=None):
        """See ControlDir.create_branch()."""
        from .branch import SvnBranch
        from .push import (
            check_dirs_exist,
            create_branch_container,
            create_branch_with_hidden_commit,
            )
        if repository is None:
            repository = self.find_repository()

        repository.lock_write()
        try:
            if mapping is None:
                mapping = repository.get_mapping()

            if name is not None and "/" in name:
                raise errors.InvalidBranchName(name)
            relpath = self._determine_relpath(name).strip("/")
            if relpath == "":
                if repository.get_latest_revnum() > 0:
                    # Bail out if there are already revisions in this repository
                    raise errors.AlreadyBranchError(repository.transport.base)
                # TODO: Set NULL_REVISION in SVN_PROP_BZR_BRANCHING_SCHEME on rev0
            bp_parts = relpath.split("/")
            existing_bp_parts = check_dirs_exist(repository.svn_transport, bp_parts,
                -1)
            if len(existing_bp_parts) == len(bp_parts) and relpath != "":
                raise errors.AlreadyBranchError(repository.transport.base)
            if len(existing_bp_parts) < len(bp_parts)-1:
                create_branch_container(repository.svn_transport, relpath,
                    "/".join(existing_bp_parts))
            if relpath != "":
                create_branch_with_hidden_commit(repository, relpath,
                    NULL_REVISION, set_metadata=(not lossy))
            branch = SvnBranch(repository, self, relpath, mapping)
            if append_revisions_only == False:
                branch.set_append_revisions_only(False)
            return branch
        finally:
            repository.unlock()

    def get_branch_reference(self, name=None):
        """See ControlDir.get_branch_reference()."""
        # No branch is a reference branch, but we should
        # still raise the appropriate errors if there is no
        # branch with the specified name.
        self.open_branch(name=name)
        return None

    def set_branch_reference(self, target_branch, name=None):
        # Sorry, no branch references.
        raise errors.IncompatibleFormat(target_branch._format, self._format)

    def open_branch(self, name=None, unsupported=True, ignore_fallbacks=False,
            mapping=None, branch_path=None, repository=None, revnum=None,
            possible_transports=None, project=None):
        """See ControlDir.open_branch()."""
        from .branch import SvnBranch
        if branch_path is None:
            branch_path = self._determine_relpath(name)
        if repository is None:
            repository = self.find_repository()
        if mapping is None:
            mapping = repository.get_mapping()
        return SvnBranch(repository, self, branch_path, mapping, revnum=revnum,
            project=project)

    def create_repository(self, shared=None, format=None):
        """See ControlDir.create_repository."""
        if shared:
            from .repository import SvnRepositoryFormat
            raise errors.IncompatibleFormat(
                SvnRepositoryFormat(), self._format)
        return self.open_repository()

    def push_branch(self, source, revision_id=None, overwrite=False,
        remember=False, create_prefix=False, name=None, lossy=False):
        from .branch import SvnBranch
        if lossy and isinstance(source, SvnBranch):
            raise errors.LossyPushToSameVCS(source, self)
        ret = SubversionPushResult()
        ret.source_branch = source
        ret.workingtree_updated = None
        ret.stacked_on = None
        ret.master_branch = None
        try:
            target_branch = self.open_branch(name=name)
            if source.get_push_location() is None or remember:
                source.set_push_location(target_branch.base)
            ret.target_branch = target_branch
            with target_branch.lock_write():
                ret.branch_push_result = source.push(
                    target_branch, stop_revision=revision_id,
                    overwrite=overwrite, lossy=lossy)
        except errors.NotBranchError:
            if create_prefix:
                self.svn_transport.create_prefix()
            ret.target_branch = self.import_branch(source, revision_id,
                overwrite=overwrite, lossy=lossy)
            ret.target_branch_path = "/" + ret.target_branch.get_branch_path()
            tag_ret = source.tags.merge_to(ret.target_branch.tags,
                overwrite)
            if isinstance(tag_ret, tuple):
                (ret.tag_updates, ret.tag_conflicts) = tag_ret
            else:
                ret.tag_conflicts = tag_ret
            if source.get_push_location() is None or remember:
                source.set_push_location(ret.target_branch.base)
        return ret

    def destroy_branch(self, branch_name=None):
        import subvertpy
        relpath = self._determine_relpath(branch_name)
        if relpath == "":
            raise errors.UnsupportedOperation(self.destroy_branch, self)
        dirname, basename = urlutils.split(relpath)
        conn = self.svn_transport.get_connection(dirname.strip("/"))
        try:
            with conn.get_commit_editor({"svn:log": "Remove branch."}) as ce:
                with ce.open_root() as root:
                    try:
                        root.delete_entry(basename)
                    except subvertpy.SubversionException as e:
                        if e.args[1] == subvertpy.ERR_FS_TXN_OUT_OF_DATE:
                            # Make sure the branch still exists
                            self.open_branch(branch_name)
                        raise
        finally:
            self.svn_transport.add_connection(conn)

    def destroy_repository(self):
        raise errors.UnsupportedOperation(self.destroy_repository, self)

    def can_convert_format(self):
        return False

    def get_config(self):
        from .config import SvnRepositoryConfig
        if self._config is None:
            self._config = SvnRepositoryConfig(self.root_transport.base,
                self.svn_transport.get_uuid())
        return self._config

    def list_branches(self):
        return self.get_branches().values()

    def get_branches(self):
        repos = self.find_repository()
        layout = repos.get_layout()
        branches = {}
        for project, bp, nick, has_props, revnum in layout.get_branches(repos,
                repos.get_latest_revnum()):
            b = self.open_branch(branch_path=bp, repository=repos,
                project=project)
            branches[b.name] = b
        return branches

