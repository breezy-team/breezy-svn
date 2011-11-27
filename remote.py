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
    osutils,
    trace,
    urlutils,
    )
from bzrlib.branch import InterBranch
from bzrlib.controldir import (
    ControlDirFormat,
    ControlDir,
    format_registry,
    )
from bzrlib.lockable_files import (
    TransportLock,
    )
from bzrlib.revision import (
    NULL_REVISION,
    )
from bzrlib.push import (
    PushResult,
    )
from bzrlib.transport import (
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
        from bzrlib.plugins.svn.repository import SvnRepositoryFormat
        return SvnRepositoryFormat()

    def is_supported(self):
        """See ControlDirFormat.is_supported()."""
        return True

    def get_branch_format(self):
        from bzrlib.plugins.svn.branch import SvnBranchFormat
        return SvnBranchFormat()

    def open(self, transport, _found=False):
        import subvertpy
        try:
            return SvnRemoteAccess(transport, self)
        except subvertpy.SubversionException, (_, num):
            if num in (subvertpy.ERR_RA_DAV_REQUEST_FAILED,
                       subvertpy.ERR_RA_DAV_NOT_VCC,
                       subvertpy.ERR_RA_LOCAL_REPOS_OPEN_FAILED):
                raise errors.NotBranchError(transport.base)
            if num == subvertpy.ERR_XML_MALFORMED:
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
        from bzrlib.plugins.svn import lazy_check_versions
        lazy_check_versions()
        from bzrlib.transport.local import LocalTransport
        import os
        import subvertpy
        from subvertpy import repos
        # For subvertpy < 0.8.6

        if not isinstance(transport, LocalTransport):
            raise UninitializableOnRemoteTransports(self)

        local_path = transport.local_abspath(".").rstrip("/").encode(osutils._fs_enc)
        assert type(local_path) == str
        try:
            repos.create(local_path)
        except subvertpy.SubversionException, (_, num):
            if num == subvertpy.ERR_DIR_NOT_EMPTY:
                raise errors.BzrError("Directory is not empty")
            raise
        # All revision property changes
        revprop_hook = os.path.join(local_path, "hooks", "pre-revprop-change")
        open(revprop_hook, 'w').write("#!/bin/sh")
        os.chmod(revprop_hook, os.stat(revprop_hook).st_mode | 0111)
        from bzrlib.plugins.svn.transport import get_svn_ra_transport
        return self.open(get_svn_ra_transport(transport), _found=True)

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
        from bzrlib.plugins.svn.transport import bzr_to_svn_url, get_svn_ra_transport
        _transport = get_svn_ra_transport(_transport)
        if _format is None:
            _format = SvnRemoteFormat()
        self._format = _format
        self._config = None
        self.transport = None
        self.root_transport = _transport

        self.svn_url, readonly = bzr_to_svn_url(self.root_transport.base)
        self.svn_root_url = _transport.get_svn_repos_root()
        self.root_url = _transport.get_repos_root()

        assert self.svn_url.lower().startswith(self.svn_root_url.lower())
        self._branch_path = urllib.unquote(self.svn_url[len(self.svn_root_url):])

    def break_lock(self):
        pass

    def clone_on_transport(self, transport, revision_id=None,
        force_new_repo=False, preserve_stacking=False, stacked_on=None,
        create_prefix=False, use_existing_dir=True, no_tree=False):
        """Clone this bzrdir and its contents to transport verbatim.

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
            raise errors.UnstackableBranchFormat(
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
        from bzrlib.repository import InterRepository
        from bzrlib.transport.local import LocalTransport
        relpath = self._determine_relpath(None)
        if relpath == "":
            guessed_layout = self.find_repository().get_guessed_layout()
            if guessed_layout is not None and not guessed_layout.is_branch(""):
                trace.warning('Cloning Subversion repository as branch. '
                        'To import the individual branches in the repository, '
                        'use "bzr svn-import".')
        target_transport = get_transport(url, possible_transports)
        target_transport.ensure_base()
        cloning_format = self.cloning_metadir()
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
            raise errors.UnstackableBranchFormat(self._format.get_branch_format(),
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
        from bzrlib.plugins.svn.errors import NoSvnRepositoryPresent
        from bzrlib.plugins.svn.repository import SvnRepository
        if self._branch_path == "":
            return SvnRepository(self, self.root_transport)
        raise NoSvnRepositoryPresent(self.root_transport.base)

    def _find_or_create_repository(self, force_new_repo=False):
        return self.find_repository()

    def find_repository(self, _ignore_branch_path=False):
        """Open the repository associated with this ControlDir.

        :return: instance of SvnRepository.
        """
        from bzrlib.plugins.svn.repository import SvnRepository
        transport = self.root_transport
        if self.root_url != transport.base:
            transport = transport.clone_root()
        if _ignore_branch_path:
            return SvnRepository(self, transport)
        else:
            return SvnRepository(self, transport, self._branch_path)

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
        raise errors.UnsupportedOperation(self.create_workingtree, self)

    def needs_format_conversion(self, format):
        """See ControlDir.needs_format_conversion()."""
        return not isinstance(self._format, format.__class__)

    def import_branch(self, source, stop_revision=None, overwrite=False, name=None):
        """Create a new branch in this repository, possibly
        with the specified history, optionally importing revisions.

        :param source: Source branch
        :param stop_revision: Tip of new branch
        :return: Branch object
        """
        from bzrlib.plugins.svn.errors import NotSvnBranchPath
        from bzrlib.plugins.svn.push import InterToSvnRepository
        source.lock_read()
        try:
            if stop_revision is None:
                stop_revision = source.last_revision()
            if stop_revision == NULL_REVISION:
                return self.create_branch()
            relpath = self._determine_relpath(name)
            target_branch_path = relpath.lstrip("/")
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
                        stop_revision, push_metadata=True, overwrite=overwrite)
                return self.open_branch(name)
            finally:
                repos.unlock()
        finally:
            source.unlock()

    def _determine_relpath(self, branch_name):
        from bzrlib.plugins.svn.errors import NoCustomBranchPaths
        repos = self.find_repository()
        layout = repos.get_layout()
        if branch_name is None and getattr(self, "_get_selected_branch", False):
            branch_name = self._get_selected_branch()
        if branch_name is None and layout.is_branch_or_tag(self._branch_path):
            return self._branch_path
        try:
            if branch_name is not None:
                branch_name = osutils.safe_utf8(branch_name)
            return layout.get_branch_path(branch_name, self._branch_path)
        except NoCustomBranchPaths:
            if branch_name is None:
                return self._branch_path
            else:
                raise errors.NoColocatedBranchSupport(layout)

    def create_branch(self, name=None, repository=None, mapping=None,
            lossy=False, append_revisions_only=None):
        """See ControlDir.create_branch()."""
        from bzrlib.plugins.svn.branch import SvnBranch
        from bzrlib.plugins.svn.push import (
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

            relpath = self._determine_relpath(name).strip("/")
            if relpath == "":
                if repository.get_latest_revnum() > 0:
                    # Bail out if there are already revisions in this repository
                    raise errors.AlreadyBranchError(repository.transport.base)
                # TODO: Set NULL_REVISION in SVN_PROP_BZR_BRANCHING_SCHEME on rev0
            bp_parts = relpath.split("/")
            existing_bp_parts = check_dirs_exist(repository.transport, bp_parts,
                -1)
            if len(existing_bp_parts) == len(bp_parts) and relpath != "":
                raise errors.AlreadyBranchError(repository.transport.base)
            if len(existing_bp_parts) < len(bp_parts)-1:
                create_branch_container(repository.transport, relpath, "/".join(existing_bp_parts))
            if relpath != "":
                create_branch_with_hidden_commit(repository, relpath, NULL_REVISION,
                        set_metadata=(not lossy))
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
        self.open_branch()
        return None

    def open_branch(self, name=None, unsupported=True, ignore_fallbacks=False,
            mapping=None, branch_path=None, repository=None, revnum=None):
        """See ControlDir.open_branch()."""
        from bzrlib.plugins.svn.branch import SvnBranch
        if branch_path is None:
            branch_path = self._determine_relpath(name)
        if repository is None:
            repository = self.find_repository()
        if mapping is None:
            mapping = repository.get_mapping()
        return SvnBranch(repository, self, branch_path, mapping, revnum=revnum)

    def create_repository(self, shared=None, format=None):
        """See ControlDir.create_repository."""
        if shared == False:
            from bzrlib.plugins.svn.repository import SvnRepositoryFormat
            raise errors.IncompatibleFormat(
                SvnRepositoryFormat(), self._format)
        return self.open_repository()

    def push_branch(self, source, revision_id=None, overwrite=False,
        remember=False, create_prefix=False, name=None):
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
            target_branch.lock_write()
            try:
                ret.branch_push_result = source.push(
                    target_branch, stop_revision=revision_id,
                    overwrite=overwrite)
            finally:
                target_branch.unlock()
        except errors.NotBranchError:
            if create_prefix:
                self.root_transport.create_prefix()
            ret.target_branch = self.import_branch(source, revision_id,
                overwrite=overwrite)
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
        relpath = self._determine_relpath(branch_name)
        if relpath == "":
            raise errors.UnsupportedOperation(self.destroy_branch, self)
        dirname, basename = urlutils.split(relpath)
        conn = self.root_transport.get_connection(dirname.strip("/"))
        try:
            ce = conn.get_commit_editor({"svn:log": "Remove branch."})
            try:
                root = ce.open_root()
                root.delete_entry(basename)
                root.close()
            except:
                ce.abort()
                raise
            ce.close()
        finally:
            self.root_transport.add_connection(conn)

    def destroy_repository(self):
        raise errors.UnsupportedOperation(self.destroy_repository, self)

    def can_convert_format(self):
        return False

    def get_config(self):
        from bzrlib.plugins.svn.config import SvnRepositoryConfig
        if self._config is None:
            self._config = SvnRepositoryConfig(self.root_transport.base,
                self.root_transport.get_uuid())
        return self._config

    def list_branches(self):
        repos = self.find_repository()
        layout = repos.get_layout()
        branches = []
        for project, bp, nick, has_props, revnum in layout.get_branches(repos,
                repos.get_latest_revnum()):
            branches.append(self.open_branch(branch_path=bp, repository=repos))
        return branches

