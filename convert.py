# Copyright (C) 2005-2009 by Jelmer Vernooij
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA


"""Conversion of full repositories."""

from __future__ import absolute_import

import os
from subvertpy import (
    ERR_STREAM_MALFORMED_DATA,
    NODE_FILE,
    SubversionException,
    repos,
    )

from breezy import (
    controldir,
    osutils,
    ui,
    urlutils,
    )
from breezy.errors import (
    BzrError,
    FileExists,
    IncompatibleRepositories,
    NotBranchError,
    NoSuchFile,
    NoSuchRevision,
    NoRepositoryPresent,
    )
from breezy.transport import (
    get_transport,
    )

from breezy.plugins.svn import (
    changes,
    )
from breezy.plugins.svn.branch import (
    SvnBranch,
    )
from breezy.plugins.svn.fetch import (
    FetchRevisionFinder,
    InterFromSvnToInventoryRepository,
    )


LATEST_IMPORT_REVISION_FILENAME = "svn-import-revision"


def get_latest_svn_import_revision(repo, uuid):
    """Retrieve the latest revision checked by svn-import.

    :param repo: A repository object.
    :param uuid: Subversion repository UUID.
    """
    try:
        text = repo.controldir.transport.get_bytes(LATEST_IMPORT_REVISION_FILENAME)
    except NoSuchFile:
        return 0
    (text_uuid, revnum) = text.strip().split(" ")
    if text_uuid != uuid:
        return 0
    return int(revnum)


def put_latest_svn_import_revision(repo, uuid, revnum):
    """Store the latest revision checked by svn-import.

    :param repo: A repository object.
    :param uuid: Subversion repository UUID.
    :param revnum: A revision number.
    """
    repo.controldir.transport.put_bytes(LATEST_IMPORT_REVISION_FILENAME,
                             "%s %d\n" % (uuid, revnum))


class NotDumpFile(BzrError):
    """A file specified was not a dump file."""
    _fmt = """%(dumpfile)s is not a dump file."""

    def __init__(self, dumpfile):
        BzrError.__init__(self)
        self.dumpfile = dumpfile


def load_dumpfile(dumpfile, outputdir, feedback_stream=None):
    """Load a Subversion dump file.

    :param dumpfile: Path to dump file.
    :param outputdir: Directory in which Subversion repository should be
        created.
    """
    if feedback_stream is None:
        class NullStream(object):

            def read(self): return
            def write(self, data): return
            def close(self): return
        feedback_stream = NullStream()
    r = repos.create(outputdir)
    if dumpfile.endswith(".gz"):
        import gzip
        file = gzip.GzipFile(dumpfile)
    elif dumpfile.endswith(".bz2"):
        import bz2
        file = bz2.BZ2File(dumpfile)
    else:
        file = open(dumpfile)
    try:
        try:
            r.load_fs(file, feedback_stream, repos.LOAD_UUID_DEFAULT)
        except SubversionException, (_, num):
            if num == ERR_STREAM_MALFORMED_DATA:
                raise NotDumpFile(dumpfile)
            raise
    finally:
        file.close()
    return r


def contains_parent_path(s, p):
    """See if one of the parent paths of p exists in s.

    """
    while p != "":
        if p in s:
            return p
        (p, _) = urlutils.split(p)
    return False


class RepositoryConverter(object):

    def __init__(self, source_repos, output_url, layout=None,
                 create_shared_repo=True, working_trees=False, all=False,
                 format=None, filter_branch=None, keep=False,
                 incremental=False, to_revnum=None, prefix=None,
                 colocated=False, remember_parent=True):
        """Convert a Subversion repository and its' branches to a
        Bazaar repository.

        :param source_repos: Subversion repository
        :param output_url: URL to write Bazaar repository to.
        :param layout: Repository layout (object) to use
        :param create_shared_repo: Whether to create a shared Bazaar
            repository
        :param working_trees: Whether to create working trees
        :param all: Whether old revisions, even those not part of any
            existing branches, should be imported.
        :param format: Format to use
        :param remember_parent: Remember parent branch location
        """
        assert not all or create_shared_repo
        if format is None:
            self._format = controldir.format_registry.make_controldir('default')
        else:
            self._format = format

        self.dirs = {}
        self.to_transport = get_transport(output_url)
        try:
            self.to_transport.mkdir('.')
        except FileExists:
            pass
        if layout is not None:
            source_repos.set_layout(layout)
        else:
            layout = source_repos.get_layout()

        try:
            target_dir = controldir.ControlDir.open_from_transport(self.to_transport)
        except NotBranchError:
            target_dir = None
        else:
            format = target_dir._format

        if colocated is None:
            colocated = getattr(format, "colocated_branches", False)

        if target_dir is None and (colocated or create_shared_repo):
            target_dir = self.get_dir(prefix, prefix)

        if create_shared_repo:
            try:
                target_repos = target_dir.open_repository()
                target_repos_is_empty = False # FIXME: Call Repository.is_empty() ?
                if not layout.is_branch(u"") and not target_repos.is_shared() and not colocated:
                    raise BzrError("Repository %r is not shared." % target_repos)
            except NoRepositoryPresent:
                target_repos = target_dir.create_repository(shared=True)
                target_repos_is_empty = True
            target_repos.set_make_working_trees(working_trees)
        else:
            target_repos = None
            target_repos_is_empty = False

        source_repos.lock_read()
        try:
            if incremental and target_repos is not None:
                from_revnum = get_latest_svn_import_revision(target_repos,
                    source_repos.uuid)
            else:
                from_revnum = 0
            if to_revnum is None:
                to_revnum = source_repos.get_latest_revnum()
            if to_revnum < from_revnum:
                return
            mapping = source_repos.get_mapping()
            existing_branches = {}
            deleted = set()
            it = source_repos._revmeta_provider.iter_all_changes(layout,
                mapping.is_branch_or_tag, to_revnum, from_revnum,
                prefix=prefix)
            if create_shared_repo:
                revfinder = FetchRevisionFinder(source_repos, target_repos,
                                                target_repos_is_empty)
                revmetas = []
            else:
                revmetas = None
            if all:
                heads = None
            else:
                heads = set()
            with ui.ui_factory.nested_progress_bar() as pb:
                for kind, item in it:
                    if kind == "revision":
                        pb.update("finding branches", to_revnum-item.metarev.revnum,
                                  to_revnum-from_revnum)
                        if (not item.metarev.branch_path in existing_branches and
                            layout.is_branch(item.metarev.branch_path) and
                            not contains_parent_path(deleted, item.metarev.branch_path)):
                            existing_branches[item.metarev.branch_path] = SvnBranch(
                                source_repos, None, item.metarev.branch_path,
                                revnum=item.metarev.revnum, _skip_check=True,
                                mapping=mapping)
                            if heads is not None:
                                heads.add(item)
                        if revmetas is not None:
                            revmetas.append(item)
                    elif kind == "delete":
                        (path, revnum) = item
                        deleted.add(path)

            if create_shared_repo:
                if not InterFromSvnToInventoryRepository.is_compatible(source_repos, target_repos):
                    raise IncompatibleRepositories(source_repos, target_repos)
                inter = InterFromSvnToInventoryRepository.get(source_repos, target_repos)
                self._fetch_to_shared_repo(inter, prefix, from_revnum, revmetas,
                                           revfinder, mapping, heads)

            if not keep:
                self._remove_branches(deleted, existing_branches.keys())

            existing_branches = existing_branches.values()
            if filter_branch is not None:
                existing_branches = filter(filter_branch, existing_branches)
            self._create_branches(existing_branches, prefix,
                  create_shared_repo, working_trees, colocated,
                  remember_parent)
        finally:
            source_repos.unlock()

        if target_repos is not None:
            put_latest_svn_import_revision(target_repos, source_repos.uuid,
                                           to_revnum)

    def _fetch_to_shared_repo(self, inter, prefix, from_revnum, revmetas,
                              revfinder, mapping, heads):
        def needs_manual_check(revmeta):
            if (prefix is not None and
                not changes.path_is_child(prefix, revmeta.metarev.branch_path)):
                # Parent branch path is outside of prefix; we need to
                # check manually
                return True
            if revmeta.metarev.revnum < from_revnum:
                return True
            return False

        # TODO: Skip revisions in removed branches unless all=True
        with ui.ui_factory.nested_progress_bar() as pb:
            pb.update("checking revisions to fetch", 0,
                      len(revmetas))
            revfinder.find_iter_revisions(revmetas, mapping,
                needs_manual_check, pb=pb, heads=heads)
        missing = revfinder.get_missing()
        inter.fetch(needed=missing)

    def _get_nested_branch(self, source_branch, prefix, remember_parent):
        target_dir = self.get_dir(source_branch.get_branch_path(), prefix)
        try:
            target_dir.find_repository()
        except NoRepositoryPresent:
            target_dir.create_repository()
        try:
            return target_dir.open_branch()
        except NotBranchError:
            target_branch = target_dir.create_branch()
            if remember_parent:
                target_branch.set_parent(source_branch.base)
            return target_branch

    def _get_colocated_branch(self, source_branch, prefix, remember_parent):
        target_dir = self.get_dir(prefix, prefix)

        if source_branch.project in (None, ""):
            name = source_branch.name
        else:
            if source_branch.name is None:
                name = source_branch.project
            else:
                name = urlutils.join(
                    source_branch.project, source_branch.name)

        try:
            return target_dir.open_branch(name)
        except NotBranchError:
            target_branch = target_dir.create_branch(name)
            if remember_parent:
                target_branch.set_parent(source_branch.base)
            return target_branch

    def _create_branches(self, existing_branches, prefix, shared,
                         working_trees, colocated, remember_parent):
        with ui.ui_factory.nested_progress_bar() as pb:
            for i, source_branch in enumerate(existing_branches):
                try:
                    pb.update("%s:%d" % (source_branch.get_branch_path(),
                        source_branch.get_revnum()), i,
                        len(existing_branches))
                except SubversionException, (_, ERR_FS_NOT_DIRECTORY):
                    continue
                if colocated:
                    target_branch = self._get_colocated_branch(source_branch,
                            prefix, remember_parent)
                else:
                    target_branch = self._get_nested_branch(source_branch,
                            prefix, remember_parent)
                try:
                    target_branch.pull(source_branch, overwrite=True)
                except NoSuchRevision:
                    if source_branch.check_path() == NODE_FILE:
                        self.to_transport.delete_tree(
                            source_branch.get_branch_path())
                        continue
                    raise
                if working_trees and not target_branch.controldir.has_workingtree():
                    target_branch.controldir.create_workingtree()

    def _remove_branches(self, removed_branches, exceptions):
        """Recursively remove a set of branches.

        :param removed_branches: Branches to remove recursively
        :param exceptions: Branches to *not* remove
        """
        # Remove removed branches
        for bp in removed_branches:
            skip = False
            for e in exceptions:
                if bp.startswith(e+"/"):
                    skip = True
                    break
            if skip:
                continue
            fullpath = self.to_transport.local_abspath(bp)
            if not os.path.isdir(fullpath):
                continue
            osutils.rmtree(fullpath)

    def get_dir(self, path, prefix=None):
        """Open ControlDir for path, optionally creating it."""
        if prefix is not None:
            assert path.startswith(prefix)
            path = path[len(prefix):].strip("/")
        if path is None:
            path = ""
        if self.dirs.has_key(path):
            return self.dirs[path]
        nt = self.to_transport.clone(path)
        try:
            self.dirs[path] = controldir.ControlDir.open_from_transport(nt)
        except NotBranchError:
            nt.create_prefix()
            self.dirs[path] = self._format.initialize_on_transport(nt)
        return self.dirs[path]

def convert_repository(*args, **kwargs):
    RepositoryConverter(*args, **kwargs)
