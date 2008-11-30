# Copyright (C) 2005-2007 by Jelmer Vernooij
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
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

from itertools import tee
import os

from bzrlib import (
        bzrdir,
        osutils,
        ui,
        urlutils,
        )
from bzrlib.errors import (
        BzrError,
        NotBranchError,
        NoSuchFile,
        NoRepositoryPresent,
        ) 
from bzrlib.repository import InterRepository
from bzrlib.revision import ensure_null
from bzrlib.transport import get_transport

from subvertpy import SubversionException, repos, ERR_STREAM_MALFORMED_DATA

from bzrlib.plugins.svn.branch import SvnBranch
from bzrlib.plugins.svn.fetch import FetchRevisionFinder
from bzrlib.plugins.svn.format import get_rich_root_format
from bzrlib.plugins.svn.revmeta import filter_revisions

LATEST_IMPORT_REVISION_FILENAME = "svn-import-revision"

def get_latest_svn_import_revision(repo, uuid):
    """Retrieve the latest revision checked by svn-import.
    
    :param repo: A repository object.
    :param uuid: Subversion repository UUID.
    """
    try:
        text = repo.bzrdir.transport.get_bytes(LATEST_IMPORT_REVISION_FILENAME)
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
    repo.bzrdir.transport.put_bytes(LATEST_IMPORT_REVISION_FILENAME, 
                             "%s %d\n" % (uuid, revnum))


def transport_makedirs(transport, location_url):
    """Create missing directories.
    
    :param transport: Transport to use.
    :param location_url: URL for which parents should be created.
    """
    needed = [(transport, transport.relpath(location_url))]
    while needed:
        try:
            transport, relpath = needed[-1]
            transport.mkdir(relpath)
            needed.pop()
        except NoSuchFile:
            if relpath == "":
                raise
            needed.append((transport, urlutils.dirname(relpath)))


class NotDumpFile(BzrError):
    """A file specified was not a dump file."""
    _fmt = """%(dumpfile)s is not a dump file."""
    def __init__(self, dumpfile):
        BzrError.__init__(self)
        self.dumpfile = dumpfile


def load_dumpfile(dumpfile, outputdir):
    """Load a Subversion dump file.

    :param dumpfile: Path to dump file.
    :param outputdir: Directory in which Subversion repository should be 
        created.
    """
    from cStringIO import StringIO
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
            r.load_fs(file, StringIO(), repos.LOAD_UUID_DEFAULT)
        except SubversionException, (_, num):
            if num == ERR_STREAM_MALFORMED_DATA:
                raise NotDumpFile(dumpfile)
            raise
    finally:
        file.close()
    return r


def contains_parent_path(s, p):
    while p != "":
        if p in s:
            return p
        (p, _) = urlutils.split(p)
    return False



def convert_repository(source_repos, output_url, layout=None,
                       create_shared_repo=True, working_trees=False, all=False,
                       format=None, filter_branch=None, keep=False, 
                       incremental=False, to_revnum=None):
    """Convert a Subversion repository and its' branches to a 
    Bazaar repository.

    :param source_repos: Subversion repository
    :param output_url: URL to write Bazaar repository to.
    :param layout: Repository layout (object) to use
    :param create_shared_repo: Whether to create a shared Bazaar repository
    :param working_trees: Whether to create working trees
    :param all: Whether old revisions, even those not part of any existing 
        branches, should be imported
    :param format: Format to use
    """
    def remove_branches(to_transport, removed_branches, exceptions):
        # Remove removed branches
        for bp in removed_branches:
            skip = False
            for e in exceptions:
                if bp.startswith(e+"/"):
                    skip = True
                    break
            if skip:
                continue
            fullpath = to_transport.local_abspath(bp)
            if not os.path.isdir(fullpath):
                continue
            osutils.rmtree(fullpath)
    assert not all or create_shared_repo
    if format is None:
        format = get_rich_root_format()
    dirs = {}
    to_transport = get_transport(output_url)
    def get_dir(path):
        if dirs.has_key(path):
            return dirs[path]
        nt = to_transport.clone(path)
        try:
            dirs[path] = bzrdir.BzrDir.open_from_transport(nt)
        except NotBranchError:
            transport_makedirs(to_transport, urlutils.join(to_transport.base, path))
            dirs[path] = format.initialize_on_transport(nt)
        return dirs[path]

    if layout is not None:
        source_repos.set_layout(layout)
    else:
        layout = source_repos.get_layout()

    if create_shared_repo:
        try:
            target_repos = get_dir("").open_repository()
            target_repos_is_empty = False # FIXME: Call Repository.is_empty() ?
            assert (layout.is_branch("") or 
                    target_repos.is_shared())
        except NoRepositoryPresent:
            target_repos = get_dir("").create_repository(shared=True)
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
        project = None
        if to_revnum is None:
            to_revnum = source_repos.get_latest_revnum()
        mapping = source_repos.get_mapping()
        existing_branches = {}
        deleted = set()
        it = source_repos._revmeta_provider.iter_all_changes(layout, mapping.is_branch_or_tag, to_revnum, from_revnum, project=project)
        if create_shared_repo:
            revfinder = FetchRevisionFinder(source_repos, target_repos, target_repos_is_empty)
            (it, it_rev) = tee(it)
        pb = ui.ui_factory.nested_progress_bar()
        try:
            for kind, item in it:
                if kind == "revision":
                    if (not item.branch_path in existing_branches and 
                        layout.is_branch(item.branch_path, project=project) and 
                        not contains_parent_path(deleted, item.branch_path)):
                        existing_branches[item.branch_path] = SvnBranch(source_repos, item.branch_path, revnum=item.revnum, _skip_check=True)
                elif kind == "delete":
                    deleted.add(item)
        finally:
            pb.finished()

        if create_shared_repo:
            inter = InterRepository.get(source_repos, target_repos)

            if (target_repos.is_shared() and 
                  getattr(inter, '_supports_revmetas', None) and 
                  inter._supports_revmetas):
                # TODO: Skip revisions in removed branches unless all=True
                revmetas = revfinder.find_iter(filter_revisions(it_rev), 
                                               mapping)
                inter.fetch(needed=revmetas)
            elif all:
                inter.fetch()

        if not keep:
            remove_branches(to_transport, deleted, existing_branches.keys())

        existing_branches = existing_branches.values()
        if filter_branch is not None:
            existing_branches = filter(filter_branch, existing_branches)
        source_graph = source_repos.get_graph()
        pb = ui.ui_factory.nested_progress_bar()
        try:
            for i, source_branch in enumerate(existing_branches):
                pb.update("%s:%d" % (source_branch.get_branch_path(), 
                    source_branch.get_revnum()), i, len(existing_branches))
                target_dir = get_dir(source_branch.get_branch_path())
                if not create_shared_repo:
                    try:
                        target_dir.open_repository()
                    except NoRepositoryPresent:
                        target_dir.create_repository()
                try:
                    target_branch = target_dir.open_branch()
                except NotBranchError:
                    target_branch = target_dir.create_branch()
                    target_branch.set_parent(source_branch.base)
                if source_branch.last_revision() != target_branch.last_revision():
                    # Check if target_branch contains a subset of 
                    # source_branch. If that is not the case, 
                    # assume that source_branch has been replaced 
                    # and remove target_branch
                    if not source_graph.is_ancestor(
                            ensure_null(target_branch.last_revision()),
                            ensure_null(source_branch.last_revision())):
                        target_branch.set_revision_history([])
                    target_branch.pull(source_branch)
                if working_trees and not target_dir.has_workingtree():
                    target_dir.create_workingtree()
        finally:
            pb.finished()
    finally:
        source_repos.unlock()

    if target_repos is not None:
        put_latest_svn_import_revision(target_repos, source_repos.uuid, to_revnum)
        
