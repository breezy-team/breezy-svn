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

import os

from bzrlib import osutils, ui, urlutils
from bzrlib.bzrdir import BzrDir, Converter
from bzrlib.errors import (BzrError, NotBranchError, NoSuchFile, 
                           NoRepositoryPresent) 
from bzrlib.repository import InterRepository
from bzrlib.revision import ensure_null
from bzrlib.transport import get_transport

from bzrlib.plugins.svn.branch import SvnBranch
from subvertpy import SubversionException, repos, ERR_STREAM_MALFORMED_DATA
from bzrlib.plugins.svn.format import get_rich_root_format

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
            dirs[path] = BzrDir.open_from_transport(nt)
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
            assert (layout.is_branch("") or 
                    layout.is_tag("") or 
                    target_repos.is_shared())
        except NoRepositoryPresent:
            target_repos = get_dir("").create_repository(shared=True)
        target_repos.set_make_working_trees(working_trees)
    else:
        target_repos = None

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
        # Searching history for touched and removed branches is more expensive than 
        # just listing the branches in HEAD, so avoid it if possible.
        # If there's more than one subdirectory (we always have .bzr), we may 
        # have to remove existing branches.
        if (not keep and to_transport.has(".") and len(to_transport.list_dir(".")) > 1):
            removed_branches = source_repos.find_deleted_branches_between(layout=layout, 
                from_revnum=from_revnum, to_revnum=to_revnum, project=project)
        else:
            removed_branches = []
        mapping = source_repos.get_mapping()
        revmetas = []
        existing_branches = {}
        pb = ui.ui_factory.nested_progress_bar()
        # FIXME: Use filter_branch
        try:
            for revmeta in source_repos._revmeta_provider.iter_all_changes(layout, mapping,
                                                                   to_revnum, from_revnum,
                                                                   project=project):
                pb.update("determining revisions to fetch", to_revnum-revmeta.revnum, to_revnum)
                if revmeta.is_hidden(mapping):
                    continue
                if target_repos is not None and not target_repos.has_revision(revmeta.get_revision_id(mapping)):
                    revmetas.append(revmeta)
                if not revmeta.branch_path in existing_branches:
                    existing_branches[revmeta.branch_path] = SvnBranch(source_repos, revmeta.branch_path, revnum=revmeta.revnum, _skip_check=True)
        finally:
            pb.finished()
        existing_branches = existing_branches.values()

        if create_shared_repo:
            inter = InterRepository.get(source_repos, target_repos)

            if all:
                inter.fetch()
            elif (target_repos.is_shared() and 
                  getattr(inter, '_supports_branches', None) and 
                  inter._supports_branches):
                revmetas.reverse()
                inter.fetch(revmetas=revmetas, mapping=source_repos.get_mapping())

        if not keep:
            # Remove removed branches
            for bp in removed_branches:
                # TODO: Perhaps check if path is a valid branch with the right last
                # revid?
                fullpath = to_transport.local_abspath(bp)
                if not os.path.isdir(fullpath):
                    continue
                osutils.rmtree(fullpath)

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
        

class SvnConverter(Converter):
    """Converts from a Subversion directory to a bzr dir."""
    def __init__(self, target_format):
        """Create a SvnConverter.
        :param target_format: The format the resulting repository should be.
        """
        super(SvnConverter, self).__init__()
        self.target_format = target_format

    def convert(self, to_convert, pb):
        """See Converter.convert()."""
        convert_repository(to_convert.open_repository(), to_convert.base, 
                           format=self.target_format, all=True, pb=pb)
