# Copyright (C) 2005-2007 Jelmer Vernooij <jelmer@samba.org>

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
"""Handles branch-specific operations."""

from bzrlib import (
        tag,
        ui,
        urlutils,
        )
from bzrlib.branch import (
        BranchFormat,
        BranchCheckResult,
        PullResult,
        )
from bzrlib.bzrdir import (
        BzrDir,
        )
from bzrlib.decorators import (
        needs_write_lock,
        )
from bzrlib.errors import (
        DivergedBranches,
        NoSuchRevision,
        NotBranchError,
        UnstackableBranchFormat,
        )
from bzrlib.revision import (
        is_null,
        ensure_null,
        )
from bzrlib.workingtree import (
        WorkingTree,
        )

from subvertpy import (
        wc,
        SubversionException,
        NODE_DIR,
        ERR_FS_NO_SUCH_REVISION,
        )

from bzrlib.plugins.svn.commit import push, push_ancestors, create_branch_with_hidden_commit
from bzrlib.plugins.svn.config import BranchConfig
from bzrlib.plugins.svn.errors import NotSvnBranchPath
from bzrlib.plugins.svn.foreign import ForeignBranch, FakeControlFiles
from bzrlib.plugins.svn.format import get_rich_root_format
from bzrlib.plugins.svn.repository import SvnRepository
from bzrlib.plugins.svn.tags import SubversionTags
from bzrlib.plugins.svn.transport import bzr_to_svn_url

import os

class SvnBranch(ForeignBranch):
    """Maps to a Branch in a Subversion repository """
    def __init__(self, repository, branch_path, revnum=None, _skip_check=False):
        """Instantiate a new SvnBranch.

        :param repos: SvnRepository this branch is part of.
        :param branch_path: Relative path inside the repository this
            branch is located at.
        :param revnum: Subversion revision number of the branch to 
            look at; none for latest.
        """
        self.repository = repository
        self._format = SvnBranchFormat()
        assert isinstance(self.repository, SvnRepository)
        super(SvnBranch, self).__init__(self.repository.get_mapping())
        self.control_files = FakeControlFiles()
        self._lock_mode = None
        self._lock_count = 0
        self.layout = self.repository.get_layout()
        self._branch_path = branch_path.strip("/")
        self.base = urlutils.join(self.repository.base, 
                        self._branch_path).rstrip("/")
        self._revmeta_cache = None
        self._revnum = revnum
        assert isinstance(self._branch_path, str)
        if not _skip_check:
            try:
                revnum = self._revnum or self.repository.get_latest_revnum()
                if self.repository.transport.check_path(self._branch_path, 
                    revnum) != NODE_DIR:
                    raise NotBranchError(self.base)
            except SubversionException, (_, num):
                if num == ERR_FS_NO_SUCH_REVISION:
                    raise NotBranchError(self.base)
                raise
        try:
            (type, self.project, _, ip) = self.layout.parse(branch_path)
        except NotSvnBranchPath:
            raise NotBranchError(branch_path)
        if type not in ('branch', 'tag') or ip != '':
            raise NotBranchError(branch_path)

    def _make_tags(self):
        if self.supports_tags():
            return SubversionTags(self)
        else:
            return tag.DisabledTags(self)

    def set_branch_path(self, branch_path):
        """Change the branch path for this branch.

        :param branch_path: New branch path.
        """
        self._branch_path = branch_path.strip("/")

    def _get_append_revisions_only(self):
        value = self.get_config().get_user_option('append_revisions_only')
        return value == 'True'

    def unprefix(self, relpath):
        """Remove the branch path from a relpath.

        :param relpath: path from the repository root.
        """
        assert relpath.startswith(self.get_branch_path()), \
                "expected %s prefix, got %s" % (self.get_branch_path(), relpath)
        return relpath[len(self.get_branch_path()):].strip("/")

    def get_branch_path(self, revnum=None):
        """Find the branch path of this branch in the specified revnum.

        :param revnum: Revnum to look for.
        """
        if revnum is None:
            return self._branch_path

        last_revmeta = self.last_revmeta()
        if revnum == last_revmeta.revnum:
            return last_revmeta.branch_path

        # Use revnum - this branch may have been moved in the past 
        return self.repository.transport.get_locations(
                    last_revmeta.branch_path, last_revmeta.revnum,
                    [revnum])[revnum].strip("/")

    def get_revnum(self):
        """Obtain the Subversion revision number this branch was 
        last changed in.

        :return: Revision number
        """
        return self.last_revmeta().revnum

    def last_revmeta(self):
        """Return the revmeta element for the last revision in this branch.
        """
        for revmeta in self._revision_meta_history():
            if not revmeta.is_hidden(self.mapping):
                return revmeta
        return None

    def check(self):
        """See Branch.Check.

        Doesn't do anything for Subversion repositories at the moment (yet).
        """
        return BranchCheckResult(self)

    def _create_heavyweight_checkout(self, to_location, revision_id=None, 
                                     hardlink=False):
        """Create a new heavyweight checkout of this branch.

        :param to_location: URL of location to create the new checkout in.
        :param revision_id: Revision that should be the tip of the checkout.
        :param hardlink: Whether to hardlink
        :return: WorkingTree object of checkout.
        """
        checkout_branch = BzrDir.create_branch_convenience(
            to_location, force_new_tree=False, format=get_rich_root_format())
        checkout = checkout_branch.bzrdir
        checkout_branch.bind(self)
        # pull up to the specified revision_id to set the initial 
        # branch tip correctly, and seed it with history.
        checkout_branch.pull(self, stop_revision=revision_id)
        return checkout.create_workingtree(revision_id, hardlink=hardlink)

    def lookup_revision_id(self, revid):
        """Look up the matching Subversion revision number on the mainline of 
        the branch.

        :param revid: Revision id to look up.
        :return: Revision number on the branch. 
        :raises NoSuchRevision: If the revision id was not found.
        """
        (uuid, bp, revnum), mapping = self.repository.lookup_revision_id(revid, 
            ancestry=(self.get_branch_path(), self.get_revnum()), 
            project=self.project)
        assert bp.strip("/") == self.get_branch_path(revnum).strip("/"), \
                "Got %r, expected %r" % (bp, self.get_branch_path(revnum))
        return revnum

    def _create_lightweight_checkout(self, to_location, revision_id=None):
        """Create a new lightweight checkout of this branch.

        :param to_location: URL of location to create the checkout in.
        :param revision_id: Tip of the checkout.
        :return: WorkingTree object of the checkout.
        """
        from bzrlib.plugins.svn.workingtree import update_wc
        if revision_id is not None:
            revnum = self.lookup_revision_id(revision_id)
        else:
            revnum = self.get_revnum()

        svn_url = bzr_to_svn_url(self.base)
        os.mkdir(to_location)
        wc.ensure_adm(to_location, self.repository.uuid, svn_url,
                      bzr_to_svn_url(self.repository.base), revnum)
        adm = wc.WorkingCopy(None, to_location, write_lock=True)
        try:
            conn = self.repository.transport.connections.get(svn_url)
            try:
                update_wc(adm, to_location, conn, revnum)
            finally:
                if not conn.busy:
                    self.repository.transport.add_connection(conn)
        finally:
            adm.close()
        wt = WorkingTree.open(to_location)
        return wt

    def create_checkout(self, to_location, revision_id=None, lightweight=False,
                        accelerator_tree=None, hardlink=False):
        """See Branch.create_checkout()."""
        if lightweight:
            return self._create_lightweight_checkout(to_location, revision_id)
        else:
            return self._create_heavyweight_checkout(to_location, revision_id, 
                                                     hardlink=hardlink)

    def generate_revision_id(self, revnum):
        """Generate a new revision id for a revision on this branch."""
        assert isinstance(revnum, int)
        try:
            return self.repository.generate_revision_id(
                revnum, self.get_branch_path(revnum), self.mapping)
        except SubversionException, (_, num):
            if num == ERR_FS_NO_SUCH_REVISION:
                raise NoSuchRevision(self, revnum)
            raise

    def get_config(self):
        return BranchConfig(self)
       
    def _get_nick(self, local=False, possible_master_transports=None):
        """Find the nick name for this branch.

        :return: Branch nick
        """
        bp = self._branch_path.strip("/")
        if self._branch_path == "":
            return self.base.split("/")[-1]
        return bp

    nick = property(_get_nick)

    def set_revision_history(self, rev_history):
        """See Branch.set_revision_history()."""
        if (rev_history == [] or 
            not self.repository.has_revision(rev_history[-1], project=self.project)):
            raise NotImplementedError("set_revision_history can't add ghosts")
        push(self.repository.get_graph(), 
             self, self.repository, rev_history[-1])
        self._clear_cached_state()

    def set_last_revision_info(self, revno, revid):
        """See Branch.set_last_revision_info()."""

    def mainline_missing_revisions(self, other, stop_revision):
        """Find the revisions missing on the mainline.
        
        :param other: Other branch to retrieve revisions from.
        :param stop_revision: Revision to stop fetching at.
        """
        missing = []
        lastrevid = self.last_revision()
        for revid in other.repository.iter_reverse_revision_history(stop_revision):
            if lastrevid == revid:
                missing.reverse()
                return missing
            missing.append(revid)
        return None

    def otherline_missing_revisions(self, other, stop_revision, overwrite=False):
        """Find the revisions missing on the mainline.
        
        :param other: Other branch to retrieve revisions from.
        :param stop_revision: Revision to stop fetching at.
        :param overwrite: Whether or not the existing data should be overwritten
        """
        missing = []
        for revid in other.repository.iter_reverse_revision_history(stop_revision):
            if self.repository.has_revision(revid, project=self.project):
                missing.reverse()
                return missing
            missing.append(revid)
        if not overwrite:
            return None
        else:
            return missing
 
    def last_revision_info(self):
        """See Branch.last_revision_info()."""
        last_revid = self.last_revision()
        return self.revision_id_to_revno(last_revid), last_revid

    def revision_id_to_revno(self, revision_id):
        """Given a revision id, return its revno"""
        if is_null(revision_id):
            return 0
        revmeta_history = self._revision_meta_history()
        # FIXME: Maybe we can parse revision_id as a bzr-svn roundtripped revision?
        for revmeta in revmeta_history:
            if revmeta.get_revision_id(self.mapping) == revision_id:
                return len(revmeta_history) - revmeta_history.index(revmeta) - revmeta.get_hidden_lhs_ancestors_count(self.mapping)
        raise NoSuchRevision(self, revision_id)

    def get_root_id(self, revnum=None):
        if revnum is None:
            tree = self.basis_tree()
        else:
            tree = self.repository.revision_tree(self.get_rev_id(revnum))
        return tree.get_root_id()

    def set_push_location(self, location):
        """See Branch.set_push_location()."""
        raise NotImplementedError(self.set_push_location)

    def get_push_location(self):
        """See Branch.get_push_location()."""
        # get_push_location not supported on Subversion
        return None

    def _revision_meta_history(self):
        if self._revmeta_cache is None:
            pb = ui.ui_factory.nested_progress_bar()
            try:
                self._revmeta_cache = self.repository._revmeta_provider.get_mainline(self.get_branch_path(), self._revnum or self.repository.get_latest_revnum(), self.mapping, pb=pb)
            finally:
                pb.finished()
        return self._revmeta_cache

    def _gen_revision_history(self):
        """Generate the revision history from last revision
        """
        history = [revmeta.get_revision_id(self.mapping) for revmeta in self._revision_meta_history() if not revmeta.is_hidden(self.mapping)]
        history.reverse()
        return history

    def last_revision(self):
        """See Branch.last_revision()."""
        # Shortcut for finding the tip. This avoids expensive generation time
        # on large branches.
        return self.last_revmeta().get_revision_id(self.mapping)

    @needs_write_lock
    def dpull(self, source, stop_revision=None):
        from bzrlib.plugins.svn.commit import dpush
        return dpush(self, source, stop_revision)

    def pull(self, source, overwrite=False, stop_revision=None, 
             _hook_master=None, run_hooks=True, _push_merged=None,
             _override_svn_revprops=None):
        """See Branch.pull()."""
        result = PullResult()
        result.source_branch = source
        result.master_branch = None
        result.target_branch = self
        source.lock_read()
        try:
            (result.old_revno, result.old_revid) = self.last_revision_info()
            self.update_revisions(source, stop_revision, overwrite, 
                                  _push_merged=_push_merged,
                                  _override_svn_revprops=_override_svn_revprops)
            result.tag_conflicts = source.tags.merge_to(self.tags, overwrite)
            (result.new_revno, result.new_revid) = self.last_revision_info()
            return result
        finally:
            source.unlock()

    def generate_revision_history(self, revision_id, last_rev=None, 
        other_branch=None):
        """Create a new revision history that will finish with revision_id.
        
        :param revision_id: the new tip to use.
        :param last_rev: The previous last_revision. If not None, then this
            must be a ancestory of revision_id, or DivergedBranches is raised.
        :param other_branch: The other branch that DivergedBranches should
            raise with respect to.
        """
        # stop_revision must be a descendant of last_revision
        # make a new revision history from the graph

    def _synchronize_history(self, destination, revision_id):
        """Synchronize last revision and revision history between branches.

        This version is most efficient when the destination is also a
        BzrBranch6, but works for BzrBranch5, as long as the destination's
        repository contains all the lefthand ancestors of the intended
        last_revision.  If not, set_last_revision_info will fail.

        :param destination: The branch to copy the history into
        :param revision_id: The revision-id to truncate history at.  May
          be None to copy complete history.
        """
        if revision_id is None:
            revno, revision_id = self.last_revision_info()
        else:
            revno = self.revision_id_to_revno(revision_id)
        destination.set_last_revision_info(revno, revision_id)

    def update_revisions(self, other, stop_revision=None, overwrite=False, 
                         graph=None, _push_merged=False, 
                         _override_svn_revprops=None):
        """See Branch.update_revisions()."""
        if stop_revision is None:
            stop_revision = ensure_null(other.last_revision())
        if (self.last_revision() == stop_revision or
            self.last_revision() == other.last_revision()):
            return
        if graph is None:
            graph = self.repository.get_graph()
        other_graph = other.repository.get_graph()
        if not other_graph.is_ancestor(self.last_revision(), 
                                                        stop_revision):
            if graph.is_ancestor(stop_revision, self.last_revision()):
                return
            if not overwrite:
                raise DivergedBranches(self, other)
        todo = self.mainline_missing_revisions(other, stop_revision)
        if todo is None:
            # Not possible to add cleanly onto mainline, perhaps need a replace operation
            todo = self.otherline_missing_revisions(other, stop_revision, overwrite)
        if todo is None:
            raise DivergedBranches(self, other)
        if _push_merged is None:
            _push_merged = (self.layout.push_merged_revisions(self.project) and 
                            self.get_config().get_push_merged_revisions())
        self._push_missing_revisions(graph, other, other_graph, todo, 
                                     _push_merged, _override_svn_revprops)

    def _push_missing_revisions(self, my_graph, other, other_graph, todo, 
                                push_merged=False, _override_svn_revprops=None):
        if self.repository.has_revision(todo[-1]) and self.mapping.supports_hidden:
            create_branch_with_hidden_commit(self.repository, 
                self.get_branch_path(), todo[-1], set_metadata=True,
                deletefirst=True)
            self._clear_cached_state()
            return
        pb = ui.ui_factory.nested_progress_bar()
        try:
            for revid in todo:
                pb.update("pushing revisions", todo.index(revid), 
                          len(todo))
                if push_merged:
                    parent_revids = other_graph.get_parent_map([revid])[revid]
                    push_ancestors(self.repository, other.repository, self.layout, 
                                   self.project, parent_revids, other_graph,
                                   create_prefix=True)
                push(my_graph, self, other.repository, revid, 
                     override_svn_revprops=_override_svn_revprops)
                self._clear_cached_state()
        finally:
            pb.finished()

    def lock_write(self):
        """See Branch.lock_write()."""
        # TODO: Obtain lock on the remote server?
        if self._lock_mode:
            assert self._lock_mode == 'w'
            self._lock_count += 1
        else:
            self._lock_mode = 'w'
            self._lock_count = 1
        self.repository.lock_write()
        
    def lock_read(self):
        """See Branch.lock_read()."""
        if self._lock_mode:
            assert self._lock_mode in ('r', 'w')
            self._lock_count += 1
        else:
            self._lock_mode = 'r'
            self._lock_count = 1
        self.repository.lock_read()

    def unlock(self):
        """See Branch.unlock()."""
        self._lock_count -= 1
        if self._lock_count == 0:
            self._lock_mode = None
            self._clear_cached_state()
        self.repository.unlock()

    def _clear_cached_state(self):
        super(SvnBranch, self)._clear_cached_state()
        self._cached_revnum = None
        self._revmeta_cache = None

    def get_parent(self):
        """See Branch.get_parent()."""
        return None

    def set_parent(self, url):
        """See Branch.set_parent()."""

    def get_physical_lock_status(self):
        """See Branch.get_physical_lock_status()."""
        return False

    def sprout(self, to_bzrdir, revision_id=None):
        """See Branch.sprout()."""
        result = to_bzrdir.create_branch()
        self.copy_content_into(result, revision_id=revision_id)
        result.set_parent(self.bzrdir.root_transport.base)
        return result

    def get_stacked_on_url(self):
        raise UnstackableBranchFormat(self._format, self.base)

    def __str__(self):
        return '%s(%r)' % (self.__class__.__name__, self.base)

    def supports_tags(self):
        return self._format.supports_tags() and self.mapping.supports_tags()

    __repr__ = __str__


class SvnBranchFormat(BranchFormat):
    """Branch format for Subversion Branches."""
    def __init__(self):
        BranchFormat.__init__(self)

    def __get_matchingbzrdir(self):
        """See BranchFormat.__get_matchingbzrdir()."""
        from bzrlib.plugins.svn.remote import SvnRemoteFormat
        return SvnRemoteFormat()

    _matchingbzrdir = property(__get_matchingbzrdir)

    def get_format_description(self):
        """See BranchFormat.get_format_description."""
        return 'Subversion Smart Server'

    def get_format_string(self):
        """See BranchFormat.get_format_string()."""
        return 'Subversion Smart Server'

    def initialize(self, to_bzrdir):
        """See BranchFormat.initialize()."""
        raise NotImplementedError(self.initialize)

    def supports_tags(self):
        return True
