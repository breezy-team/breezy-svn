# Copyright (C) 2005-2009 Jelmer Vernooij <jelmer@samba.org>

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


"""Handles branch-specific operations."""


import os
from subvertpy import (
    ERR_FS_ALREADY_EXISTS,
    ERR_FS_NO_SUCH_REVISION,
    NODE_DIR,
    SubversionException,
    wc,
    )

from bzrlib import (
    osutils,
    tag,
    trace,
    ui,
    urlutils,
    )
from bzrlib.branch import (
    Branch,
    BranchFormat,
    BranchCheckResult,
    BranchPushResult,
    GenericInterBranch,
    InterBranch,
    PullResult,
    )
from bzrlib.bzrdir import (
    BzrDir,
    format_registry,
    )
from bzrlib.errors import (
    AlreadyBranchError,
    DivergedBranches,
    IncompatibleFormat,
    LocalRequiresBoundBranch,
    NoSuchRevision,
    NotBranchError,
    UnstackableBranchFormat,
    )
from bzrlib.foreign import (
    ForeignBranch,
    )
from bzrlib.revision import (
    NULL_REVISION,
    is_null,
    ensure_null,
    )
from bzrlib.workingtree import (
    WorkingTree,
    )

from bzrlib.plugins.svn import (
    util,
    )
from bzrlib.plugins.svn.fetch import (
    FetchRevisionFinder,
    InterFromSvnRepository,
    )
from bzrlib.plugins.svn.push import (
    InterToSvnRepository,
    SubversionBranchDiverged,
    create_branch_with_hidden_commit,
    )
from bzrlib.plugins.svn.config import (
    BranchConfig,
    )
from bzrlib.plugins.svn.errors import (
    NotSvnBranchPath,
    PushToEmptyBranch,
    )
from bzrlib.plugins.svn.repository import (
    SvnRepository,
    )
from bzrlib.plugins.svn.tags import (
    SubversionTags,
    )
from bzrlib.plugins.svn.transport import (
    bzr_to_svn_url,
    )

class SubversionBranchCheckResult(BranchCheckResult):
    """Result of checking a Subversion branch."""


class SubversionSourcePullResult(PullResult):
    """Subversion source pull result.

    The main difference with a standard Bazaar PullResult is that it also
    reports the Subversion revision number.
    """

    def report(self, to_file):
        if not trace.is_quiet():
            if self.old_revid in (self.new_revid, NULL_REVISION):
                to_file.write('No revisions to pull.\n')
            else:
                if self.new_revmeta is None:
                    self.new_revmeta, _ = self.source_branch.repository._get_revmeta(self.new_revid)
                to_file.write('Now on revision %d (svn revno: %d).\n' %
                        (self.new_revno, self.new_revmeta.revnum))
        self._show_tag_conficts(to_file)


class SubversionWriteLock(object):
    """A (dummy) write lock on a Subversion object."""

    __slots__ = ('unlock')

    def __init__(self, unlock):
        self.unlock = unlock

    def __repr__(self):
        return "<%s>" % self.__class__.__name__


class SubversionReadLock(object):
    """A (dummy) read lock on a Subversion object."""

    __slots__ = ('unlock')

    def __init__(self, unlock):
        self.unlock = unlock

    def __repr__(self):
        return "<%s>" % self.__class__.__name__


class SubversionTargetBranchPushResult(BranchPushResult):
    """Subversion target branch push result."""

    def _lookup_revno(self, revid):
        assert isinstance(revid, str), "was %r" % revid
        # Try in source branch first, it'll be faster
        try:
            return self.source_branch.revision_id_to_revno(revid)
        except NoSuchRevision:
            return self.target_branch.revision_id_to_revno(revid)

    @property
    def old_revno(self):
        try:
            return self._lookup_revno(self.old_revid)
        except NoSuchRevision:
            # Last resort
            return len(
                self.target_branch.repository.iter_reverse_revision_history(
                    self.old_revid))

    @property
    def new_revno(self):
        return self._lookup_revno(self.new_revid)


class SubversionTargetPullResult(PullResult):
    """Subversion target branch pull result."""

    def _lookup_revno(self, revid):
        # Try in source branch first, it'll be faster
        try:
            return self.source_branch.revision_id_to_revno(revid)
        except NoSuchRevision:
            return self.target_branch.revision_id_to_revno(revid)

    @property
    def old_revno(self):
        try:
            return self._lookup_revno(self.old_revid)
        except NoSuchRevision:
            # Last resort
            return len(
                list(self.target_branch.repository.iter_reverse_revision_history(
                    self.old_revid)))

    @property
    def new_revno(self):
        return self._lookup_revno(self.new_revid)


class SvnBranch(ForeignBranch):
    """Maps to a Branch in a Subversion repository """

    def __init__(self, repository, branch_path, revnum=None, _skip_check=False,
                 mapping=None):
        """Instantiate a new SvnBranch.

        :param repos: SvnRepository this branch is part of.
        :param branch_path: Relative path inside the repository this
            branch is located at.
        :param revnum: Subversion revision number of the branch to
            look at; none for latest.
        :param _skip_check: If True, don't check if the branch actually exists.
        """
        self.repository = repository
        self.bzrdir = repository.bzrdir
        self._format = SvnBranchFormat()
        self.layout = self.repository.get_layout()
        assert isinstance(self.repository, SvnRepository)
        super(SvnBranch, self).__init__(mapping or self.repository.get_mapping())
        self._lock_mode = None
        self._lock_count = 0
        self._branch_path = branch_path.strip("/")
        self.base = urlutils.join(self.repository.base,
                        self._branch_path).rstrip("/")
        self._revmeta_cache = None
        self._revnum = revnum
        assert isinstance(self._branch_path, str)
        if not _skip_check:
            try:
                if self.check_path() != NODE_DIR:
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

    def _push_should_merge_tags(self):
        return self.supports_tags()

    def check_path(self):
        return self.repository.transport.check_path(self._branch_path, self._revnum or self.repository.get_latest_revnum())

    def supports_tags(self):
        """See Branch.supports_tags()."""
        return (self._format.supports_tags() and
                self.mapping.supports_tags() and
                self.layout.supports_tags())

    def set_branch_path(self, branch_path):
        """Change the branch path for this branch.

        :param branch_path: New branch path.
        """
        self._branch_path = branch_path.strip("/")

    def get_branch_path(self, revnum=None):
        """Find the branch path of this branch in the specified revnum.

        :param revnum: Revnum to look for.
        """
        if revnum is None:
            return self._branch_path

        assert revnum >= 0

        last_revmeta, _ = self.last_revmeta()
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
        return self.last_revmeta()[0].revnum

    def get_child_submit_format(self):
        """Return the preferred format of submissions to this branch."""
        ret = self.get_config().get_user_option("child_submit_format")
        if ret is not None:
            return ret
        return "svn"

    def last_revmeta(self):
        """Return the revmeta element for the last revision in this branch.
        """
        for revmeta, mapping in self._revision_meta_history():
            return revmeta, mapping
        return None, None

    def check(self):
        """See Branch.Check.

        Doesn't do anything for Subversion repositories at the moment (yet).
        """
        # TODO: Check svn file properties?
        return SubversionBranchCheckResult(self)

    def _create_heavyweight_checkout(self, to_location, revision_id=None,
                                     hardlink=False):
        """Create a new heavyweight checkout of this branch.

        :param to_location: URL of location to create the new checkout in.
        :param revision_id: Revision that should be the tip of the checkout.
        :param hardlink: Whether to hardlink
        :return: WorkingTree object of checkout.
        """
        checkout_branch = BzrDir.create_branch_convenience(
            to_location, force_new_tree=False,
            format=format_registry.make_bzrdir('default'))
        checkout = checkout_branch.bzrdir
        checkout_branch.bind(self)
        # pull up to the specified revision_id to set the initial
        # branch tip correctly, and seed it with history.
        checkout_branch.pull(self, stop_revision=revision_id)
        return checkout.create_workingtree(revision_id, hardlink=hardlink)

    def lookup_bzr_revision_id(self, revid):
        """Look up the matching Subversion revision number on the mainline of
        the branch.

        :param revid: Revision id to look up.
        :return: Revision number on the branch.
        :raises NoSuchRevision: If the revision id was not found.
        """
        (uuid, bp, revnum), mapping = self.repository.lookup_bzr_revision_id(revid,
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
            revnum = self.lookup_bzr_revision_id(revision_id)
        else:
            revnum = self.get_revnum()

        svn_url = bzr_to_svn_url(self.base)
        os.mkdir(to_location.encode(osutils._fs_enc))
        wc.ensure_adm(to_location.encode("utf-8"), self.repository.uuid, svn_url,
                      bzr_to_svn_url(self.repository.base), revnum)
        adm = wc.WorkingCopy(None, to_location.encode("utf-8"), write_lock=True)
        try:
            conn = self.repository.transport.connections.get(svn_url)
            try:
                update_wc(adm, to_location.encode("utf-8"), conn, revnum)
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
        revmeta_history = self._revision_meta_history()
        for revmeta, mapping in revmeta_history:
            if revmeta.revnum == revnum:
                return revmeta.get_revision_id(mapping)
            if revmeta.revnum < revnum:
                break
        raise NoSuchRevision(self, revnum)

    def get_config(self):
        return BranchConfig(self.base, self.repository.uuid)

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
        if rev_history == []:
            self._set_last_revision(NULL_REVISION)
        else:
            self._set_last_revision(rev_history[-1])

    def set_last_revision_info(self, revno, revid):
        """See Branch.set_last_revision_info()."""
        # FIXME: 

    def _set_last_revision(self, revid):
        try:
            rev = self.repository.get_revision(revid)
        except NoSuchRevision:
            raise NotImplementedError("set_last_revision_info can't add ghosts")
        if rev.parent_ids:
            base_revid = rev.parent_ids[0]
        else:
            base_revid = NULL_REVISION
        interrepo = InterToSvnRepository(self.repository, self.repository)
        base_foreign_info = interrepo._get_foreign_revision_info(base_revid, self.get_branch_path())
        interrepo.push_single_revision(self.get_branch_path(), self.get_config(), rev,
            push_metadata=True, root_action=("replace", self.get_revnum()),
            base_foreign_info=base_foreign_info)
        self._clear_cached_state()

    def last_revision_info(self):
        """See Branch.last_revision_info()."""
        last_revid = self.last_revision()
        return self.revision_id_to_revno(last_revid), last_revid

    def revision_id_to_revno(self, revision_id):
        """Given a revision id, return its revno"""
        if is_null(revision_id):
            return 0
        revmeta_history = self._revision_meta_history()
        # FIXME: Maybe we can parse revision_id as a bzr-svn roundtripped
        # revision?
        for revmeta, mapping in revmeta_history:
            if revmeta.get_revision_id(mapping) == revision_id:
                return revmeta.get_revno(mapping)
        raise NoSuchRevision(self, revision_id)

    def get_root_id(self):
        tree = self.basis_tree()
        return tree.get_root_id()

    def set_push_location(self, location):
        """See Branch.set_push_location()."""
        trace.mutter("setting push location for %s to %s", self.base, location)

    def get_push_location(self):
        """See Branch.get_push_location()."""
        # get_push_location not supported on Subversion
        return None

    def _iter_revision_meta_ancestry(self, pb=None):
        return self.repository._iter_reverse_revmeta_mapping_ancestry(
            self.get_branch_path(),
            self._revnum or self.repository.get_latest_revnum(), self.mapping,
            lhs_history=self._revision_meta_history(), pb=pb)

    def _revision_meta_history(self):
        if self._revmeta_cache is None:
            self._revmeta_cache = util.lazy_readonly_list(
                self.repository._iter_reverse_revmeta_mapping_history(
                    self.get_branch_path(),
                    self._revnum or self.repository.get_latest_revnum(),
                    to_revnum=0, mapping=self.mapping))
        return self._revmeta_cache

    def get_rev_id(self, revno, history=None):
        """Find the revision id of the specified revno."""
        if revno == 0:
            return NULL_REVISION
        last_revno = self.revno()
        if revno <= 0 or revno > last_revno:
            raise NoSuchRevision(self, revno)
        count = last_revno - revno
        for (revmeta, mapping) in self._revision_meta_history():
            if revmeta.is_hidden(mapping):
                continue
            if count == 0:
                assert revmeta.get_revno(mapping) == revno, "Expected %d, was (%r,%r) %d" % (revno, revmeta, mapping, revmeta.get_revno(mapping))
                return revmeta.get_revision_id(mapping)
            count -= 1
        raise AssertionError

    def _gen_revision_history(self):
        """Generate the revision history from last revision."""
        history = [revmeta.get_revision_id(mapping) for revmeta, mapping in self._revision_meta_history()]
        history.reverse()
        return history

    def last_revision(self):
        """See Branch.last_revision()."""
        # Shortcut for finding the tip. This avoids expensive generation time
        # on large branches.
        last_revmeta, mapping = self.last_revmeta()
        return last_revmeta.get_revision_id(mapping)

    def get_push_merged_revisions(self):
        return (self.layout.push_merged_revisions(self.project) and
                self.get_config().get_push_merged_revisions())

    def import_last_revision_info(self, source_repo, revno, revid):
        interrepo = InterToSvnRepository(source_repo, self.repository)
        last_revmeta, mapping = self.last_revmeta()
        interrepo.push_todo(last_revmeta.get_revision_id(mapping),
            last_revmeta.get_foreign_revid(), mapping, revid, self.layout,
            self.project, self.get_branch_path(), self.get_config(),
            push_merged=self.get_push_merged_revisions(),
            overwrite=False, push_metadata=True)

    def import_last_revision_info_and_tags(self, source, revno, revid):
        self.import_last_revision_info(source.repository, revno, revid)
        self.tags.merge_to(source.tags, overwrite=False)

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
            revision_id = self.last_revision()
        destination.generate_revision_history(revision_id)

    def is_locked(self):
        return self._lock_count != 0

    def break_lock(self):
        pass

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
        return SubversionWriteLock(self.unlock)

    def lock_read(self):
        """See Branch.lock_read()."""
        if self._lock_mode:
            assert self._lock_mode in ('r', 'w')
            self._lock_count += 1
        else:
            self._lock_mode = 'r'
            self._lock_count = 1
        self.repository.lock_read()
        return SubversionReadLock(self.unlock)

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

    def get_stacked_on_url(self):
        raise UnstackableBranchFormat(self._format, self.base)

    def __str__(self):
        return '%s(%r)' % (self.__class__.__name__, self.base)

    __repr__ = __str__

    def _basic_push(self, target, overwrite=False, stop_revision=None):
        return InterBranch.get(self, target)._basic_push(
            overwrite, stop_revision)


class SvnBranchFormat(BranchFormat):
    """Branch format for Subversion Branches."""

    def network_name(self):
        return "subversion"

    def __get_matchingbzrdir(self):
        """See BranchFormat.__get_matchingbzrdir()."""
        from bzrlib.plugins.svn.remote import SvnRemoteFormat
        return SvnRemoteFormat()

    _matchingbzrdir = property(__get_matchingbzrdir)

    def get_format_description(self):
        """See BranchFormat.get_format_description."""
        return 'Subversion Smart Server'

    def get_foreign_tests_branch_factory(self):
        from bzrlib.plugins.svn.tests.test_branch import ForeignTestsBranchFactory
        return ForeignTestsBranchFactory()

    def initialize(self, to_bzrdir):
        """See BranchFormat.initialize()."""
        from bzrlib.plugins.svn.remote import SvnRemoteAccess
        if not isinstance(to_bzrdir, SvnRemoteAccess):
            raise IncompatibleFormat(self, to_bzrdir._format)
        repository = to_bzrdir.find_repository()
        try:
            create_branch_with_hidden_commit(repository,
                to_bzrdir.branch_path, NULL_REVISION, set_metadata=True,
                deletefirst=False)
        except SubversionException, (_, num):
            if num == ERR_FS_ALREADY_EXISTS:
                raise AlreadyBranchError(to_bzrdir.user_url)
            raise
        return to_bzrdir.open_branch()

    def supports_tags(self):
        return True

    def make_tags(self, branch):
        if branch.supports_tags():
            return SubversionTags(branch)
        else:
            return tag.DisabledTags(branch)


class InterSvnOtherBranch(GenericInterBranch):
    """InterBranch implementation that is optimized for copying from
    Subversion.

    The two main differences with the generic implementation are:
     * No revision numbers are calculated for the Subversion branch
       (since this requires browsing the entire history)
     * Only recent tags are fetched, since that saves a lot of
       history browsing operations
    """

    @staticmethod
    def _get_branch_formats_to_test():
        from bzrlib.branch import format_registry
        return [(SvnBranchFormat(), format_registry.get_default())]

    def update_revisions(self, stop_revision=None, overwrite=False,
                         graph=None):
        self._update_revisions(stop_revision, overwrite, graph)

    def _update_revisions(self, stop_revision=None, overwrite=False,
            graph=None, limit=None):
        """Like InterBranch.update_revisions, but with a few additional
            parameters.

        :param limit: Optional maximum number of revisions to fetch
        :return: Last revision
        """
        self.source.lock_read()
        try:
            other_last_revision = self.source.last_revision()
            if stop_revision is None:
                stop_revision = other_last_revision
                if is_null(stop_revision):
                    # if there are no commits, we're done.
                    return self.target.last_revision_info()

            # what's the current last revision, before we fetch [and
            # change it possibly]
            last_rev = self.target.last_revision()
            # we fetch here so that we don't process data twice in the
            # common case of having something to pull, and so that the
            # check for already merged can operate on the just fetched
            # graph, which will be cached in memory.
            revisionfinder = FetchRevisionFinder(self.source.repository,
                self.target.repository)
            pb = ui.ui_factory.nested_progress_bar()
            try:
                revisionfinder.find_until(
                    self.source.last_revmeta()[0].get_foreign_revid(),
                    self.source.mapping,
                    find_ghosts=False, pb=pb)
            finally:
                pb.finished()
            interrepo = InterFromSvnRepository(self.source.repository,
                self.target.repository)
            missing = revisionfinder.get_missing(limit=limit)
            if limit is not None and len(missing) > 0:
                stop_revision = missing[-1][0].get_revision_id(missing[-1][1])
            interrepo.fetch(needed=missing,
                project=self.source.project, mapping=self.source.mapping)
            # Check to see if one is an ancestor of the other
            if not overwrite:
                if graph is None:
                    graph = self.target.repository.get_graph()
                self.target.lock_read()
                try:
                    if self.target._check_if_descendant_or_diverged(
                            stop_revision, last_rev, graph, self.source):
                        # stop_revision is a descendant of last_rev, but we
                        # aren't overwriting, so we're done.
                        return self.target.last_revision_info()
                finally:
                    self.target.unlock()
            self.target.generate_revision_history(stop_revision)
            return self.target.last_revision_info()
        finally:
            self.source.unlock()

    def _basic_push(self, overwrite=False, stop_revision=None):
        result = BranchPushResult()
        result.source_branch = self.source
        result.target_branch = self.target
        graph = self.target.repository.get_graph(self.source.repository)
        result.old_revno, result.old_revid = self.target.last_revision_info()
        self.update_revisions(stop_revision, overwrite=overwrite, graph=graph)
        # FIXME: Tags
        result.new_revno, result.new_revid = self.target.last_revision_info()
        return result

    def pull(self, overwrite=False, stop_revision=None,
             _hook_master=None, run_hooks=True, possible_transports=None,
             _override_hook_target=None, local=False, limit=None):
        """See InterBranch.pull()."""
        if local:
            raise LocalRequiresBoundBranch()
        result = SubversionSourcePullResult()
        if _override_hook_target is None:
            result.target_branch = self
        else:
            result.target_branch = _override_hook_target
        result.source_branch = self.source
        result.master_branch = None
        result.target_branch = self.target
        self.source.lock_read()
        try:
            (result.old_revno, result.old_revid) = \
                self.target.last_revision_info()
            if result.old_revid == NULL_REVISION:
                result.old_revmeta = None
                tags_since_revnum = None
            else:
                try:
                    result.old_revmeta, _ = \
                        self.source.repository._get_revmeta(result.old_revid)
                    tags_since_revnum = result.old_revmeta.revnum
                except NoSuchRevision:
                    result.old_revmeta = None
                    tags_since_revnum = None
            if stop_revision == NULL_REVISION:
                result.new_revid = NULL_REVISION
                result.new_revmeta = None
            elif stop_revision is not None:
                result.new_revmeta, _ = \
                    self.source.repository._get_revmeta(stop_revision)
            else:
                result.new_revmeta = None
            (result.new_revno, result.new_revid) = self._update_revisions(
                stop_revision, overwrite, limit=limit)
            if self.source.supports_tags():
                result.tag_conflicts = self.source.tags.merge_to(
                    self.target.tags, overwrite,
                    _from_revnum=tags_since_revnum,
                    _to_revnum=self.source.repository.get_latest_revnum())
            if _hook_master:
                result.master_branch = _hook_master
                result.local_branch = result.target_branch
            else:
                result.master_branch = result.target_branch
                result.local_branch = None
            if run_hooks:
                for hook in Branch.hooks['post_pull']:
                    hook(result)
        finally:
            self.source.unlock()
        return result

    @classmethod
    def is_compatible(self, source, target):
        return (isinstance(source, SvnBranch) and
                not isinstance(target, SvnBranch))

InterBranch.register_optimiser(InterSvnOtherBranch)


class InterOtherSvnBranch(InterBranch):
    """InterBranch implementation that is optimized for copying to
    Subversion.

    """

    @staticmethod
    def _get_branch_formats_to_test():
        from bzrlib.branch import format_registry
        return [(format_registry.get_default(), SvnBranchFormat())]

    def update_revisions(self, stop_revision=None, overwrite=False, graph=None):
        """See Branch.update_revisions()."""
        self._update_revisions(stop_revision=stop_revision,
            overwrite=overwrite, graph=graph)

    def _target_is_empty(self, graph, revid):
        parent_revids = tuple(graph.get_parent_map([revid])[revid])
        if parent_revids != (NULL_REVISION,):
            return False
        tree_contents = self.target.repository.transport.get_dir(
            self.target.get_branch_path(), self.target.get_revnum())[0]
        return tree_contents == {}

    def copy_content_into(self, revision_id=None):
        if revision_id is None:
            revision_id = self.source.last_revision()
        self._push(revision_id, overwrite=True, push_metadata=True)

    def _push(self, stop_revision, overwrite, push_metadata):
        old_last_revid = self.target.last_revision()
        if old_last_revid == stop_revision:
            return { old_last_revid: (old_last_revid, None) }
        push_merged = self.target.get_push_merged_revisions()
        interrepo = InterToSvnRepository(
            self.source.repository, self.target.repository)
        try:
            return interrepo.push_branch(self.target.get_branch_path(),
                    self.target.get_config(), self.target.last_revmeta(),
                    stop_revision=stop_revision, overwrite=overwrite,
                    push_metadata=push_metadata, push_merged=push_merged,
                    layout=self.target.layout, project=self.target.project)
        except SubversionBranchDiverged, e:
            if self._target_is_empty(interrepo.get_graph(), e.target_revid):
                raise PushToEmptyBranch(self.target, self.source)
            raise DivergedBranches(self.target, self.source)

    def _update_revisions(self, stop_revision=None, overwrite=False,
            graph=None, override_svn_revprops=None):
        old_last_revid = self.target.last_revision()
        if stop_revision is None:
            stop_revision = ensure_null(self.source.last_revision())
        self._push(stop_revision, overwrite=overwrite, push_metadata=True)
        self.target._clear_cached_state()
        assert isinstance(old_last_revid, str)
        return (old_last_revid, self.target.last_revision())

    def _update_revisions_lossy(self, stop_revision=None, overwrite=False):
        """Push derivatives of the revisions missing from target from source
        into target.

        :param target: Branch to push into
        :param source: Branch to retrieve revisions from
        :param stop_revision: If not None, stop at this revision.
        :return: Map of old revids to new revids.
        """
        self.source.lock_write()
        try:
            if stop_revision is None:
                stop_revision = ensure_null(self.source.last_revision())
            revid_map = self._push(stop_revision, overwrite=overwrite,
                push_metadata=False)
            interrepo = InterFromSvnRepository(self.target.repository,
                                               self.source.repository)
            interrepo.fetch(revision_id=revid_map[stop_revision][0],
                mapping=self.target.mapping, project=self.target.project)
            self.target._clear_cached_state()
            assert stop_revision in revid_map
            assert len(revid_map.keys()) > 0
            return dict([(k, v[0]) for (k, v) in revid_map.iteritems()])
        finally:
            self.source.unlock()

    def lossy_push(self, stop_revision=None):
        """See InterBranch.lossy_push()."""
        result = SubversionTargetBranchPushResult()
        result.target_branch = self.target
        result.master_branch = None
        result.source_branch = self.source
        self.source.lock_write()
        try:
            result.old_revid = self.target.last_revision()
            result.revidmap = self._update_revisions_lossy(stop_revision)
            result.new_revid = self.target.last_revision()
            # FIXME: Tags ?
            return result
        finally:
            self.source.unlock()

    def update_tags(self, overwrite=False):
        return self.source.tags.merge_to(self.target.tags, overwrite)

    def push(self, overwrite=False, stop_revision=None,
            _override_svn_revprops=None,
            _override_hook_source_branch=None):
        """See InterBranch.push()."""
        result = SubversionTargetBranchPushResult()
        result.target_branch = self.target
        result.master_branch = None
        result.source_branch = self.source
        self.source.lock_read()
        try:
            (result.old_revid, result.new_revid) = \
                self._update_revisions(stop_revision, overwrite,
                    override_svn_revprops=_override_svn_revprops)
            result.tag_conflicts = self.update_tags(overwrite)
            return result
        finally:
            self.source.unlock()

    def pull(self, overwrite=False, stop_revision=None,
             _hook_master=None, run_hooks=True, possible_transports=None,
             _override_svn_revprops=None, local=False,
             limit=None):
        """See InterBranch.pull()."""
        if local:
            raise LocalRequiresBoundBranch()
        result = SubversionTargetPullResult()
        result.source_branch = self.source
        result.master_branch = None
        result.target_branch = self.target
        self.source.lock_read()
        try:
            (result.old_revid, result.new_revid) = \
                self._update_revisions(stop_revision, overwrite,
                    override_svn_revprops=_override_svn_revprops)
            result.tag_conflicts = self.update_tags(overwrite)
            return result
        finally:
            self.source.unlock()

    @classmethod
    def is_compatible(self, source, target):
        return isinstance(target, SvnBranch)


InterBranch.register_optimiser(InterOtherSvnBranch)
