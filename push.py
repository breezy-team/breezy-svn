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
"""Pushing to Subversion repositories."""

from bzrlib import (
    debug,
    ui,
    urlutils,
    )
from bzrlib.errors import (
    BzrError,
    DivergedBranches,
    NoSuchRevision,
    )
from bzrlib.repository import (
    InterRepository,
    Repository,
    )
from bzrlib.revision import (
    NULL_REVISION,
    ensure_null,
    )
from bzrlib.trace import (
    mutter,
    )

from bzrlib.plugins.svn.commit import (
    SvnCommitBuilder,
    _check_dirs_exist,
    )
from bzrlib.plugins.svn.config import (
    BranchConfig,
    )
from bzrlib.plugins.svn.errors import (
    ChangesRootLHSHistory, 
    convert_svn_error,
    )
from bzrlib.plugins.svn.repository import (
    SvnRepositoryFormat, 
    SvnRepository,
    )
from bzrlib.plugins.svn.transport import (
    url_join_unescaped_path,
    )

from subvertpy import (
    SubversionException,
    properties,
    )


def push_new(graph, target_repository, target_branch_path, source, stop_revision,
             push_metadata=True, append_revisions_only=False, 
             override_svn_revprops=None):
    """Push a revision into Subversion, creating a new branch.

    This will do a new commit in the target branch.

    :param graph: Repository graph.
    :param target_repository: Repository to push to
    :param target_branch_path: Path to create new branch at
    :param source: Source repository
    """
    assert isinstance(source, Repository)
    start_revid = stop_revision
    for revid in source.iter_reverse_revision_history(stop_revision):
        if target_repository.has_revision(revid):
            break
        start_revid = revid
    rev = source.get_revision(start_revid)
    if rev.parent_ids == []:
        start_revid_parent = NULL_REVISION
    else:
        start_revid_parent = rev.parent_ids[0]
    # If this is just intended to create a new branch
    mapping = target_repository.get_mapping()
    if (start_revid != NULL_REVISION and 
        start_revid_parent != NULL_REVISION and 
        stop_revision == start_revid and mapping.supports_hidden):
        if (target_repository.has_revision(start_revid) or 
            start_revid == NULL_REVISION):
            revid = start_revid
        else:
            revid = start_revid_parent
        create_branch_with_hidden_commit(target_repository, target_branch_path,
                                         revid, set_metadata=True, 
                                         deletefirst=None)
    else:
        return push_revision_tree(graph, target_repository, target_branch_path, 
                              BranchConfig(urlutils.join(target_repository.base, target_branch_path),
                                  target_repository.uuid), 
                              source, start_revid_parent, start_revid, 
                              rev, push_metadata=push_metadata,
                              append_revisions_only=append_revisions_only,
                              override_svn_revprops=override_svn_revprops)


def dpush(target, source, stop_revision=None):
    """Push derivatives of the revisions missing from target from source into 
    target.

    :param target: Branch to push into
    :param source: Branch to retrieve revisions from
    :param stop_revision: If not None, stop at this revision.
    :return: Map of old revids to new revids.
    """
    source.lock_write()
    try:
        if stop_revision is None:
            stop_revision = ensure_null(source.last_revision())
        if target.last_revision() in (stop_revision, source.last_revision()):
            return { source.last_revision(): source.last_revision() }
        # Request graph from source repository, since it is most likely
        # faster than the target (Subversion) repository
        graph = source.repository.get_graph(target.repository)
        if not graph.is_ancestor(target.last_revision(), stop_revision):
            if graph.is_ancestor(stop_revision, target.last_revision()):
                return { source.last_revision(): source.last_revision() }
            raise DivergedBranches(source, target)
        todo = target.mainline_missing_revisions(source, stop_revision)
        if todo is None:
            # Not possible to add cleanly onto mainline, perhaps need a replace operation
            todo = self.otherline_missing_revisions(other, stop_revision)
        if todo is None:
            raise DivergedBranches(self, other)
        revid_map = {}
        target_branch_path = target.get_branch_path()
        target_config = target.get_config()
        pb = ui.ui_factory.nested_progress_bar()
        try:
            # FIXME: Call create_branch_with_hidden_commit if the revision is 
            # already present in the target repository ?
            for rev in source.repository.get_revisions(todo):
                pb.update("pushing revisions", todo.index(rev.revision_id), 
                          len(todo))
                if len(rev.parent_ids) == 0:
                    base_revid = NULL_REVISION
                elif rev.parent_ids[0] in revid_map:
                    base_revid = revid_map[rev.parent_ids[0]]
                else:
                    base_revid = rev.parent_ids[0]
                revid_map[rev.revision_id] = push(graph, target.repository,
                        target_branch_path, target_config, base_revid, 
                        source.repository, rev, push_metadata=False)
        finally:
            pb.finished()
        source.repository.fetch(target.repository, 
                                revision_id=revid_map[rev.revision_id])
        target._clear_cached_state()
        assert stop_revision in revid_map
        assert len(revid_map.keys()) > 0
        return revid_map
    finally:
        source.unlock()


def replay_delta(builder, old_trees, new_tree):
    """Replays a delta to a commit builder.

    :param builder: The commit builder.
    :param old_tree: Original tree on top of which the delta should be applied
    :param new_tree: New tree that should be committed
    """
    for path, ie in new_tree.inventory.iter_entries():
        builder.record_entry_contents(ie.copy(), 
            [old_tree.inventory for old_tree in old_trees], 
            path, new_tree, None)
    builder.finish_inventory()


def push_revision_tree(graph, target_repo, branch_path, config, source_repo, base_revid, 
                       revision_id, rev, push_metadata=True,
                       append_revisions_only=True,
                       override_svn_revprops=None):
    """Push a revision tree into a target repository.

    :param graph: Repository graph.
    :param target_repo: Target repository.
    :param branch_path: Branch path.
    :param config: Branch configuration.
    :param source_repo: Source repository.
    :param base_revid: Base revision id.
    :param revision_id: Revision id to push.
    :param rev: Revision object of revision to push.
    :param push_metadata: Whether to push metadata.
    :param append_revisions_only: Append revisions only.
    :return: Revision id of newly created revision.
    """
    assert rev.revision_id in (None, revision_id)
    old_tree = source_repo.revision_tree(revision_id)
    if rev.parent_ids:
        base_tree = source_repo.revision_tree(rev.parent_ids[0])
    else:
        base_tree = source_repo.revision_tree(NULL_REVISION)

    if push_metadata:
        base_revids = rev.parent_ids
    else:
        base_revids = [base_revid]

    try:
        opt_signature = source_repo.get_signature_text(rev.revision_id)
    except NoSuchRevision:
        opt_signature = None

    builder = SvnCommitBuilder(target_repo, branch_path, base_revids,
                               config, rev.timestamp,
                               rev.timezone, rev.committer, rev.properties, 
                               revision_id, 
                               base_tree.inventory,
                               push_metadata=push_metadata,
                               graph=graph, opt_signature=opt_signature,
                               texts=source_repo.texts,
                               append_revisions_only=append_revisions_only,
                               override_svn_revprops=override_svn_revprops)
    parent_trees = [base_tree]
    for p in rev.parent_ids[1:]:
        try:
            parent_trees.append(source_repo.revision_tree(p))
        except NoSuchRevision:
            pass # Ghost, ignore
    replay_delta(builder, parent_trees, old_tree)
    try:
        revid = builder.commit(rev.message)
    except SubversionException, (msg, num):
        if num == ERR_FS_TXN_OUT_OF_DATE:
            raise DivergedBranches(source_repo, target_repo)
        raise
    except ChangesRootLHSHistory:
        raise BzrError("Unable to push revision %r because it would change the ordering of existing revisions on the Subversion repository root. Use rebase and try again or push to a non-root path" % revision_id)

    return revid


def push(graph, target_repo, target_path, target_config, base_revid, source_repo, rev, push_metadata=True):
    """Push a revision into Subversion.

    This will do a new commit in the target branch.

    :param target: Branch to push to
    :param source_repo: Branch to pull the revision from
    :param rev: Revision id for the revision to push
    :return: revision id of revision that was pushed
    """
    assert isinstance(source_repo, Repository)
    mutter('pushing %r (%r)', rev.revision_id, rev.parent_ids)

    source_repo.lock_read()
    try:
        revid = push_revision_tree(graph, target_repo, target_path, target_config, 
                                   source_repo, base_revid, rev.revision_id, 
                                   rev, push_metadata=push_metadata,
                                   append_revisions_only=target_config.get_append_revisions_only(),
                                   override_svn_revprops=target_config.get_override_svn_revprops())
    finally:
        source_repo.unlock()
    assert revid == rev.revision_id or not push_metadata
    return revid


class InterToSvnRepository(InterRepository):
    """Any to Subversion repository actions."""

    _matching_repo_format = SvnRepositoryFormat()

    def __init__(self, source, target, graph=None):
        InterRepository.__init__(self, source, target)
        self._graph = graph

    @staticmethod
    def _get_repo_format_to_test():
        """See InterRepository._get_repo_format_to_test()."""
        return None

    def push_branch(self, todo, layout, project, target_branch, target_config,
        push_merged=False):
        """Push a series of revisions into a Subversion repository.

        """
        pb = ui.ui_factory.nested_progress_bar()
        try:
            for rev in self.source.get_revisions(todo):
                pb.update("pushing revisions", todo.index(rev.revision_id), 
                          len(todo))
                if push_merged:
                    self.push_ancestors(layout, project, 
                        rev.parent_ids, create_prefix=True)
                if rev.parent_ids:
                    base_revid = rev.parent_ids[0]
                else:
                    base_revid = NULL_REVISION
                push(self._graph, self.target, target_branch, 
                     target_config, base_revid, self.source, rev)
        finally:
            pb.finished()

    def push_ancestors(self, layout, project, parent_revids, 
                       create_prefix=False):
        """Push the ancestors of a revision.

        :param layout: Subversion layout
        :param project: Project name
        :param parent_revids: The revision ids of the basic ancestors to push
        :param create_prefix: Whether to optionally create the prefix of the branches.
        """
        for parent_revid in self.target.has_revisions(parent_revids[1:]):
            # Push merged revisions
            unique_ancestors = self._graph.find_unique_ancestors(parent_revid, [parent_revids[0]])
            for x in self._graph.iter_topo_order(unique_ancestors):
                if self.target.has_revision(x):
                    continue
                rev = self.source.get_revision(x)
                rhs_branch_path = determine_branch_path(rev, layout, project)
                try:
                    push_new(self._graph, self.target, rhs_branch_path, 
                             self.source, x, append_revisions_only=False)
                except MissingPrefix, e:
                    if not create_prefix:
                        raise
                    revprops = {properties.PROP_REVISION_LOG: "Add branches directory."}
                    if self.target.transport.has_capability("commit-revprops"):
                        revprops[mapping.SVN_REVPROP_BZR_SKIP] = ""
                    create_branch_prefix(self.target, revprops, e.path.split("/")[:-1], filter(lambda x: x != "", e.existing_path.split("/")))
                    push_new(self._graph, self.target, rhs_branch_path, self.source, x, append_revisions_only=False)

    def push_nonmainline_revision(self, rev, layout):
        mutter('pushing %r', rev.revision_id)
        if rev.parent_ids == []:
            parent_revid = NULL_REVISION
        else:
            parent_revid = rev.parent_ids[0]

        if parent_revid == NULL_REVISION:
            bp = determine_branch_path(rev, layout, None)
        else:
            (uuid, bp, _), _ = self.target.lookup_revision_id(parent_revid)
            (tp, target_project, _, ip) = layout.parse(bp)
            if tp != 'branch' or ip != "":
                bp = determine_branch_path(rev, layout, None)
        target_config = BranchConfig(urlutils.join(self.target.base, bp) , self.target.uuid)
        if (layout.push_merged_revisions(target_project) and 
            target_config.get_push_merged_revisions()):
            self.push_ancestors(layout, target_project, 
                rev.parent_ids, create_prefix=True)
        push_revision_tree(self._graph, self.target, 
            bp, target_config, 
            self.source, parent_revid, rev.revision_id, rev, 
            append_revisions_only=target_config.get_append_revisions_only())

    def copy_content(self, revision_id=None, pb=None):
        """See InterRepository.copy_content."""
        self.source.lock_read()
        try:
            if self._graph is None:
                self._graph = self.source.get_graph(self.target)
            assert revision_id is not None, "fetching all revisions not supported"
            # Go back over the LHS parent until we reach a revid we know
            todo = []
            for revision_id in self.source.iter_reverse_revision_history(revision_id):
                if self.target.has_revision(revision_id):
                    break
                todo.append(revision_id)
            if todo == []:
                # Nothing to do
                return
            todo.reverse()
            mutter("pushing %r into svn", todo)
            layout = self.target.get_layout()
            for rev in self.source.get_revisions(todo):
                if pb is not None:
                    pb.update("pushing revisions", todo.index(rev.revision_id), 
                        len(todo))
                self.push_nonmainline_revision(rev, layout)
        finally:
            self.source.unlock()

    def fetch(self, revision_id=None, pb=None, find_ghosts=False):
        """Fetch revisions. """
        self.copy_content(revision_id=revision_id, pb=pb)

    @staticmethod
    def is_compatible(source, target):
        """Be compatible with SvnRepository."""
        return isinstance(target, SvnRepository)


def determine_branch_path(rev, layout, project=None):
    """Create a sane branch path to use for a revision.
    
    :param rev: Revision object
    :param layout: Subversion layout
    :param project: Optional project name, as used by the layout
    :return: Branch path string
    """
    nick = (rev.properties.get('branch-nick') or "merged").encode("utf-8").replace("/","_")
    if project is None:
        return layout.get_branch_path(nick)
    else:
        return layout.get_branch_path(nick, project)


def create_branch_prefix(repository, revprops, bp_parts, existing_bp_parts):
    """Create a branch prefixes (e.g. "branches")

    :param repository: Subversion repository
    :param revprops: Revision properties to set
    :param bp_parts: Branch path elements that should be created (list of names, 
        ["branches", "foo"] for "branches/foo")
    :param existing_bp_parts: Branch path elements that already exist.
    """
    conn = repository.transport.get_connection()
    try:
        ci = convert_svn_error(conn.get_commit_editor)(revprops)
        try:
            root = ci.open_root()
            name = None
            batons = [root]
            for p in existing_bp_parts:
                if name is None:
                    name = p
                else:
                    name += "/" + p
                batons.append(batons[-1].open_directory(name))
            for p in bp_parts[len(existing_bp_parts):]:
                if name is None:
                    name = p
                else:
                    name += "/" + p
                batons.append(batons[-1].add_directory(name))
            for baton in reversed(batons):
                baton.close()
        except:
            ci.abort()
            raise
        ci.close()
    finally:
        repository.transport.add_connection(conn)


def create_branch_with_hidden_commit(repository, branch_path, revid,
                                     set_metadata=True,
                                     deletefirst=False):
    """Create a new branch using a simple "svn cp" operation.

    :param repository: Repository in which to create the branch.
    :param branch_path: Branch path
    :param revid: Revision id to keep as tip.
    :param deletefirst: Whether to delete an existing branch at this location first.
    """
    revprops = {properties.PROP_REVISION_LOG: "Create new branch."}
    revmeta, mapping = repository._get_revmeta(revid)
    fileprops = dict(revmeta.get_fileprops().iteritems())
    if set_metadata:
        assert mapping.supports_hidden
        (set_custom_revprops, set_custom_fileprops) = repository._properties_to_set(mapping)
        if set_custom_revprops:
            mapping.export_hidden_revprops(branch_path, revprops)
            if (not set_custom_fileprops and 
                not repository.transport.has_capability("log-revprops")):
                # Tell clients about first approximate use of revision 
                # properties
                mapping.export_revprop_redirect(
                    repository.get_latest_revnum()+1, fileprops)
        if set_custom_fileprops:
            mapping.export_hidden_fileprops(fileprops)
    parent = urlutils.dirname(branch_path)

    bp_parts = branch_path.split("/")
    existing_bp_parts =_check_dirs_exist(repository.transport, bp_parts, -1)
    if (len(bp_parts) not in (len(existing_bp_parts), len(existing_bp_parts)+1)):
        raise MissingPrefix("/".join(bp_parts), "/".join(existing_bp_parts))

    if deletefirst is None:
        deletefirst = (bp_parts == existing_bp_parts)

    conn = repository.transport.get_connection(parent)
    try:
        ci = convert_svn_error(conn.get_commit_editor)(revprops)
        try:
            root = ci.open_root()
            if deletefirst:
                root.delete_entry(urlutils.basename(branch_path))
            branch_dir = root.add_directory(urlutils.basename(branch_path), 
                    url_join_unescaped_path(repository.base, revmeta.branch_path), revmeta.revnum)
            for k, (ov, nv) in properties.diff(fileprops, revmeta.get_fileprops()).iteritems():
                branch_dir.change_prop(k, nv)
            branch_dir.close()
            root.close()
        except:
            ci.abort()
            raise
        ci.close()
    finally:
        repository.transport.add_connection(conn)
