# Copyright (C) 2006-2011 Jelmer Vernooij <jelmer@samba.org>

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

from __future__ import absolute_import

from collections import defaultdict

import subvertpy
from subvertpy import (
    ERR_FS_ROOT_DIR ,
    ERR_FS_TXN_OUT_OF_DATE,
    SubversionException,
    properties,
    )

from breezy import (
    ui,
    urlutils,
    )
from breezy.errors import (
    AlreadyBranchError,
    AppendRevisionsOnlyViolation,
    BzrError,
    DivergedBranches,
    NoSuchRevision,
    NotWriteLocked,
    RevisionNotPresent,
    )
from breezy.repository import (
    InterRepository,
    )
from breezy.revision import (
    NULL_REVISION,
    )
from six import (
    text_type,
    )
from breezy.bzr.testament import (
    StrictTestament,
    )
from breezy.trace import (
    mutter,
    )

from .commit import (
    SvnCommitBuilder,
    )
from .config import (
    SvnBranchStack,
    )
from .errors import (
    ChangesRootLHSHistory,
    MissingPrefix,
    SubversionBranchDiverged,
    convert_svn_error,
    )
from .mapping import (
    SVN_REVPROP_BZR_SKIP,
    )
from .repository import (
    SvnRepositoryFormat,
    SvnRepository,
    )
from .transport import (
    check_dirs_exist,
    create_branch_prefix,
    url_join_unescaped_path,
    )


def create_branch_container(transport, prefix, already_present):
    """Create a branch prefix.

    :param transport: Repository root transport
    :param prefix: Prefix to create
    :param already_present: Path that already exists
    """
    revprops = {properties.PROP_REVISION_LOG: "Add branches directory."}
    if transport.has_capability("commit-revprops"):
        revprops[SVN_REVPROP_BZR_SKIP] = ""
    create_branch_prefix(transport, revprops, prefix.split("/")[:-1], filter(lambda x: x != "", already_present.split("/")))


def _filter_iter_changes(iter_changes):
    """Process iter_changes.

    Converts 'missing' entries in the iter_changes iterator to 'deleted'
    entries.

    :param iter_changes: An iter_changes to process.
    :return: A generator of changes.
    """
    for change in iter_changes:
        kind = change.kind[1]
        versioned = change.versioned[1]
        if kind is None and versioned:
            # 'missing' path
            # Reset the new path (None) and new versioned flag (False)
            change = TreeChange(
                change.file_id, (change.path[0], None),
                change.changed_content,
                (change.versioned[0], False), *change[4:])
        if any(change.versioned):
            yield change


def push_revision_tree(graph, target_repo, branch_path, config_stack,
                       source_repo, base_revid, revision_id, rev,
                       base_foreign_revid, base_mapping, push_metadata,
                       root_action):
    """Push a revision tree into a target repository.

    :param graph: Repository graph.
    :param target_repo: Target repository.
    :param branch_path: Branch path.
    :param config_stack: Branch configuration.
    :param source_repo: Source repository.
    :param base_revid: Base revision id.
    :param revision_id: Revision id to push.
    :param rev: Revision object of revision to push.
    :param push_metadata: Whether to push metadata.
    :param root_action: Action to take on the tree root
    :return: Revision id of newly created revision.
    """
    if not isinstance(branch_path, text_type):
        raise TypeError(branch_path)
    if target_repo._lock_mode != 'w':
        raise NotWriteLocked(target_repo)
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

    if push_metadata:
        testament = StrictTestament.from_revision_tree(old_tree)
    else:
        testament = None

    builder = SvnCommitBuilder(target_repo, branch_path, base_revids,
                               config_stack, rev.timestamp,
                               rev.timezone, rev.committer, rev.properties,
                               revision_id, base_foreign_revid, base_mapping,
                               root_action, base_tree,
                               push_metadata=push_metadata,
                               graph=graph, opt_signature=opt_signature,
                               testament=testament,
                               _rich_root_bump=not source_repo.supports_rich_root())
    target_repo.start_write_group()
    try:
        builder.will_record_deletes()
        iter_changes = old_tree.iter_changes(base_tree)
        iter_changes = _filter_iter_changes(iter_changes)
        for path, fs_hash in builder.record_iter_changes(
                old_tree, base_tree.get_revision_id(), iter_changes):
            pass
        builder.finish_inventory()
    except BaseException:
        builder.abort()
        raise
    try:
        revid = builder.commit(rev.message)
    except SubversionException as e:
        builder.abort()
        if e.args[1] == ERR_FS_TXN_OUT_OF_DATE:
            raise DivergedBranches(source_repo, target_repo)
        raise
    except ChangesRootLHSHistory:
        raise BzrError("Unable to push revision %r because it would change "
            "the ordering of existing revisions on the Subversion repository "
            "root. Use rebase and try again or push to a non-root path" %
            revision_id)

    return revid, (builder.result_foreign_revid, builder.mapping)


class InterToSvnRepository(InterRepository):
    """Any to Subversion repository actions."""

    _matching_repo_format = SvnRepositoryFormat()

    def __init__(self, source, target, graph=None):
        InterRepository.__init__(self, source, target)
        self._graph = graph
        # Dictionary: revid -> branch_path -> (foreign_revid, mapping)
        self._foreign_info = defaultdict(dict)

    def _target_has_revision(self, revid, project=None):
        """Slightly optimized version of self.target.has_revision()."""
        if revid in self._foreign_info:
            return True
        return self.target.has_revision(revid, project=project)

    def _get_foreign_revision_info(self, revid, path=None):
        """Find the revision info for a revision id.

        :param revid: Revision id to foreign foreign revision info for
        :param path: Preferred path
        :return: Foreign revision id and mapping
        """
        if revid == NULL_REVISION:
            return None, None
        if not revid in self._foreign_info:
            # FIXME: Prefer revisions in path
            return self.target.lookup_bzr_revision_id(revid)
        if path is not None and path in self._foreign_info[revid]:
            return self._foreign_info[revid][path]
        else:
            return self._foreign_info[revid].values()[0]

    def _add_path_info(self, path, revid, foreign_info):
        self._foreign_info[revid][path] = foreign_info

    @staticmethod
    def _get_repo_format_to_test():
        """See InterRepository._get_repo_format_to_test()."""
        return None

    def _get_root_action(self, path, parent_ids, overwrite,
                         append_revisions_only, create_prefix=False):
        """Determine the action to take on the tree root.

        :param path: Branch path
        :param parent_ids: Parent ids
        :param overwrite: Whether to overwrite any existing history
        :param create_prefix: Whether to create the prefix for path
        :return: root_action tuple for use with SvnCommitBuilder
        """
        assert not append_revisions_only or not overwrite
        bp_parts = path.split("/")
        existing_bp_parts = check_dirs_exist(self.target.svn_transport, bp_parts,
            -1)
        if (len(existing_bp_parts) != len(bp_parts) and
            len(existing_bp_parts)+1 != len(bp_parts)):
            existing_path = "/".join(existing_bp_parts)
            if create_prefix:
                create_branch_container(self.target.svn_transport, path,
                    existing_path)
                return ("create", )
            raise MissingPrefix(path, existing_path)
        if len(existing_bp_parts) < len(bp_parts):
            # Branch doesn't exist yet
            return ("create", )
        (revmeta, hidden, mapping) = next(self.target._revmeta_provider._iter_reverse_revmeta_mapping_history(
            path, self.target.get_latest_revnum(), to_revnum=0,
            mapping=self.target.get_mapping()))
        assert not hidden
        if tuple(parent_ids) == () or tuple(parent_ids) == (NULL_REVISION,):
            return ("replace", revmeta.metarev.revnum)
        else:
            if revmeta.get_revision_id(mapping) != parent_ids[0]:
                if append_revisions_only:
                    raise AppendRevisionsOnlyViolation(
                        urlutils.join(self.target.base, path))
                return ("replace", revmeta.metarev.revnum)
            else:
                return ("open", revmeta.metarev.revnum)

    def _otherline_missing_revisions(self, graph, stop_revision, project, overwrite):
        """Find the revisions missing on the mainline.

        :param stop_revision: Revision to stop fetching at.
        :param overwrite: Whether or not the existing data should be
            overwritten
        """
        missing = []
        for revid in graph.iter_lefthand_ancestry(stop_revision,
                (NULL_REVISION, None)):
            if self._target_has_revision(revid, project=project):
                missing.reverse()
                return missing
            missing.append(revid)
        if not overwrite:
            return None
        else:
            missing.reverse()
            return missing

    def _todo(self, target_branch, last_revid, last_foreign_revid, stop_revision, project,
            overwrite, append_revisions_only):
        graph = self.get_graph()
        todo = []
        if append_revisions_only:
            for revid in graph.iter_lefthand_ancestry(stop_revision,
                    (NULL_REVISION, None)):
                if revid == last_revid:
                    break
                todo.append(revid)
            else:
                if last_revid != NULL_REVISION:
                    url = urlutils.join(self.target.base, target_branch)
                    raise AppendRevisionsOnlyViolation(url)
            todo.reverse()
            return todo, ("open", last_foreign_revid[2])
        else:
            for revid in graph.iter_lefthand_ancestry(stop_revision,
                    (NULL_REVISION, None)):
                if self._target_has_revision(revid, project=project):
                    break
                todo.append(revid)
            todo.reverse()
            return todo, ("replace", last_foreign_revid[2])

    def push_branch(self, target_branch_path, target_config, old_last_revid,
            old_last_foreign_revid, mapping, stop_revision, layout, project,
            overwrite, push_metadata, push_merged):
        graph = self.get_graph()
        if not overwrite:
            heads = graph.heads([old_last_revid, stop_revision])
            if heads == set([old_last_revid]):
                # stop-revision is ancestor of current tip
                return { stop_revision: (stop_revision, None),
                         old_last_revid: (old_last_revid, None)}
            if heads == set([old_last_revid, stop_revision]):
                raise SubversionBranchDiverged(self.source, stop_revision,
                        self.target, target_branch_path, old_last_revid)
        append_revisions_only = target_config.get('append_revisions_only')
        if append_revisions_only is None:
            append_revisions_only = (not overwrite)
        return self.push_todo(old_last_revid, old_last_foreign_revid,
            mapping, stop_revision, layout, project,
            target_branch_path, target_config, push_merged=push_merged,
            overwrite=overwrite, push_metadata=push_metadata,
            append_revisions_only=append_revisions_only)

    def push_todo(self, last_revid, last_foreign_revid, mapping,
            stop_revision, layout, project, target_branch,
            target_config, push_merged, overwrite, push_metadata, append_revisions_only):
        todo, root_action = self._todo(target_branch, last_revid,
                last_foreign_revid, stop_revision, project, overwrite,
                append_revisions_only=append_revisions_only)
        if (mapping.supports_hidden and
            self.target.has_revision(stop_revision)):
            # FIXME: Only do this if there is no existing branch, or if
            # append_revisions_only=False
            # Revision is already present in the repository, so just
            # copy from there.
            (revid, foreign_revinfo) = self.copy_revision(target_branch,
                stop_revision, set_metadata=push_metadata, deletefirst=True)
            return { stop_revision: (revid, foreign_revinfo) }
        else:
            return self.push_revision_series(
                todo, layout, project, target_branch, target_config,
                push_merged, root_action=root_action,
                push_metadata=push_metadata)

    def copy_revision(self, target_branch, stop_revision, set_metadata, deletefirst):
        (revid, foreign_revinfo) = create_branch_with_hidden_commit(
            self.target, target_branch, stop_revision,
            set_metadata=set_metadata, deletefirst=deletefirst)
        self._add_path_info(target_branch, revid, foreign_revinfo)
        return (revid, foreign_revinfo)

    def push_revision_series(self, todo, layout, project, target_branch,
            target_config, push_merged, root_action, push_metadata):
        """Push a series of revisions into a Subversion repository.

        :param todo: New revisions to push
        :return: Dictionary mapping revision ids to tuples
            with new revision id and foreign info.
        """
        assert todo != []
        revid_map = {}
        with ui.ui_factory.nested_progress_bar() as pb:
            for rev in self.source.get_revisions(todo):
                pb.update("pushing revisions", todo.index(rev.revision_id),
                          len(todo))
                if len(rev.parent_ids) == 0:
                    base_revid = NULL_REVISION
                else:
                    base_revid = rev.parent_ids[0]
                try:
                    base_foreign_info = revid_map[base_revid][1]
                except KeyError:
                    base_foreign_info = self._get_foreign_revision_info(
                        base_revid, target_branch)
                last_revid, last_foreign_info = self.push_revision_inclusive(
                    target_branch, target_config, rev, root_action=root_action,
                    base_foreign_info=base_foreign_info,
                    push_metadata=push_metadata, push_merged=push_merged,
                    project=project, layout=layout)
                revid_map[rev.revision_id] = (last_revid, last_foreign_info)
                root_action = ("open", last_foreign_info[0][2])
            return revid_map

    def push_revision_inclusive(self, target_path, target_config, rev,
            push_merged, layout, project, root_action, push_metadata,
            base_foreign_info, exclude=None):
        """Push a revision including ancestors.

        :return: Tuple with pushed revision id and foreign revision id
        """
        if push_merged and len(rev.parent_ids) > 1:
            self.push_ancestors(layout, project, rev.parent_ids,
                    exclude=[target_path])
        return self.push_single_revision(target_path, target_config, rev,
            push_metadata=push_metadata, root_action=root_action,
            base_foreign_info=base_foreign_info)

    def push_single_revision(self, target_path, target_config, rev,
            push_metadata, root_action, base_foreign_info):
        """Push a single revision.

        :param target_path: Target branch path in the svn repository
        :param target_config: Config object for the target branch
        :param rev: Revision object of revision that needs to be pushed
        :param append_revisions_only: Whether to append revisions only
        :param push_metadata: Whether to push svn-specific metadata
        :param base_revid: Base revision (used when pushing a custom base),
            e.g. during dpush.
        :param delete_root_revnum: If not None, maximum revision of the root
            to delete
        :return: Tuple with pushed revision id and foreign revision id
        """
        (base_foreign_revid, base_mapping) = base_foreign_info
        if rev.parent_ids:
            base_revid = rev.parent_ids[0]
        else:
            base_revid = NULL_REVISION

        mutter('pushing %r (%r)', rev.revision_id, rev.parent_ids)
        with self.source.lock_read():
            revid, foreign_info = push_revision_tree(self.get_graph(),
                self.target, target_path, target_config, self.source,
                base_revid, rev.revision_id, rev, base_foreign_revid,
                base_mapping, push_metadata=push_metadata,
                root_action=root_action)
        assert revid == rev.revision_id or not push_metadata
        self._add_path_info(target_path, revid, foreign_info)
        return (revid, foreign_info)

    def _get_branch_config(self, branch_path):
        return SvnBranchStack(urlutils.join(self.target.base, branch_path),
                self.target.uuid)

    def push_ancestors(self, layout, project, parent_revids, lossy=False,
                       exclude=None):
        """Push the ancestors of a revision.

        :param layout: Subversion layout
        :param project: Project name
        :param parent_revids: The revision ids of the basic ancestors to push
        """
        present_rhs_parents = self.target.has_revisions(parent_revids[1:])
        unique_ancestors = set()
        missing_rhs_parents = set(parent_revids[1:]) - present_rhs_parents
        graph = self.get_graph()
        for parent_revid in missing_rhs_parents:
            # Push merged revisions
            ancestors = graph.find_unique_ancestors(parent_revid,
                [parent_revids[0]])
            unique_ancestors.update(ancestors)
        for x in self.get_graph().iter_topo_order(unique_ancestors):
            if self._target_has_revision(x, project):
                continue
            rev = self.source.get_revision(x)
            rhs_branch_path = determine_branch_path(rev, layout, project)
            mutter("pushing ancestor %r to %s", x, rhs_branch_path)

            if rev.parent_ids:
                parent_revid = rev.parent_ids[0]
            else:
                parent_revid = NULL_REVISION

            base_foreign_revid, base_mapping = self._get_foreign_revision_info(
                parent_revid)
            if base_foreign_revid is None:
                target_project = None
            else:
                (_, target_project, _, _) = layout.parse(base_foreign_revid[1])
            bp = determine_branch_path(rev, layout, target_project, exclude)
            target_config = self._get_branch_config(bp)
            push_merged = (layout.push_merged_revisions(target_project) and
                target_config.get('push_merged_revisions'))
            root_action = self._get_root_action(bp, rev.parent_ids,
                overwrite=True,
                append_revisions_only=(target_config.get('append_revisions_only') or False),
                create_prefix=True)
            self.push_revision_inclusive(bp, target_config, rev,
                push_metadata=not lossy, push_merged=push_merged,
                layout=layout, project=target_project,
                root_action=root_action,
                base_foreign_info=(base_foreign_revid, base_mapping))

    def push_new_branch(self, layout, project, target_branch_path,
        stop_revision, push_metadata, overwrite=False):
        """Push a new branch.

        :param layout: Repository layout to use
        :param project: Project name
        :param target_branch_path: Target branch path
        :param stop_revision: New branch tip revision id
        :param push_merged: Whether to push merged revisions
        :param overwrite: Whether to override any existing branch
        """
        target_config = self._get_branch_config(target_branch_path)
        mapping = self.target.get_mapping()
        push_merged = (layout.push_merged_revisions(project) and
            target_config.get('push_merged_revisions'))

        graph = self.source.get_graph()
        start_revid_parent = NULL_REVISION
        start_revid = stop_revision
        for revid in graph.iter_lefthand_ancestry(stop_revision,
                (NULL_REVISION,)):
            if self._target_has_revision(revid):
                start_revid_parent = revid
                break
            start_revid = revid
        try:
            # If this is just intended to create a new branch
            if (start_revid_parent != NULL_REVISION and
                stop_revision == start_revid and
                (mapping.supports_hidden or not push_metadata)):
                if (self._target_has_revision(start_revid) or start_revid == NULL_REVISION):
                    revid = start_revid
                else:
                    revid = start_revid_parent
                begin_revid, begin_foreign_info = self.copy_revision(
                    target_branch_path, stop_revision=revid, set_metadata=push_metadata,
                    deletefirst=False)
            else:
                start_parent_foreign_info = self._get_foreign_revision_info(
                    start_revid_parent)
                rev = self.source.get_revision(start_revid)
                begin_revid, begin_foreign_info = self.push_single_revision(
                    target_branch_path, target_config,
                    rev, push_metadata=push_metadata,
                    base_foreign_info=start_parent_foreign_info,
                    root_action=("create", ))
        except SubversionException as e:
            if e.args[1] == subvertpy.ERR_FS_TXN_OUT_OF_DATE:
                raise AlreadyBranchError(target_branch_path)
            raise

        if stop_revision != begin_revid:
            begin_foreign_revinfo = self._get_foreign_revision_info(
                begin_revid)
            self.push_todo(
                begin_revid, begin_foreign_info[0], mapping, stop_revision,
                layout, project, target_branch_path, target_config,
                push_merged=push_merged, overwrite=False, push_metadata=push_metadata,
                append_revisions_only=True)

    def get_graph(self):
        if self._graph is None:
            self._graph = self.source.get_graph(self.target)
        return self._graph

    def copy_content(self, revision_id=None, pb=None, project=None,
            mapping=None, limit=None, lossy=False, exclude_non_mainline=None):
        """See InterRepository.copy_content."""
        with self.source.lock_read(), self.target.lock_write():
            graph = self.get_graph()
            if revision_id is not None:
                heads = [revision_id]
            else:
                heads = graph.heads(self.source.all_revision_ids())
                exclude_non_mainline = False
            todo = []
            # Go back over the LHS parent until we reach a revid we know
            for head in heads:
                try:
                    for revid in graph.iter_lefthand_ancestry(head,
                            (NULL_REVISION, None)):
                        if self._target_has_revision(revid):
                            break
                        todo.append(revid)
                except RevisionNotPresent as e:
                    raise NoSuchRevision(self.source, e.revision_id)
            todo.reverse()
            if limit is not None:
                # FIXME: This only considers mainline revisions.
                # Properly keeping track of how many revisions have been
                # pushed will be fairly complicated though, so for the
                # moment this is reasonable enough (and passes tests).
                todo = todo[:limit]
            mutter("pushing %r into svn", todo)
            base_foreign_info = None
            layout = self.target.get_layout()
            for rev in self.source.get_revisions(todo):
                if pb is not None:
                    pb.update("pushing revisions",
                        todo.index(rev.revision_id), len(todo))
                mutter('pushing %r', rev.revision_id)

                if base_foreign_info is None:
                    if rev.parent_ids:
                        base_revid = rev.parent_ids[0]
                    else:
                        base_revid = NULL_REVISION
                    base_foreign_info  = self._get_foreign_revision_info(
                        base_revid)

                (base_foreign_revid, base_mapping) = base_foreign_info
                if base_foreign_revid is None:
                    target_project = None
                else:
                    (_, target_project, _, _) = layout.parse(
                        base_foreign_revid[1])
                bp = determine_branch_path(rev, layout, target_project)
                mutter("pushing revision include %r to %s",
                        rev.revision_id, bp)
                target_config = self._get_branch_config(bp)
                can_push_merged = layout.push_merged_revisions(target_project)
                if exclude_non_mainline is None:
                    push_merged = can_push_merged and (
                        target_config.get('push_merged_revisions'))
                else:
                    push_merged = (not exclude_non_mainline)
                if push_merged and not can_push_merged:
                    raise BzrError(
                        "Unable to push merged revisions, layout "
                        "does not provide branch path")
                append_revisions_only = target_config.get('append_revisions_only')
                if append_revisions_only is None:
                    append_revisions_only = True
                root_action = self._get_root_action(bp, rev.parent_ids,
                    overwrite=False,
                    append_revisions_only=append_revisions_only,
                    create_prefix=True)
                (pushed_revid,
                        base_foreign_info) = self.push_revision_inclusive(
                    bp, target_config, rev, push_metadata=not lossy,
                    push_merged=push_merged, root_action=root_action,
                    layout=layout, project=target_project,
                    base_foreign_info=base_foreign_info)

    def fetch(self, revision_id=None, pb=None, find_ghosts=False,
        fetch_spec=None, project=None, mapping=None, target_is_empty=False,
        limit=None, exclude_non_mainline=None, lossy=False):
        """Fetch revisions. """
        if fetch_spec is not None:
            recipe = fetch_spec.get_recipe()
            if recipe[0] in ("search", "proxy-search"):
                heads = recipe[1]
            else:
                raise AssertionError("Unknown search type %s" % recipe[0])
            for revid in heads:
                self.copy_content(revision_id=revid, pb=pb, project=project,
                    mapping=mapping, limit=limit,
                    exclude_non_mainline=exclude_non_mainline)
        else:
            self.copy_content(revision_id=revision_id, pb=pb, project=project,
                    mapping=mapping, limit=limit,
                    exclude_non_mainline=exclude_non_mainline)

    @staticmethod
    def is_compatible(source, target):
        """Be compatible with SvnRepository."""
        if not isinstance(target, SvnRepository):
            return False
        return True


def determine_branch_path(rev, layout, project=None, exclude=None):
    """Create a sane branch path to use for a revision.

    :param rev: Revision object
    :param layout: Subversion layout
    :param project: Optional project name, as used by the layout
    :return: Branch path string
    """
    if exclude is None:
        exclude = []
    bp = None
    nick = (rev.properties.get('branch-nick') or "merged")
    nick = nick.encode("utf-8").replace("/","_")
    while True:
        if project is None:
            bp = layout.get_branch_path(nick)
        else:
            bp = layout.get_branch_path(nick, project)
        if bp not in exclude:
            return bp
        nick += '-merged'


def create_branch_with_hidden_commit(repository, branch_path, revid,
                                     set_metadata=True, deletefirst=False):
    """Create a new branch using a simple "svn cp" operation.

    :param repository: Repository in which to create the branch.
    :param branch_path: Branch path
    :param revid: Revision id to keep as tip.
    :param deletefirst: Whether to delete an existing branch at this location
        first.
    :return: Revision id that was pushed and the related foreign revision id.
    """
    revprops = {properties.PROP_REVISION_LOG: "Create new branch."}
    if revid == NULL_REVISION:
        old_fileprops = {}
        fileprops = {}
        mapping = repository.get_mapping()
        from_url = None
        from_revnum = -1
    else:
        revmeta, mapping = repository._get_revmeta(revid)
        old_fileprops = revmeta.get_fileprops()
        fileprops = dict(old_fileprops.items())
        from_url = url_join_unescaped_path(repository.base,
            revmeta.metarev.branch_path)
        from_revnum = revmeta.metarev.revnum
    if set_metadata:
        if not mapping.supports_hidden:
            raise AssertionError("mapping format %r doesn't support hidden" %
                mapping)
        to_url = urlutils.join(repository.base, branch_path)
        config = SvnBranchStack(to_url, repository.uuid)
        (set_custom_revprops,
            set_custom_fileprops) = repository._properties_to_set(mapping, config)
        if set_custom_revprops:
            mapping.export_hidden_revprops(branch_path, revprops)
            if (not set_custom_fileprops and
                not repository.svn_transport.has_capability("log-revprops")):
                # Tell clients about first approximate use of revision
                # properties
                mapping.export_revprop_redirect(
                    repository.get_latest_revnum()+1, fileprops)
        if set_custom_fileprops:
            mapping.export_hidden_fileprops(fileprops)
    parent = urlutils.dirname(branch_path)

    bp_parts = branch_path.split("/")
    existing_bp_parts = check_dirs_exist(repository.svn_transport, bp_parts, -1)
    if len(bp_parts) not in (len(existing_bp_parts), len(existing_bp_parts)+1):
        raise MissingPrefix("/".join(bp_parts), "/".join(existing_bp_parts))

    if deletefirst is None:
        deletefirst = (bp_parts == existing_bp_parts)

    foreign_revid = [repository.uuid, branch_path]

    def done(revno, *args):
        foreign_revid.append(revno)

    conn = repository.svn_transport.get_connection(parent)
    try:
        with convert_svn_error(conn.get_commit_editor)(revprops, done) as ci:
            root = ci.open_root()
            if deletefirst:
                try:
                    root.delete_entry(urlutils.basename(branch_path))
                except SubversionException as e:
                    if e.args[1] == ERR_FS_ROOT_DIR:
                        raise ChangesRootLHSHistory()
                    raise
            branch_dir = root.add_directory(
                urlutils.basename(branch_path), from_url, from_revnum)
            for k, (ov, nv) in properties.diff(fileprops, old_fileprops).items():
                branch_dir.change_prop(k, nv)
            branch_dir.close()
            root.close()
        repository._cache_add_new_revision(foreign_revid[2], revid, None)
        return revid, (tuple(foreign_revid), mapping)
    finally:
        repository.svn_transport.add_connection(conn)
