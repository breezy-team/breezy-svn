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

try:
    from collections import defaultdict
except ImportError:
    from bzrlib.plugins.svn.pycompat import defaultdict

import subvertpy
from subvertpy import (
    ERR_FS_TXN_OUT_OF_DATE,
    SubversionException,
    properties,
    )

from bzrlib import (
    ui,
    urlutils,
    )
from bzrlib.errors import (
    AlreadyBranchError,
    AppendRevisionsOnlyViolation,
    BzrError,
    DivergedBranches,
    NoSuchRevision,
    )
from bzrlib.repository import (
    InterRepository,
    )
from bzrlib.revision import (
    NULL_REVISION,
    )
from bzrlib.testament import (
    StrictTestament,
    )
from bzrlib.trace import (
    mutter,
    )

from bzrlib.plugins.svn.commit import (
    SvnCommitBuilder,
    )
from bzrlib.plugins.svn.config import (
    BranchConfig,
    )
from bzrlib.plugins.svn.errors import (
    ChangesRootLHSHistory,
    MissingPrefix,
    convert_svn_error,
    )
from bzrlib.plugins.svn.mapping import (
    SVN_REVPROP_BZR_SKIP,
    )
from bzrlib.plugins.svn.repository import (
    SvnRepositoryFormat,
    SvnRepository,
    )
from bzrlib.plugins.svn.transport import (
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


def find_push_base_revision(source, target, stop_revision):
    """Find the first revision to push.

    """
    start_revid = stop_revision
    for revid in source.iter_reverse_revision_history(stop_revision):
        if target.has_revision(revid):
            break
        start_revid = revid
    return start_revid


def _filter_iter_changes(iter_changes):
    """Process iter_changes.

    Converts 'missing' entries in the iter_changes iterator to 'deleted'
    entries.

    :param iter_changes: An iter_changes to process.
    :return: A generator of changes.
    """
    for change in iter_changes:
        kind = change[6][1]
        versioned = change[3][1]
        if kind is None and versioned:
            # 'missing' path
            # Reset the new path (None) and new versioned flag (False)
            change = (change[0], (change[1][0], None), change[2],
                (change[3][0], False)) + change[4:]
        if change[3][0] or change[3][1]:
            yield change


def push_revision_tree(graph, target_repo, branch_path, config, source_repo,
                       base_revid, revision_id, rev,
                       base_foreign_revid, base_mapping,
                       push_metadata,
                       root_action):
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
    :param root_action: Action to take on the tree root
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

    if push_metadata:
        testament = StrictTestament(rev, old_tree.inventory)
    else:
        testament = None

    builder = SvnCommitBuilder(target_repo, branch_path, base_revids,
                               config, rev.timestamp,
                               rev.timezone, rev.committer, rev.properties,
                               revision_id, base_foreign_revid, base_mapping,
                               root_action, base_tree.inventory,
                               push_metadata=push_metadata,
                               graph=graph, opt_signature=opt_signature,
                               texts=source_repo.texts,
                               testament=testament)
    try:
        builder.will_record_deletes()
        iter_changes = old_tree.iter_changes(base_tree)
        iter_changes = _filter_iter_changes(iter_changes)
        for file_id, path, fs_hash in builder.record_iter_changes(
            old_tree, base_tree.get_revision_id(), iter_changes):
            pass
        builder.finish_inventory()
    except:
        builder.abort()
        raise
    try:
        revid = builder.commit(rev.message)
    except SubversionException, (msg, num):
        if num == ERR_FS_TXN_OUT_OF_DATE:
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

    def _target_has_revision(self, revid):
        """Slightly optimized version of self.target.has_revision()."""
        if revid in self._foreign_info:
            return True
        return self.target.has_revision(revid)

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

    def _add_path_info(self, revid, path, foreign_info):
        self._foreign_info[revid][path] = foreign_info

    @staticmethod
    def _get_repo_format_to_test():
        """See InterRepository._get_repo_format_to_test()."""
        return None

    def _get_root_action(self, path, rev, overwrite, target_config, create_prefix=False):
        append_revisions_only = target_config.get_append_revisions_only(not overwrite)
        bp_parts = path.split("/")
        existing_bp_parts = check_dirs_exist(self.target.transport, bp_parts, -1)
        if (len(existing_bp_parts) != len(bp_parts) and
            len(existing_bp_parts)+1 != len(bp_parts)):
            existing_path = "/".join(existing_bp_parts)
            if create_prefix:
                create_branch_container(self.target.transport, path, existing_path)
                return ("create", )
            raise MissingPrefix(path, existing_path)
        if len(existing_bp_parts) < len(bp_parts):
            # Branch doesn't exist yet
            return ("create", )
        delete_root_revnum = self._get_delete_root_revnum(
            path, rev.parent_ids,
            overwrite=overwrite, append_revisions_only=append_revisions_only)
        if len(existing_bp_parts) == len(bp_parts) and delete_root_revnum is None:
            return ("open", )
        elif delete_root_revnum is not None:
            return ("replace", delete_root_revnum)

    def _get_delete_root_revnum(self, target_path, parent_revids, overwrite,
            append_revisions_only):
        #FIXME
        from bzrlib.branch import Branch
        url = urlutils.join(self.target.base, target_path)
        b = Branch.open(url)
        if parent_revids == [] or parent_revids == [NULL_REVISION]:
            return b.get_revnum()
        else:
            if b.last_revision() not in parent_revids:
                if not overwrite:
                    raise DivergedBranches(self.source, self.target)
                return b.get_revnum()
            elif b.last_revision() != parent_revids[0]:
                if append_revisions_only:
                    raise AppendRevisionsOnlyViolation(url)
                return b.get_revnum()
            return None

    def push_revision_series(self, todo, layout, project, target_branch,
            target_config, push_merged, overwrite, push_metadata):
        """Push a series of revisions into a Subversion repository.

        :param todo: New revisions to push
        :return: Dictionary mapping revision ids to tuples
            with new revision id and foreign info.
        """
        assert todo != []
        revid_map = {}
        root_action = None
        pb = ui.ui_factory.nested_progress_bar()
        try:
            for rev in self.source.get_revisions(todo):
                if root_action is None:
                    root_action = self._get_root_action(target_branch, rev,
                        overwrite=overwrite, target_config=target_config)
                pb.update("pushing revisions", todo.index(rev.revision_id),
                          len(todo))
                if len(rev.parent_ids) == 0:
                    base_revid = NULL_REVISION
                elif rev.parent_ids[0] in revid_map:
                    base_revid = revid_map[rev.parent_ids[0]][0]
                else:
                    base_revid = rev.parent_ids[0]
                last_revid, last_foreign_info = self.push_revision_inclusive(
                    target_branch, target_config, rev,
                    root_action=root_action,
                    base_revid=base_revid, push_metadata=push_metadata,
                    push_merged=push_merged, project=project, layout=layout)
                revid_map[rev.revision_id] = (last_revid, last_foreign_info)
                root_action = ("open", )
            return revid_map
        finally:
            pb.finished()

    def push_revision_inclusive(self, target_path, target_config, rev,
            push_merged, layout, project, root_action, push_metadata,
            base_revid=None):
        """Push a revision including ancestors.

        :return: Tuple with pushed revision id and foreign revision id
        """
        if push_merged and len(rev.parent_ids) > 1:
            self.push_ancestors(layout, project, rev.parent_ids)
        return self.push_single_revision(target_path, target_config, rev,
            push_metadata=push_metadata, base_revid=base_revid,
            root_action=root_action)

    def push_single_revision(self, target_path, target_config, rev,
            push_metadata, root_action, base_revid=None):
        """Push a single revision.

        :param target_path: Target branch path in the svn repository
        :param target_config: Config object for the target branch
        :param rev: Revision object of revision that needs to be pushed
        :param append_revisions_only: Whether to append revisions only
        :param push_metadata: Whether to push svn-specific metadata
        :param base_revid: Base revision (used when pushing a custom base),
            e.g. during dpush.
        :param delete_root_revnum: If not None, maximum revision of the root to delete
        :return: Tuple with pushed revision id and foreign revision id
        """
        if base_revid is None:
            if rev.parent_ids:
                base_revid = rev.parent_ids[0]
            else:
                base_revid = NULL_REVISION
        base_foreign_revid, base_mapping = self._get_foreign_revision_info(
            base_revid, target_path)
        if rev.parent_ids:
            base_revid = rev.parent_ids[0]
        else:
            base_revid = NULL_REVISION

        mutter('pushing %r (%r)', rev.revision_id, rev.parent_ids)
        self.source.lock_read()
        try:
            revid, foreign_info = push_revision_tree(self.get_graph(),
                self.target, target_path, target_config, self.source,
                base_revid, rev.revision_id, rev, base_foreign_revid,
                base_mapping, push_metadata=push_metadata,
                root_action=root_action)
        finally:
            self.source.unlock()
        assert revid == rev.revision_id or not push_metadata
        self._add_path_info(target_path, revid, foreign_info)
        return (revid, foreign_info)

    def _get_branch_config(self, branch_path):
        return BranchConfig(urlutils.join(self.target.base, branch_path),
                self.target.uuid)

    def push_new_branch_first_revision(self, target_branch_path,
            stop_revision, push_metadata=True, append_revisions_only=False):
        """Push a revision into Subversion, creating a new branch.

        :param graph: Repository graph.
        :param target_repository: Repository to push to
        :param target_branch_path: Path to create new branch at
        :param source: Source repository
        :return: Revision id of the pushed revision, foreign revision id that
            was pushed
        """
        start_revid = find_push_base_revision(self.source, self.target,
                stop_revision)
        rev = self.source.get_revision(start_revid)
        if rev.parent_ids == []:
            start_revid_parent = NULL_REVISION
        else:
            start_revid_parent = rev.parent_ids[0]
        # If this is just intended to create a new branch
        mapping = self.target.get_mapping()
        if (start_revid_parent != NULL_REVISION and stop_revision == start_revid and (mapping.supports_hidden or not push_metadata) and not append_revisions_only):
            if (self._target_has_revision(start_revid) or
                start_revid == NULL_REVISION):
                revid = start_revid
            else:
                revid = start_revid_parent
            revid, foreign_info = create_branch_with_hidden_commit(self.target,
                target_branch_path, revid, set_metadata=push_metadata,
                deletefirst=False)
        else:
            revid, foreign_info = self.push_single_revision(target_branch_path,
                self._get_branch_config(target_branch_path),
                rev, push_metadata=push_metadata,
                base_revid=start_revid_parent,
                root_action=("create", ))
        self._add_path_info(target_branch_path, revid, foreign_info)
        return revid, foreign_info

    def push_ancestors(self, layout, project, parent_revids):
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
            ancestors = graph.find_unique_ancestors(parent_revid, [parent_revids[0]])
            unique_ancestors.update(ancestors)
        for x in self.get_graph().iter_topo_order(unique_ancestors):
            if self._target_has_revision(x):
                continue
            rev = self.source.get_revision(x)
            rhs_branch_path = determine_branch_path(rev, layout, project)
            # FIXME: See if the existing revision at rhs_branch_path is already
            # at base revision
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
            bp = determine_branch_path(rev, layout, target_project)
            target_config = self._get_branch_config(bp)
            push_merged = (layout.push_merged_revisions(target_project) and
                target_config.get_push_merged_revisions())
            root_action = self._get_root_action(bp, rev, overwrite=False,
                target_config=target_config, create_prefix=True)
            self.push_revision_inclusive(bp, target_config, rev,
                push_metadata=True, push_merged=push_merged,
                layout=layout, project=target_project,
                root_action=root_action)

    def push_new_branch(self, layout, project, target_branch_path,
        stop_revision, push_merged=None, overwrite=False):
        """Push a new branch.

        :param layout: Repository layout to use
        :param project: Project name
        :param target_branch_path: Target branch path
        :param stop_revision: New branch tip revision id
        :param push_merged: Whether to push merged revisions
        :param overwrite: Whether to override any existing branch
        """
        if self.target.transport.check_path(target_branch_path,
            self.target.get_latest_revnum()) != subvertpy.NODE_NONE:
            raise AlreadyBranchError(target_branch_path)
        target_config = self._get_branch_config(target_branch_path)
        if push_merged is None:
            push_merged = (layout.push_merged_revisions(project) and
                           target_config.get_push_merged_revisions())
        begin_revid, _ = self.push_new_branch_first_revision(
            target_branch_path, stop_revision, append_revisions_only=True)
        todo = []
        for revid in self.source.iter_reverse_revision_history(stop_revision):
            if revid == begin_revid:
                break
            todo.append(revid)
        todo.reverse()
        if todo != []:
            self.push_revision_series(todo, layout, project,
                target_branch_path, target_config, push_merged,
                overwrite=overwrite, push_metadata=True)

    def get_graph(self):
        if self._graph is None:
            self._graph = self.source.get_graph(self.target)
        return self._graph

    def copy_content(self, revision_id=None, pb=None):
        """See InterRepository.copy_content."""
        self.source.lock_read()
        try:
            assert revision_id is not None, "fetching all revisions not supported"
            # Go back over the LHS parent until we reach a revid we know
            todo = []
            for revision_id in self.source.iter_reverse_revision_history(revision_id):
                if self._target_has_revision(revision_id):
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
                mutter('pushing %r', rev.revision_id)

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
                bp = determine_branch_path(rev, layout, target_project)
                target_config = self._get_branch_config(bp)
                push_merged = (layout.push_merged_revisions(target_project) and
                    target_config.get_push_merged_revisions())
                root_action = self._get_root_action(bp, rev, overwrite=False, target_config=target_config)
                (pushed_revid, (pushed_foreign_revid, pushed_mapping)) = self.push_revision_inclusive(bp,
                    target_config, rev, push_metadata=True,
                    root_action=root_action, layout=layout, project=target_project)
                overwrite_revnum = pushed_foreign_revid[2]
        finally:
            self.source.unlock()

    def fetch(self, revision_id=None, pb=None, find_ghosts=False,
        fetch_spec=None):
        """Fetch revisions. """
        if fetch_spec is not None:
            recipe = fetch_spec.get_recipe()
            if recipe[0] in ("search", "proxy-search"):
                heads = recipe[1]
            else:
                raise AssertionError("Unknown search type %s" % recipe[0])
            for revid in heads:
                self.copy_content(revision_id=revid, pb=pb)
        else:
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


def create_branch_with_hidden_commit(repository, branch_path, revid,
                                     set_metadata=True,
                                     deletefirst=False):
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
        fileprops = dict(old_fileprops.iteritems())
        from_url = url_join_unescaped_path(repository.base,
            revmeta.branch_path)
        from_revnum = revmeta.revnum
    if set_metadata:
        if not mapping.supports_hidden:
            raise AssertionError("mapping format %r doesn't support hidden" %
                mapping)
        (set_custom_revprops,
            set_custom_fileprops) = repository._properties_to_set(mapping)
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
    existing_bp_parts = check_dirs_exist(repository.transport, bp_parts, -1)
    if (len(bp_parts) not in (len(existing_bp_parts), len(existing_bp_parts)+1)):
        raise MissingPrefix("/".join(bp_parts), "/".join(existing_bp_parts))

    if deletefirst is None:
        deletefirst = (bp_parts == existing_bp_parts)

    foreign_revid = [repository.uuid, branch_path]

    def done(revno, *args):
        foreign_revid.append(revno)

    conn = repository.transport.get_connection(parent)
    try:
        ci = convert_svn_error(conn.get_commit_editor)(revprops, done)
        try:
            root = ci.open_root()
            if deletefirst:
                root.delete_entry(urlutils.basename(branch_path))
            branch_dir = root.add_directory(
                urlutils.basename(branch_path), from_url, from_revnum)
            for k, (ov, nv) in properties.diff(fileprops, old_fileprops).iteritems():
                branch_dir.change_prop(k, nv)
            branch_dir.close()
            root.close()
        except:
            ci.abort()
            raise
        ci.close()
        return revid, (tuple(foreign_revid), mapping)
    finally:
        repository.transport.add_connection(conn)
