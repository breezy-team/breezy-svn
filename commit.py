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


"""Committing to Subversion repositories."""

from __future__ import absolute_import

from collections import defaultdict

from functools import partial

from io import (
    BytesIO,
    )
from subvertpy import (
    ERR_BAD_PROPERTY_VALUE,
    ERR_FS_NOT_DIRECTORY,
    ERR_REPOS_DISABLED_FEATURE,
    delta,
    properties,
    SubversionException,
    ra,
    )

from breezy import (
    debug,
    graph as _mod_graph,
    osutils,
    urlutils,
    trace,
    ui,
    )
from breezy.errors import (
    BzrError,
    NoSuchId,
    NoSuchRevision,
    RootMissing,
    )
from breezy.bzr.inventory import (
    entry_factory,
    )
from six import text_type
from breezy.repository import (
    CommitBuilder,
    )
from breezy.revision import (
    NULL_REVISION,
    )

from . import (
    mapping,
    )
from .errors import (
    convert_svn_error,
    ChangesRootLHSHistory,
    MissingPrefix,
    RevpropChangeFailed,
    )
from .svk import (
    generate_svk_feature,
    parse_svk_features,
    serialize_svk_features,
    SVN_PROP_SVK_MERGE
    )
from .transport import (
    url_join_unescaped_path,
    )
from .util import (
    lazy_dict,
    )

DUMMY_ROOT_PROPERTY_NAME = "bzr:svn:dummy"

_fileprops_warned_repos = set()


def warn_setting_fileprops(uuid):
    """Warn the user about the fact that file properties are going to be set.
    """
    if uuid in _fileprops_warned_repos:
        return
    _fileprops_warned_repos.add(uuid)
    trace.warning(
            "Storing Bazaar metadata in file properties. "
            "Use `bzr push --lossy` for lossfull push without file properties."
            "Upgrade the server to Subversion 1.5 or later to use "
            "(less visible) revision properties.")


def _revision_id_to_svk_feature(revid, lookup_revid):
    """Create a SVK feature identifier from a revision id.

    :param revid: Revision id to convert.
    :return: Matching SVK feature identifier.
    """
    assert isinstance(revid, bytes)
    foreign_revid, _ = lookup_revid(revid)
    # TODO: What about renamed revisions? Should use
    # repository.lookup_bzr_revision_id here.
    return generate_svk_feature(foreign_revid)


def update_svk_features(oldvalue, merges, lookup_revid=None):
    """Update a set of SVK features to include the specified set of merges."""

    if lookup_revid is None:
        lookup_revid = mapping.mapping_registry.parse_revision_id

    old_svk_features = parse_svk_features(oldvalue)
    svk_features = set(old_svk_features)

    # SVK compatibility
    for merge in merges:
        try:
            svk_features.add(_revision_id_to_svk_feature(merge, lookup_revid))
        except NoSuchRevision:
            pass

    if old_svk_features != svk_features:
        return serialize_svk_features(svk_features)
    return None


def update_mergeinfo(lookup_revid, graph, oldvalue, baserevid, merges):
    """Update a svn:mergeinfo property to include a specified list of merges.
    """
    with ui.ui_factory.nested_progress_bar() as pb:
        mergeinfo = properties.parse_mergeinfo_property(oldvalue)
        for i, merge in enumerate(merges):
            pb.update("updating mergeinfo property", i, len(merges))
            for (revid, parents) in graph.iter_ancestry([merge]):
                if graph.is_ancestor(revid, baserevid):
                    break
                try:
                    (uuid, path, revnum), mapping = lookup_revid(revid)
                except NoSuchRevision:
                    break

                properties.mergeinfo_add_revision(
                    mergeinfo, "/" + path, revnum)
    newvalue = properties.generate_mergeinfo_property(mergeinfo)
    if newvalue != oldvalue:
        return newvalue
    return None


def find_suitable_base(parents, ie):
    """Find a suitable base inventory and path to copy from.

    :param parents: List with (inventory, url, revnum) tuples.
    :param ie: Inventory entry to find a base for
    :return: Tuple with the most suitable (inventory, url, revnum) instance
        or None if none was found
    """
    candidates = []
    # Filter out which parents have this file id
    for (tree, url, revnum) in parents:
        try:
            path = tree.id2path(ie.file_id)
        except NoSuchId:
            pass
        else:
            if ie.kind == tree.kind(path):
                candidates.append((tree, url, revnum))
    if candidates == []:
        return None
    # Check if there's any candidate that has the *exact* revision we need
    for (tree, url, revnum) in candidates:
        try:
            path = tree.id2path(ie.file_id)
        except NoSuchId:
            pass
        else:
            if tree.get_file_revision(path, ie.file_id) == ie.revision:
                return (tree, url, revnum)
    # TODO: Ideally we should now pick the candidate with the least changes
    # for now, just pick the first candidate
    return candidates[0]


def set_svn_revprops(repository, revnum, revprops):
    """Attempt to change the revision properties on the
    specified revision.

    :param transport: SvnRaTransport connected to target repository
    :param revnum: Revision number of revision to change metadata of.
    :param revprops: Dictionary with revision properties to set.
    """
    for (name, value) in revprops.items():
        try:
            repository.svn_transport.change_rev_prop(revnum, name, value)
        except SubversionException as e:
            if e.args[1] == ERR_REPOS_DISABLED_FEATURE:
                raise RevpropChangeFailed(name)
            else:
                raise


def file_editor_send_content_changes(reader, file_editor):
    """Pass the changes to a file to the Subversion commit editor.

    :param reader: A object that can read the file contents
    :param file_editor: Subversion FileEditor object.
    """
    assert file_editor is not None
    txdelta = file_editor.apply_textdelta()
    # FIXME: Send delta against base rather than sending the
    # full contents.
    if 'check' in debug.debug_flags:
        contents = reader.read()
        digest = delta.send_stream(BytesIO(contents), txdelta)
        from .fetch import md5_string
        assert digest == md5_string(contents)
    else:
        delta.send_stream(reader, txdelta)


def path_join(basepath, name):
    if basepath:
        return urlutils.join(basepath, name)
    else:
        return name


def dir_editor_send_changes(
        base_tuple, parents, browse_tuple, path, file_id, dir_editor,
        branch_path, modified_files, visit_dirs):
    """Pass the changes to a directory to the commit editor.

    :param path: Path (from repository root) to the directory.
    :param file_id: File id of the directory
    :param dir_editor: Subversion DirEditor object.
    :return: Boolean indicating whether any changes were made
    """
    (base_tree, base_url, base_revnum) = base_tuple
    (iter_children, get_ie) = browse_tuple
    changed = False

    def mutter(text, *args):
        if 'commit' in debug.debug_flags:
            trace.mutter(text, *args)

    def branch_relative_path(*args):
        if branch_path is None:
            return urlutils.join(*args)
        else:
            return urlutils.join(branch_path, *args)

    assert dir_editor is not None
    # Loop over entries of file_id in base_tree
    # remove if they no longer exist with the same name
    # or parents
    try:
        base_path = base_tree.id2path(file_id)
    except NoSuchId:
        base_path = None
    else:
        if base_tree.kind(base_path) == 'directory':
            for child_ie in base_tree.iter_child_entries(base_path):
                new_child_ie = get_ie(child_ie.file_id)
                # remove if...
                if (
                    # ... path no longer exists
                    new_child_ie is None or
                        # ... parent changed
                        child_ie.parent_id != new_child_ie.parent_id or
                        # ... name changed
                        child_ie.name != new_child_ie.name or
                        # ... kind changed
                        child_ie.kind != new_child_ie.kind):
                    mutter('removing %r(%r)', child_ie.name, child_ie.file_id)
                    dir_editor.delete_entry(
                        branch_relative_path(path, child_ie.name),
                        base_revnum)
                    changed = True

    # Loop over file children of file_id in new_inv
    for child_ie in iter_children(file_id):
        assert child_ie is not None

        if not (child_ie.kind in ('file', 'symlink')):
            continue

        new_child_path = path_join(path, child_ie.name)
        full_new_child_path = branch_relative_path(new_child_path)
        child_editor = None
        try:
            base_path = base_tree.id2path(child_ie.file_id)
        except NoSuchId:
            base_path = None
        try:
            # add them if they didn't exist in base_tree or changed kind
            if (base_path is not None or
                    base_tree.kind(base_path) != child_ie.kind):
                mutter('adding %s %r', child_ie.kind, new_child_path)

                # Do a copy operation if at all possible, to make
                # the log a bit easier to read for Subversion people
                new_base = find_suitable_base(parents, child_ie)
                if new_base is not None:
                    child_base_path = new_base[0].id2path(child_ie.file_id)
                    copyfrom_url = url_join_unescaped_path(
                        new_base[1], child_base_path)
                    copyfrom_revnum = new_base[2]
                else:
                    # No suitable base found
                    copyfrom_url = None
                    copyfrom_revnum = -1

                child_editor = dir_editor.add_file(
                    full_new_child_path, copyfrom_url, copyfrom_revnum)
                changed = True
            # copy if they existed at different location
            elif (base_path != new_child_path or
                      base_tree.root_inventory.get_entry(child_ie.file_id).parent_id != child_ie.parent_id):
                mutter('copy %s %r -> %r', child_ie.kind, base_path,
                       new_child_path)
                child_editor = dir_editor.add_file(
                    full_new_child_path,
                    url_join_unescaped_path(base_url, base_path),
                    base_revnum)
                changed = True
            # open if they existed at the same location
            elif child_ie.file_id in modified_files:
                mutter('open %s %r', child_ie.kind, new_child_path)
                child_editor = dir_editor.open_file(
                    full_new_child_path, base_revnum)
            else:
                # Old copy of the file was retained. No need to send changes
                child_editor = None

            # handle the file
            if child_ie.file_id in modified_files:
                changed = True
                # The file editors implicitly close the file editor
                modified_files[child_ie.file_id](child_editor)
                child_editor = None
                del modified_files[child_ie.file_id]
        finally:
            if child_editor is not None:
                child_editor.close()

    # Loop over subdirectories of file_id in new_inv
    for child_ie in iter_children(file_id):
        if child_ie.kind != 'directory':
            continue

        new_child_path = path_join(path, child_ie.name)
        child_editor = None
        try:
            base_path = base_tree.id2path(child_ie.file_id)
        except NoSuchId:
            base_path = None
        try:
            # add them if they didn't exist in base_tree or changed kind
            if (base_path is not None or
                    base_tree.kind(base_path) != child_ie.kind):
                mutter('adding dir %r', child_ie.name)

                # Do a copy operation if at all possible, to make
                # the log a bit easier to read for Subversion people
                new_base = find_suitable_base(parents, child_ie)
                if new_base is not None:
                    child_base = new_base
                    child_base_path = new_base[0].id2path(child_ie.file_id)
                    assert isinstance(new_base[1], text_type)
                    copyfrom_url = url_join_unescaped_path(
                        new_base[1], child_base_path)
                    copyfrom_revnum = new_base[2]
                else:
                    # No suitable base found
                    child_base = (base_tree, base_url, base_revnum)
                    copyfrom_url = None
                    copyfrom_revnum = -1

                child_editor = dir_editor.add_directory(
                    branch_relative_path(new_child_path), copyfrom_url,
                    copyfrom_revnum)
                changed = True
            # copy if they existed at different location
            elif (base_tree.id2path(child_ie.file_id) != new_child_path or
                  base_tree.root_inventory.get_entry(child_ie.file_id).parent_id != child_ie.parent_id):
                old_child_path = base_tree.id2path(child_ie.file_id)
                mutter('copy dir %r -> %r', old_child_path, new_child_path)
                copyfrom_url = url_join_unescaped_path(
                    base_url, old_child_path)
                copyfrom_revnum = base_revnum

                child_editor = dir_editor.add_directory(
                    branch_relative_path(new_child_path),
                    copyfrom_url, copyfrom_revnum)
                changed = True
                child_base = (base_tree, base_url, base_revnum)
            # open if they existed at the same location and
            # the directory was touched
            elif new_child_path in visit_dirs:
                mutter('open dir %r', new_child_path)

                child_editor = dir_editor.open_directory(
                        branch_relative_path(new_child_path),
                        base_revnum)

                child_base = (base_tree, base_url, base_revnum)
            else:
                continue

            # Handle this directory
            changed = dir_editor_send_changes(
                child_base, parents,
                (iter_children, get_ie), new_child_path, child_ie.file_id,
                child_editor, branch_path, modified_files, visit_dirs) or changed
        finally:
            if child_editor is not None:
                child_editor.close()

    return changed


class SvnCommitBuilder(CommitBuilder):
    """Commit Builder implementation wrapped around svn_delta_editor. """

    _versioned_root = True

    support_use_record_entry_contents = False

    updates_branch = True

    def __init__(self, repository, branch_path, parents, config_stack,
                 timestamp, timezone, committer, revprops, revision_id,
                 base_foreign_revid, base_mapping, root_action, old_tree=None,
                 push_metadata=True, graph=None, opt_signature=None,
                 testament=None, branch=None, _rich_root_bump=False):
        """Instantiate a new SvnCommitBuilder.

        :param repository: SvnRepository to commit to.
        :param branch: branch path to commit to.
        :param parents: List of parent revision ids.
        :param config_stack: Branch configuration stack to use.
        :param timestamp: Optional timestamp recorded for commit.
        :param timezone: Optional timezone for timestamp.
        :param committer: Optional committer to set for commit.
        :param revprops: Bazaar revision properties to set.
        :param revision_id: Revision id for the new revision.
        :param old_tree: Optional revision on top of which
            the commit is happening
        :param push_metadata: Whether or not to push all bazaar metadata
                              (in svn file properties, etc).
        :param graph: Optional graph object
        :param opt_signature: Optional signature to write.
        :param testament: A Testament object to store
        :param root_action: Action to take on the branch root
        """
        self.push_metadata = push_metadata
        super(SvnCommitBuilder, self).__init__(
            repository, parents, config_stack, timestamp, timezone, committer,
            revprops, revision_id)
        self._basis_delta = []
        self.new_inventory = None
        self._any_changes = False

        self.conn = self.repository.svn_transport.get_connection()

        # revision ids are either specified or predictable
        self.revmeta = None
        if not isinstance(branch_path, text_type):
            raise TypeError(branch_path)
        self.branch_path = branch_path
        self.root_action = root_action
        self._override_file_ids = {}
        self._override_text_revisions = {}
        self._override_text_parents = {}
        self._heads = _mod_graph.HeadsCache(repository.get_graph()).heads
        self._branch = branch
        self._rich_root_bump = _rich_root_bump

        # Gather information about revision on top of which the commit is
        # happening
        if parents:
            self.base_revid = parents[0]
        else:
            self.base_revid = NULL_REVISION

        if graph is None:
            self.graph = self.repository.get_graph()
        else:
            self.graph = graph
        self.base_revno = self.graph.find_distance_to_null(self.base_revid, [])
        self.base_foreign_revid = base_foreign_revid
        if self.base_revid == NULL_REVISION:
            self._base_revmeta = None
            self._base_branch_props = {}
            self.base_revnum = -1
            self.base_path = None
            self.base_url = None
            if base_mapping is not None:
                self.base_mapping = base_mapping
            else:
                self.base_mapping = repository.get_mapping()
        else:
            (uuid, self.base_path, self.base_revnum) = base_foreign_revid
            self.base_mapping = base_mapping
            self._base_revmeta = self.repository._revmeta_provider.lookup_revision(
                self.base_path, self.base_revnum)
            self._base_branch_props = self._base_revmeta.get_fileprops()
            self.base_url = urlutils.join(
                self.repository.svn_transport.svn_url, self.base_path)

        self.mapping = self.repository.get_mapping()
        # FIXME: Check that self.mapping >= self.base_mapping

        self._parent_trees = None
        if old_tree is None:
            self.old_tree = self.repository.revision_tree(self.base_revid)
        else:
            self.old_tree = old_tree

        self.new_root_id = self.old_tree.path2id('')

        self._deleted_fileids = set()
        self._updated_children = defaultdict(set)
        self._updated = {}
        self._visit_dirs = set()
        self._touched_dirs = set()
        self._will_record_deletes = False
        self.modified_files = {}
        self.supports_custom_revprops = (
            self.repository.svn_transport.has_capability("commit-revprops"))
        if (self.supports_custom_revprops is None and
                self.mapping.can_use_revprops and
                self.repository.seen_bzr_revprops()):
            raise BzrError(
                "Please upgrade your Subversion client libraries "
                "to 1.5 or higher to be able to commit with "
                "Subversion mapping %s (current version is %r)" % (
                    self.mapping.name,
                    getattr(ra, "api_version", ra.version)()))

        self._svn_revprops = {}
        self._svnprops = lazy_dict(
            {}, dict, self._base_branch_props.items())
        if push_metadata:
            (self.set_custom_revprops,
                self.set_custom_fileprops) = self.repository._properties_to_set(
                    self.mapping, config_stack)
        else:
            self.set_custom_revprops = False
            self.set_custom_fileprops = False
        if self.set_custom_fileprops:
            warn_setting_fileprops(self.repository.uuid)
        if self.supports_custom_revprops:
            # If possible, submit signature directly
            if opt_signature is not None:
                self._svn_revprops[mapping.SVN_REVPROP_BZR_SIGNATURE] = opt_signature
            # Set hint for potential clients that they have to check revision
            # properties
            if (not self.set_custom_fileprops and
                    not self.repository.svn_transport.has_capability("log-revprops")):
                # Tell clients about first approximate use of revision
                # properties
                self.mapping.export_revprop_redirect(
                    self.repository.get_latest_revnum()+1, self._svnprops)
        self._testament = testament

    def _update_properties(self):
        revno = self.base_revno + 1
        if self.set_custom_fileprops:
            self.mapping.export_revision_fileprops(
                self._svnprops, self._timestamp, self._timezone,
                self._committer, self._revprops, self._new_revision_id, revno,
                self.parents, testament=self._testament)
        if self.set_custom_revprops:
            self.mapping.export_revision_revprops(
                self._svn_revprops, self.repository.uuid,
                self.branch_path, self._timestamp, self._timezone,
                self._committer, self._revprops,
                self._new_revision_id, revno, self.parents,
                testament=self._testament)

        # Determine revisions merged in this one
        merges = list(filter(lambda x: x != self.base_revid, self.parents))

        if len(merges) > 0:
            old_svk_merges = self._base_branch_props.get(
                SVN_PROP_SVK_MERGE, "")

            def lookup_revid(revid):
                return self.repository.lookup_bzr_revision_id(
                    revid, foreign_sibling=self.base_foreign_revid)
            new_svk_merges = update_svk_features(
                old_svk_merges, merges, lookup_revid)
            if new_svk_merges is not None:
                self._svnprops[SVN_PROP_SVK_MERGE] = new_svk_merges

            new_mergeinfo = update_mergeinfo(
                lookup_revid, self.graph,
                self._base_branch_props.get(properties.PROP_MERGEINFO, ""),
                self.base_revid, merges)
            if new_mergeinfo is not None:
                self._svnprops[properties.PROP_MERGEINFO] = new_mergeinfo

    def _check_properties(self):
        """Verify that all revision properties are OK."""
        for name, value in self._revprops.items():
            if osutils.contains_whitespace(name):
                raise ValueError("invalid property name %r" % name)
            if not isinstance(value, text_type):
                raise ValueError("invalid property value %r for %r" %
                                 (value, name))

    @staticmethod
    def mutter(text, *args):
        if 'commit' in debug.debug_flags:
            trace.mutter(text, *args)

    def get_basis_delta(self):
        revid = self._get_actual_revision_id()
        return self._get_basis_delta(revid)

    def will_record_deletes(self):
        self._will_record_deletes = True

    def _generate_revision_if_needed(self, revid):
        """See CommitBuilder._generate_revision_if_needed()."""
        self._new_revision_id = revid
        self.random_revid = (self._new_revision_id is None)

    def finish_inventory(self):
        """See CommitBuilder.finish_inventory()."""
        pass

    def open_branch_editors(self, root, elements, base_url, base_rev,
                            root_from, root_action):
        """Open a specified directory given an editor for the repository root.

        :param root: Editor for the repository root
        :param elements: List of directory names to open
        :param base_url: URL to base top-level branch on
        :param base_rev: Revision of path to base top-level branch on
        :param root_from: Path inside the branch to copy the root from,
            or None if it should be created from scratch.
        :param root_action: tuple with action for the root and releted revnum.
        """
        ret = [root]

        self.mutter('opening branch %r (base %r:%r)',
                    elements, base_url, base_rev)

        # Open paths leading up to branch
        for i in range(0, len(elements)-1):
            try:
                ret.append(ret[-1].open_directory(
                    "/".join(elements[0:i+1]), -1))
            except SubversionException as e:
                if e.args[1] == ERR_FS_NOT_DIRECTORY:
                    raise MissingPrefix(
                        "/".join(elements), "/".join(elements[0:i]))
                else:
                    raise

        # Branch already exists and stayed at the same location, open:
        if root_action[0] == "open":
            ret.append(ret[-1].open_directory("/".join(elements), base_rev))
        elif root_action[0] in ("create", "replace"):
            # Branch has to be created
            # Already exists, old copy needs to be removed
            name = "/".join(elements)
            if root_action[0] == "replace":
                if name == "":
                    raise ChangesRootLHSHistory()
                self.mutter("removing branch dir %r", name)
                ret[-1].delete_entry(name, root_action[1])
            self.mutter("adding branch dir %r", name)
            if base_url is None or root_from is None:
                copyfrom_url = None
            else:
                copyfrom_url = urlutils.join(base_url, root_from)
            ret.append(ret[-1].add_directory(name, copyfrom_url, base_rev))
        else:
            raise AssertionError

        return ret

    def _get_parents_tuples(self):
        """Retrieve (tree, URL, base revnum) tuples for the parents of
        this commit.
        """
        ret = []
        for p in self.parents:
            if p == self.old_tree.get_revision_id():
                ret.append((self.old_tree, self.base_url, self.base_revnum))
                continue
            try:
                (uuid, base_path, base_revnum), base_mapping = \
                    self.repository.lookup_bzr_revision_id(
                        p, foreign_sibling=self.base_foreign_revid)
            except NoSuchRevision:
                continue
            tree = None
            if self._parent_trees is not None:
                for ptree in self._parent_trees:
                    if ptree.get_revision_id() == p:
                        tree = ptree
                        break
            if tree is None:
                tree = self.repository.revision_tree(p)
            ret.append((
                tree,
                urlutils.join(
                    self.repository.svn_transport.svn_url, base_path),
                base_revnum))
        return ret

    def _iter_new_children(self, file_id):
        # Iterate over the children that were added during the commit
        for child in self._updated_children[file_id]:
            (child_path, child_ie) = self._updated[child]
            yield child_ie

        try:
            old_path = self.old_tree.id2path(file_id)
        except NoSuchId:
            return

        # Iterate over the children that were present previously
        if self.old_tree.kind(old_path) == 'directory':
            for child_ie in self.old_tree.iter_child_entries(
                    old_path):
                if (child_ie.file_id not in self._deleted_fileids and
                        child_ie.file_id not in self._updated):
                    yield child_ie

    def _get_new_ie(self, file_id):
        if file_id in self._deleted_fileids:
            return None
        try:
            return self._updated[file_id][1]
        except KeyError:
            pass
        try:
            return self.old_tree.root_inventory.get_entry(file_id)
        except NoSuchId:
            return None

    def _get_author(self):
        try:
            return ",".join(self._revprops["authors"].split("\n")).encode("utf-8")
        except KeyError:
            try:
                return self._revprops["author"].encode("utf-8")
            except KeyError:
                return self._committer

    def _update_moved_dir_child_file_ids(self, path, file_id):
        """Update file id map for children of directories that were moved.

        This only considers children that were not in any other way
        modified in this commit.
        """
        try:
            from_path = self.old_tree.id2path(file_id)
        except NoSuchId:
            return
        if self.old_tree.kind(from_path) != 'directory':
            return
        for (p, v, k, ie) in self.old_tree.list_files(
                include_root=False, from_dir=from_path, recursive=True):
            assert v
            if ie.file_id in self._updated or ie.file_id in self._deleted_fileids:
                # File was already changed in some other way this commit
                # No need to set ie.file_id
                continue
            child_path = osutils.pathjoin(path, ie.name)
            old_child_path = osutils.pathjoin(from_path, ie.name)
            self._override_file_ids[child_path] = ie.file_id
            self._override_text_revisions[child_path] = (
                    self.old_tree.get_file_revision(old_child_path, ie.file_id))
            if ie.kind == 'directory':
                self._update_moved_dir_child_file_ids(child_path, ie.file_id)

    def _commit_done(self, *args):
        """Callback that is called by the Subversion commit editor
        once the commit finishes.

        :param revision_data: Revision metadata
        """
        self.revision_metadata = args

    @convert_svn_error
    def commit(self, message):
        """Finish the commit.

        """
        self._update_properties()
        self._check_properties()
        bp_parts = self.branch_path.split("/")

        self._changed_fileprops = {}

        if self.push_metadata:
            for (path, file_id) in self._touched_dirs:
                self._update_moved_dir_child_file_ids(path, file_id)
            if self.set_custom_revprops:
                self.mapping.export_text_revisions_revprops(
                    self._override_text_revisions, self._svn_revprops)
                self.mapping.export_fileid_map_revprops(
                    self._override_file_ids, self._svn_revprops)
                self.mapping.export_text_parents_revprops(
                    self._override_text_parents, self._svn_revprops)
            if self.set_custom_fileprops:
                self.mapping.export_text_revisions_fileprops(
                    self._override_text_revisions, self._svnprops)
                self.mapping.export_fileid_map_fileprops(
                    self._override_file_ids, self._svnprops)
                self.mapping.export_text_parents_fileprops(
                    self._override_text_parents, self._svnprops)
        if self._config_stack.get("log-strip-trailing-newline"):
            if self.set_custom_revprops:
                self.mapping.export_message_revprops(
                    message, self._svn_revprops)
            if self.set_custom_fileprops:
                self.mapping.export_message_fileprops(message, self._svnprops)
            message = message.rstrip("\n")
        self._svn_revprops[properties.PROP_REVISION_LOG] = message.encode("utf-8")

        with convert_svn_error(self.conn.get_commit_editor)(
                self._svn_revprops, self._commit_done) as editor:
            self.revision_metadata = None
            for prop in self._svn_revprops:
                assert prop.split(":")[0] in ("bzr", "svk", "svn")
                if not properties.is_valid_property_name(prop):
                    trace.warning(
                        "Setting property %r with invalid characters in name",
                        prop)
            if self.supports_custom_revprops:
                timeval = properties.time_to_cstring(1000000 * self._timestamp)
                self._svn_revprops[properties.PROP_REVISION_ORIGINAL_DATE] = timeval
            if (not self.supports_custom_revprops and
                    self._svn_revprops.keys() != [properties.PROP_REVISION_LOG]):
                raise AssertionError(
                    "non-log revision properties set but not supported: %r" %
                    self._svn_revprops.keys())

            try:
                root_from = self.old_tree.id2path(self.new_root_id)
            except NoSuchId:
                root_from = None

            if (self.old_tree.path2id('') is not None and
                    self.old_tree.path2id('') != self.new_root_id and
                    self.root_action[0] == "open"):
                self.root_action = ("replace", self.root_action[1])

            try:
                root = editor.open_root(self.base_revnum)
            except SubversionException as e:
                if e.args[1] == ERR_BAD_PROPERTY_VALUE:
                    raise ValueError(
                        "Invalid property contents: %r" % e.args[0])
                raise
            branch_editors = self.open_branch_editors(
                root, bp_parts, self.base_url, self.base_revnum, root_from,
                self.root_action)
            try:
                changed = dir_editor_send_changes(
                        (self.old_tree, self.base_url, self.base_revnum),
                        self._get_parents_tuples(),
                        (self._iter_new_children, self._get_new_ie), "",
                        self.new_root_id, branch_editors[-1],
                        self.branch_path, self.modified_files,
                        self._visit_dirs)

                if self.modified_files != {}:
                    raise AssertionError(
                        "some modified files not sent: %r" %
                        self.modified_files.keys())

                # Set all the revprops
                if self.push_metadata and self._svnprops.is_loaded:
                    for prop, newvalue in self._svnprops.items():
                        oldvalue = self._base_branch_props.get(prop)
                        if oldvalue == newvalue:
                            continue
                        self._changed_fileprops[prop] = (oldvalue, newvalue)

                if self._changed_fileprops == {}:
                    # Set dummy property, so Subversion will raise an error
                    # in case of clashes.
                    branch_editors[-1].change_prop(
                        DUMMY_ROOT_PROPERTY_NAME, None)

                for prop, (oldvalue, newvalue) in self._changed_fileprops.items():
                    if not properties.is_valid_property_name(prop):
                        trace.warning(
                            "Setting property %r with invalid characters "
                            "in name", prop)
                    self.mutter("Setting root file property %r -> %r",
                                prop, newvalue)
                    branch_editors[-1].change_prop(prop, newvalue)
            finally:
                for dir_editor in reversed(branch_editors):
                    dir_editor.close()

        self.repository.svn_transport.add_connection(self.conn)
        self.conn = None

        (result_revision, result_date, result_author) = self.revision_metadata
        self.result_foreign_revid = (
            self.repository.uuid, self.branch_path, result_revision)

        if result_author is not None:
            self._svn_revprops[properties.PROP_REVISION_AUTHOR] = result_author
        self._svn_revprops[properties.PROP_REVISION_DATE] = result_date

        self.mutter('commit %d finished. author: %r, date: %r',
                    result_revision, result_author, result_date)

        override_svn_revprops = self._config_stack.get('override-svn-revprops')
        if override_svn_revprops is not None:
            new_revprops = {}
            if (("%s=committer" % properties.PROP_REVISION_AUTHOR) in override_svn_revprops or
                    properties.PROP_REVISION_AUTHOR in override_svn_revprops):
                new_revprops[properties.PROP_REVISION_AUTHOR] = self._committer.encode(
                    "utf-8")
            if "%s=author" % properties.PROP_REVISION_AUTHOR in override_svn_revprops:
                new_revprops[properties.PROP_REVISION_AUTHOR] = self._get_author()
            if properties.PROP_REVISION_DATE in override_svn_revprops:
                new_revprops[properties.PROP_REVISION_DATE] = properties.time_to_cstring(1000000*self._timestamp)
            set_svn_revprops(self.repository, result_revision, new_revprops)
            self._svn_revprops.update(new_revprops)
        logcache = getattr(self.repository._log, "cache", None)
        if logcache is not None:
            logcache.insert_revprops(result_revision, self._svn_revprops, True)

        self.revmeta = self.repository._revmeta_provider.get_revision(
                self.branch_path, result_revision,
                None,  # FIXME: Generate changes dictionary
                revprops=self._svn_revprops,
                changed_fileprops=self._changed_fileprops,
                fileprops=self._svnprops,
                )
        revid = self.revmeta.get_revision_id(self.mapping)
        self.repository._cache_add_new_revision(
            result_revision, revid, tuple(self.parents))
        if self._branch is not None:
            self._branch._clear_cached_state()

        if self.push_metadata and self._new_revision_id not in (revid, None):
            raise AssertionError(
                "Unexpected revision id %s != %s" %
                (revid, self._new_revision_id))
        self.repository.commit_write_group()
        return revid

    def _get_actual_revision_id(self):
        if self._new_revision_id is not None:
            return self._new_revision_id
        elif self.revmeta is not None:
            return self.revmeta.get_revision_id(self.mapping)
        else:
            return None

    def _get_basis_delta(self, revid):
        """Create a basis delta with a specific revision for new entries.

        :param revid: Revision id to use for changes introduced
        :return: Invalitory delta
        """
        ret = []
        for (old_path, new_path, file_id, ie) in self._basis_delta:
            if ie is not None and ie.revision is None:
                ie = ie.copy()
                ie.revision = revid
            ret.append((old_path, new_path, file_id, ie))
        return ret

    def revision_tree(self):
        from breezy.bzr.inventory import mutable_inventory_from_tree
        from breezy.bzr.inventorytree import InventoryRevisionTree
        inv = mutable_inventory_from_tree(self.old_tree)
        revid = self._get_actual_revision_id()
        inv.apply_delta(self._get_basis_delta(revid))
        return InventoryRevisionTree(self.repository, inv, revid)

    def abort(self):
        if self.conn is not None:
            self.repository.svn_transport.add_connection(self.conn)
        if self.repository.is_in_write_group():
            self.repository.abort_write_group()

    def _visit_parent_dirs(self, path):
        """Add the parents of path to the list of paths to visit."""
        while path != "":
            if "/" in path:
                path, _ = path.rsplit("/", 1)
            else:
                path = ""
            if path in self._visit_dirs:
                return
            self._visit_dirs.add(path)

    def _get_text_revision(self, new_ie, new_path, parent_trees,
                           force_change=False):
        parent_text_revisions = []
        # Compare with the older versions of this file in the parent trees.
        # Separately track those entries that have the exact same properties,
        # as they are candidates for being carried over.
        carry_over_candidates = {}
        for ptree in parent_trees:
            try:
                ppath = ptree.id2path(new_ie.file_id)
            except NoSuchId:
                continue
            prevision = ptree.get_file_revision(ppath)
            if (ptree.path2id(osutils.dirname(ppath)) == new_ie.parent_id and
                osutils.basename(ppath) == new_ie.name and (
                 (new_ie.kind == 'file' and ptree.kind(ppath) == 'file' and
                  ptree.get_file_sha1(ppath) == new_ie.text_sha1 and
                  ptree.is_executable(ppath) == new_ie.executable) or
                 (new_ie.kind == 'symlink' and
                  ptree.kind(ppath) == 'symlink' and
                  ptree.get_symlink_target(ppath) == new_ie.symlink_target) or
                 (new_ie.kind == 'directory' and
                  ptree.kind(ppath) == 'directory'))):
                carry_over_candidates[prevision] = ptree
            parent_text_revisions.append(prevision)
        heads_set = self._heads(parent_text_revisions)
        heads = []
        # Preserve order
        for p in parent_text_revisions:
            if p in heads_set:
                heads.append(p)
                heads_set.remove(p)
        if force_change:
            return None, heads
        if len(heads) == 1 and heads[0] in carry_over_candidates:
            # carry over situation
            parent_tree = carry_over_candidates[heads[0]]
            parent_path = parent_tree.id2path(new_ie.file_id)
            return parent_tree.get_file_revision(parent_path), None
        if len(heads) == 0:
            text_parents = None
        else:
            try:
                parent_path = parent_trees[0].id2path(new_ie.file_id)
            except NoSuchId:
                text_parents = None
            else:
                if heads == [parent_trees[0].get_file_revision(parent_path)]:
                    # No need to explicitly store parent text ids
                    text_parents = None
                else:
                    text_parents = heads
        return None, text_parents

    def record_delete(self, path, file_id):
        raise NotImplementedError(self.record_delete)

    def _get_file_with_stat(self, tree, path):
        def dummy_get_file_with_stat(file_id):
            return tree.get_file(path), None
        return getattr(
            tree, "get_file_with_stat", dummy_get_file_with_stat)(path)

    def _record_change(
            self, tree, parent_trees,
            file_id, paths, new_kind, new_name, new_parent_id,
            new_executable, force_change=False):
        (old_path, new_path) = paths
        self._override_file_ids[new_path] = file_id
        new_ie = entry_factory[new_kind](file_id, new_name, new_parent_id)
        if new_kind == 'file':
            new_ie.executable = new_executable
            file_obj, stat_val = self._get_file_with_stat(tree, new_path)
            new_ie.text_size, new_ie.text_sha1 = osutils.size_sha_file(
                file_obj)
            (new_ie.revision, unusual_text_parents) = self._get_text_revision(
                new_ie, new_path, parent_trees, force_change=force_change)
            self.modified_files[file_id] = get_svn_file_delta_transmitter(
                tree, self.old_tree, old_path, new_path, new_ie)
            if new_ie.revision is None:
                yield new_path, new_ie.text_sha1
        elif new_kind == 'symlink':
            new_ie.symlink_target = tree.get_symlink_target(new_path)
            new_ie.revision, unusual_text_parents = self._get_text_revision(
                new_ie, new_path, parent_trees, force_change=force_change)
            self.modified_files[file_id] = get_svn_file_delta_transmitter(
                tree, self.old_tree, old_path, new_path, new_ie)
        elif new_kind == 'directory':
            (new_ie.revision, unusual_text_parents) = self._get_text_revision(
                new_ie, new_path, parent_trees, force_change=force_change)
            self._visit_dirs.add(new_path)
            self._touched_dirs.add((new_path, file_id))
        else:
            raise AssertionError("unknown kind %r" % new_kind)
        self._override_text_revisions[new_path] = new_ie.revision
        self._override_text_parents[new_path] = unusual_text_parents
        self._visit_parent_dirs(new_path)
        self._updated[file_id] = (new_path, new_ie)
        self._updated_children[new_parent_id].add(file_id)
        self._basis_delta.append((old_path, new_path, file_id, new_ie))
        if new_path == "":
            self.new_root_id = file_id

    def record_iter_changes(self, tree, basis_revision_id, iter_changes):
        """Record a new tree via iter_changes.

        :param tree: The tree to obtain text contents from for changed objects.
        :param basis_revision_id: The revision id of the tree the iter_changes
            has been generated against. Currently assumed to be the same
            as self.parents[0] - if it is not, errors may occur.
        :param iter_changes: An iter_changes iterator with the changes to apply
            to basis_revision_id. The iterator must not include any items with
            a current kind of None - missing items must be either filtered out
            or errored-on before record_iter_changes sees the item.
        :return: A generator of (file_id, relpath, fs_hash) tuples for use with
            tree._observed_sha1.
        """
        parent_trees = [self.old_tree]
        for p in self.parents[1:]:
            try:
                parent_trees.append(self.repository.revision_tree(p))
            except NoSuchRevision:
                self.mutter('missing parent tree %r', p)
        if self.base_revid != basis_revision_id:
            raise AssertionError(
                "Invalid basis revision %s != %s" %
                (self.base_revid, basis_revision_id))
        for change in iter_changes:
            if change.kind[1] is None:
                self._basis_delta.append(
                    (change.path[0], None, change.file_id, None))
                self._deleted_fileids.add(change.file_id)
            else:
                for data in self._record_change(
                        tree, parent_trees, change.file_id, change.path,
                        change.kind[1], change.name[1], change.parent_id[1],
                        change.executable[1]):
                    yield data
            # Old path parent needs to be visited in case file_id was removed
            # from it but there were no other changes there.
            if change.path[0] not in (None, change.path[1]):
                self._visit_parent_dirs(change.path[0])
            if change.path[1] != "" or basis_revision_id != NULL_REVISION:
                self._any_changes = True
        if self.new_root_id is None or self._rich_root_bump:
            # housekeeping root entry changes do not affect no-change commits.
            for data in self._require_root_change(tree, parent_trees):
                yield data
        if len(self.parents) > 1 and self.push_metadata:
            for data in self._record_last_modified_merges(tree, parent_trees):
                yield data

    def _record_last_modified_merges(self, tree, parent_trees):
        for file_id in parent_trees[0].all_file_ids():
            if file_id in self._updated:
                # Already updated, no need to do so again
                continue
            text_revisions = []
            for ptree in parent_trees:
                try:
                    text_revisions.append(ptree.get_file_revision(
                        ptree.id2path(file_id)))
                except NoSuchId:
                    continue
            heads = self._heads(text_revisions)
            if len(heads) != 1:
                self.mutter('last modified text merge for %r (%r)', file_id,
                            text_revisions)
                path = parent_trees[0].id2path(file_id)
                if "/" in path:
                    (parent, name) = path.rsplit("/", 1)
                    parent_id = parent_trees[0].path2id(parent)
                else:
                    parent_id = parent_trees[0].path2id("")
                    name = path
                for data in self._record_change(
                        tree, parent_trees, file_id, (path, path),
                        parent_trees[0].kind(path), name, parent_id,
                        parent_trees[0].is_executable(path, file_id),
                        force_change=True):
                    yield data

    def _require_root_change(self, tree, parent_trees):
        """Enforce an appropriate root object change.

        This is called once when record_iter_changes is called, if and only if
        the root was not in the delta calculated by record_iter_changes.

        :param tree: The tree which is being committed.
        """
        if len(self.parents) == 0 and not self._rich_root_bump:
            raise RootMissing()
        if len(self.parents) == 0:
            old_path = None
        else:
            old_path = ""
        for data in self._record_change(
                tree, parent_trees, tree.path2id(''), (old_path, ""),
                "directory", "", None, False,
                force_change=self._rich_root_bump):
            yield data

    def any_changes(self):
        return self._any_changes

    def record_entry_contents(self, ie, parent_invs, path, tree,
                              content_summary):
        """Record the content of ie from tree into the commit if needed.

        Side effect: sets ie.revision when unchanged

        :param ie: An inventory entry present in the commit.
        :param parent_invs: The inventories of the parent revisions of the
            commit.
        :param path: The path the entry is at in the tree.
        :param tree: The tree which contains this entry and should be used to
            obtain content.
        :param content_summary: Summary data from the tree about the paths
                content - stat, length, exec, sha/link target. This is only
                accessed when the entry has a revision of None - that is when
                it is a candidate to commit.
        """
        raise NotImplementedError(self.record_entry_contents)


def send_svn_file_text_delta(
        tree, old_tree, old_path, new_path, ie, editor):
    """Send the file text delta to a Subversion editor object.

    Tree can either be a native Subversion tree of some sort,
    in which case the optimized Subversion functions will be used,
    or another tree.

    :param tree: Tree
    :param old_tree: Base tree
    :param editor: Editor to report changes to
    """
    with mapping.get_svn_file_contents(tree, ie.kind, new_path) as f:
        file_editor_send_content_changes(f, editor)
    file_editor_send_prop_changes(old_tree, old_path, ie, editor)
    editor.close()


def get_svn_file_delta_transmitter(tree, old_tree, old_path, new_path, ie):
    try:
        transmit_svn_file_deltas = getattr(tree, "transmit_svn_file_deltas")
    except AttributeError:
        return partial(
            send_svn_file_text_delta, tree, old_tree, old_path, new_path, ie)
    else:
        return partial(transmit_svn_file_deltas, new_path)


def file_editor_send_prop_changes(old_tree, old_path, ie, editor):
    if old_path is None:
        old_special = False
        old_executable = False
    else:
        old_executable = old_tree.is_executable(old_path)
        old_special = (old_tree.kind(old_path) == 'symlink')

    if old_executable != ie.executable:
        if ie.executable:
            value = properties.PROP_EXECUTABLE_VALUE
        else:
            value = None
        editor.change_prop(properties.PROP_EXECUTABLE, value)

    if old_special != (ie.kind == 'symlink'):
        if ie.kind == 'symlink':
            value = properties.PROP_SPECIAL_VALUE
        else:
            value = None
        editor.change_prop(properties.PROP_SPECIAL, value)
