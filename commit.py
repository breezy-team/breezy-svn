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


"""Committing to Subversion repositories."""

try:
    from collections import defaultdict
except ImportError:
    from bzrlib.plugins.svn.pycompat import defaultdict

from cStringIO import (
    StringIO,
    )
from subvertpy import (
    delta,
    properties,
    SubversionException,
    ra,
    )

from bzrlib import (
    debug,
    osutils,
    urlutils,
    trace,
    ui,
    )
from bzrlib.errors import (
    BzrError,
    NoSuchId,
    NoSuchRevision,
    )
from bzrlib.inventory import (
    Inventory,
    entry_factory,
    )
from bzrlib.repository import (
    RootCommitBuilder,
    )
from bzrlib.revision import (
    NULL_REVISION,
    )

from bzrlib.plugins.svn import (
    mapping,
    )
from bzrlib.plugins.svn.errors import (
    convert_svn_error,
    AppendRevisionsOnlyViolation,
    ChangesRootLHSHistory,
    MissingPrefix,
    RevpropChangeFailed,
    )
from bzrlib.plugins.svn.svk import (
    generate_svk_feature,
    parse_svk_features,
    serialize_svk_features,
    SVN_PROP_SVK_MERGE
    )
from bzrlib.plugins.svn.transport import (
    check_dirs_exist,
    url_join_unescaped_path,
    )
from bzrlib.plugins.svn.util import (
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
    trace.warning("Storing Bazaar metadata in file properties. "
                  "Use `bzr dpush` for lossfull push without file properties."
                  "Upgrade the server to Subversion 1.5 or later to use "
                  "(less visible) revision properties.")


def _revision_id_to_svk_feature(revid, lookup_revid):
    """Create a SVK feature identifier from a revision id.

    :param revid: Revision id to convert.
    :return: Matching SVK feature identifier.
    """
    assert isinstance(revid, str)
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
    """Update a svn:mergeinfo property to include a specified list of merges."""
    pb = ui.ui_factory.nested_progress_bar()
    try:
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

                properties.mergeinfo_add_revision(mergeinfo, "/" + path,
                    revnum)
    finally:
        pb.finished()
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
    for (inv, url, revnum) in parents:
        if ie.file_id in inv and ie.kind == inv[ie.file_id].kind:
            candidates.append((inv, url, revnum))
    if candidates == []:
        return None
    # Check if there's any candidate that has the *exact* revision we need
    for (inv, url, revnum) in candidates:
        if inv[ie.file_id].revision == ie.revision:
            return (inv, url, revnum)
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
    for (name, value) in revprops.iteritems():
        try:
            repository.transport.change_rev_prop(revnum, name, value)
        except SubversionException, (_, ERR_REPOS_DISABLED_FEATURE):
            raise RevpropChangeFailed(name)


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
        digest = delta.send_stream(StringIO(contents), txdelta)
        from bzrlib.plugins.svn.fetch import md5_string
        assert digest == md5_string(contents)
    else:
        delta.send_stream(reader, txdelta)
    file_editor.close()


def path_join(basepath, name):
    if basepath:
        return urlutils.join(basepath, name)
    else:
        return name


def dir_editor_send_changes((base_inv, base_url, base_revnum), parents,
    (iter_children, get_ie), path, file_id, dir_editor, branch_path,
    modified_files, visit_dirs):
    """Pass the changes to a directory to the commit editor.

    :param path: Path (from repository root) to the directory.
    :param file_id: File id of the directory
    :param dir_editor: Subversion DirEditor object.
    :return: Boolean indicating whether any changes were made
    """
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
    # Loop over entries of file_id in base_inv
    # remove if they no longer exist with the same name
    # or parents
    if file_id in base_inv and base_inv[file_id].kind == 'directory':
        for child_name, child_ie in base_inv[file_id].children.iteritems():
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
                trace.mutter('removing %r(%r)', child_name, child_ie.file_id)
                dir_editor.delete_entry(
                    branch_relative_path(path, child_name.encode("utf-8")),
                    base_revnum)
                changed = True

    # Loop over file children of file_id in new_inv
    for child_name, child_ie in iter_children(file_id):
        assert child_ie is not None

        if not (child_ie.kind in ('file', 'symlink')):
            continue

        new_child_path = path_join(path, child_name.encode("utf-8"))
        full_new_child_path = branch_relative_path(new_child_path)
        # add them if they didn't exist in base_inv or changed kind
        if (not child_ie.file_id in base_inv or
            base_inv[child_ie.file_id].kind != child_ie.kind):
            trace.mutter('adding %s %r', child_ie.kind, new_child_path)

            # Do a copy operation if at all possible, to make
            # the log a bit easier to read for Subversion people
            new_base = find_suitable_base(parents, child_ie)
            if new_base is not None:
                child_base_path = new_base[0].id2path(child_ie.file_id).encode("utf-8")
                copyfrom_url = url_join_unescaped_path(new_base[1], child_base_path)
                copyfrom_revnum = new_base[2]
            else:
                # No suitable base found
                copyfrom_url = None
                copyfrom_revnum = -1

            child_editor = dir_editor.add_file(full_new_child_path,
                        copyfrom_url, copyfrom_revnum)
            changed = True
        # copy if they existed at different location
        elif (base_inv.id2path(child_ie.file_id).encode("utf-8") != new_child_path or
                base_inv[child_ie.file_id].parent_id != child_ie.parent_id):
            trace.mutter('copy %s %r -> %r', child_ie.kind,
                              base_inv.id2path(child_ie.file_id),
                              new_child_path)
            child_editor = dir_editor.add_file(full_new_child_path,
                url_join_unescaped_path(base_url,
                    base_inv.id2path(child_ie.file_id).encode("utf-8")),
                base_revnum)
            changed = True
        # open if they existed at the same location
        elif child_ie.file_id in modified_files:
            trace.mutter('open %s %r', child_ie.kind, new_child_path)
            child_editor = dir_editor.open_file(full_new_child_path, base_revnum)
        else:
            # Old copy of the file was retained. No need to send changes
            child_editor = None

        # handle the file
        if child_ie.file_id in modified_files:
            changed = True
            modified_files[child_ie.file_id](child_editor)
        elif child_editor is not None:
            child_editor.close()

    # Loop over subdirectories of file_id in new_inv
    for child_name, child_ie in iter_children(file_id):
        if child_ie.kind != 'directory':
            continue

        new_child_path = path_join(path, child_name.encode("utf-8"))
        # add them if they didn't exist in base_inv or changed kind
        if (not child_ie.file_id in base_inv or
            base_inv[child_ie.file_id].kind != child_ie.kind):
            trace.mutter('adding dir %r', child_ie.name)

            # Do a copy operation if at all possible, to make
            # the log a bit easier to read for Subversion people
            new_base = find_suitable_base(parents, child_ie)
            if new_base is not None:
                child_base = new_base
                child_base_path = new_base[0].id2path(child_ie.file_id).encode("utf-8")
                assert isinstance(new_base[1], str)
                copyfrom_url = url_join_unescaped_path(new_base[1], child_base_path)
                copyfrom_revnum = new_base[2]
            else:
                # No suitable base found
                child_base = (base_inv, base_url, base_revnum)
                copyfrom_url = None
                copyfrom_revnum = -1

            child_editor = dir_editor.add_directory(
                branch_relative_path(new_child_path), copyfrom_url, copyfrom_revnum)
            changed = True
        # copy if they existed at different location
        elif (base_inv.id2path(child_ie.file_id).encode("utf-8") != new_child_path or
              base_inv[child_ie.file_id].parent_id != child_ie.parent_id):
            old_child_path = base_inv.id2path(child_ie.file_id).encode("utf-8")
            trace.mutter('copy dir %r -> %r', old_child_path, new_child_path)
            copyfrom_url = url_join_unescaped_path(base_url, old_child_path)
            copyfrom_revnum = base_revnum

            child_editor = dir_editor.add_directory(
                branch_relative_path(new_child_path),
                copyfrom_url, copyfrom_revnum)
            changed = True
            child_base = (base_inv, base_url, base_revnum)
        # open if they existed at the same location and
        # the directory was touched
        elif new_child_path in visit_dirs:
            trace.mutter('open dir %r', new_child_path)

            child_editor = dir_editor.open_directory(
                    branch_relative_path(new_child_path),
                    base_revnum)

            child_base = (base_inv, base_url, base_revnum)
        else:
            continue

        # Handle this directory
        changed = dir_editor_send_changes(child_base, parents,
            (iter_children, get_ie), new_child_path, child_ie.file_id,
            child_editor, branch_path, modified_files, visit_dirs) or changed
        child_editor.close()

    return changed


class SvnCommitBuilder(RootCommitBuilder):
    """Commit Builder implementation wrapped around svn_delta_editor. """

    support_use_record_entry_contents = False

    def __init__(self, repository, branch_path, parents, config, timestamp,
                 timezone, committer, revprops, revision_id,
                 base_foreign_revid, base_mapping, old_inv=None,
                 push_metadata=True, graph=None, opt_signature=None,
                 texts=None, append_revisions_only=True,
                 override_svn_revprops=None, testament=None,
                 overwrite_revnum=None):
        """Instantiate a new SvnCommitBuilder.

        :param repository: SvnRepository to commit to.
        :param branch: branch path to commit to.
        :param parents: List of parent revision ids.
        :param config: Branch configuration to use.
        :param timestamp: Optional timestamp recorded for commit.
        :param timezone: Optional timezone for timestamp.
        :param committer: Optional committer to set for commit.
        :param revprops: Bazaar revision properties to set.
        :param revision_id: Revision id for the new revision.
        :param old_inv: Optional revision on top of which
            the commit is happening
        :param push_metadata: Whether or not to push all bazaar metadata
                              (in svn file properties, etc).
        :param graph: Optional graph object
        :param opt_signature: Optional signature to write.
        :param testament: A Testament object to store
        :param overwrite_revnum: Oldest revision number allowed to overwrite
        """
        super(SvnCommitBuilder, self).__init__(repository, parents,
            config, timestamp, timezone, committer, revprops, revision_id)
        self.new_inventory = None

        self.conn = self.repository.transport.get_connection()

        # revision ids are either specified or predictable
        self.random_revid = False
        self.branch_path = branch_path
        self.push_metadata = push_metadata
        self.overwrite_revnum = overwrite_revnum
        self._append_revisions_only = append_revisions_only
        self._texts = texts

        if override_svn_revprops is None:
            self._override_svn_revprops = self._config.get_override_svn_revprops()
        else:
            self._override_svn_revprops = override_svn_revprops

        # Gather information about revision on top of which the commit is
        # happening
        if parents:
            self.base_revid = parents[0]
        else:
            self.base_revid = NULL_REVISION

        if graph is None:
            graph = self.repository.get_graph()
        self.base_revno = graph.find_distance_to_null(self.base_revid, [])
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
            self.base_url = urlutils.join(self.repository.transport.svn_url,
                                          self.base_path)

        self.mapping = self.repository.get_mapping()
        # FIXME: Check that self.mapping >= self.base_mapping

        self._parent_invs = None
        if self.base_revid == NULL_REVISION:
            self.old_inv = Inventory(root_id=None)
        elif old_inv is None:
            self.old_inv = self.repository.get_inventory(self.base_revid)
        else:
            self.old_inv = old_inv

        if self.old_inv.root is None:
            self.new_root_id = None
        else:
            self.new_root_id = self.old_inv.root.file_id

        # Determine revisions merged in this one
        merges = filter(lambda x: x != self.base_revid, parents)

        self._deleted_fileids = set()
        self._updated_children = defaultdict(set)
        self._updated = {}
        self.visit_dirs = set()
        self.modified_files = {}
        self.supports_custom_revprops = self.repository.transport.has_capability("commit-revprops")
        if (self.supports_custom_revprops is None and
            self.mapping.can_use_revprops and
            self.repository.seen_bzr_revprops()):
            raise BzrError("Please upgrade your Subversion client libraries to 1.5 or higher to be able to commit with Subversion mapping %s (current version is %r)" % (self.mapping.name, getattr(ra, "api_version", ra.version)()))

        self._svn_revprops = {}
        self._svnprops = lazy_dict({}, dict,
            self._base_branch_props.iteritems())
        (self.set_custom_revprops, self.set_custom_fileprops) = self.repository._properties_to_set(self.mapping)
        if self.set_custom_fileprops:
            warn_setting_fileprops(self.repository.uuid)
        if self.supports_custom_revprops:
            # If possible, submit signature directly
            if opt_signature is not None:
                self._svn_revprops[mapping.SVN_REVPROP_BZR_SIGNATURE] = opt_signature
            # Set hint for potential clients that they have to check revision
            # properties
            if (not self.set_custom_fileprops and
                not self.repository.transport.has_capability("log-revprops")):
                # Tell clients about first approximate use of revision
                # properties
                self.mapping.export_revprop_redirect(
                    self.repository.get_latest_revnum()+1, self._svnprops)
        revno = self.base_revno + 1
        if self.set_custom_fileprops:
            self.mapping.export_revision_fileprops(self._svnprops,
                timestamp, timezone, committer, revprops,
                revision_id, revno, parents, testament=testament)
        if self.set_custom_revprops and self.push_metadata:
            self.mapping.export_revision_revprops(
                self._svn_revprops, self.repository.uuid,
                self.branch_path, timestamp, timezone, committer, revprops,
                revision_id, revno, parents, testament=testament)

        if len(merges) > 0:
            old_svk_merges = self._base_branch_props.get(SVN_PROP_SVK_MERGE, "")
            def lookup_revid(revid):
                return repository.lookup_bzr_revision_id(revid,
                    foreign_sibling=self.base_foreign_revid)
            new_svk_merges = update_svk_features(old_svk_merges, merges, lookup_revid)
            if new_svk_merges is not None:
                self._svnprops[SVN_PROP_SVK_MERGE] = new_svk_merges

            new_mergeinfo = update_mergeinfo(lookup_revid, graph,
                self._base_branch_props.get(properties.PROP_MERGEINFO, ""),
                self.base_revid, merges)
            if new_mergeinfo is not None:
                self._svnprops[properties.PROP_MERGEINFO] = new_mergeinfo

    @staticmethod
    def mutter(text, *args):
        if 'commit' in debug.debug_flags:
            trace.mutter(text, *args)

    def _generate_revision_if_needed(self):
        """See CommitBuilder._generate_revision_if_needed()."""

    def finish_inventory(self):
        """See CommitBuilder.finish_inventory()."""
        pass

    def open_branch_editors(self, root, elements, existing_elements,
                           base_url, base_rev, root_from, replace_existing):
        """Open a specified directory given an editor for the repository root.

        :param root: Editor for the repository root
        :param elements: List of directory names to open
        :param existing_elements: List of directory names that exist
        :param base_url: URL to base top-level branch on
        :param base_rev: Revision of path to base top-level branch on
        :param root_from: Path inside the branch to copy the root from,
            or None if it should be created from scratch.
        :param replace_existing: Whether the current branch should be replaced
        """
        ret = [root]

        self.mutter('opening branch %r (base %r:%r)', elements, base_url, base_rev)

        # Open paths leading up to branch
        for i in range(0, len(elements)-1):
            # Does directory already exist?
            ret.append(ret[-1].open_directory(
                "/".join(existing_elements[0:i+1]), -1))

        if (len(existing_elements) != len(elements) and
            len(existing_elements)+1 != len(elements)):
            raise MissingPrefix("/".join(elements), "/".join(existing_elements))

        replace_existing |= (root_from is not None and root_from != "")

        # Branch already exists and stayed at the same location, open:
        # TODO: What if the branch didn't change but the new revision
        # was based on an older revision of the branch?
        # This needs to also check that base_rev was the latest version of
        # branch_path.
        if len(existing_elements) == len(elements) and not replace_existing:
            ret.append(ret[-1].open_directory("/".join(elements), base_rev))
        else: # Branch has to be created
            # Already exists, old copy needs to be removed
            name = "/".join(elements)
            if replace_existing:
                if name == "":
                    raise ChangesRootLHSHistory()
                self.mutter("removing branch dir %r", name)
                ret[-1].delete_entry(name, max(base_rev, self.overwrite_revnum))
            self.mutter("adding branch dir %r", name)
            if base_url is None or root_from is None:
                copyfrom_url = None
            else:
                copyfrom_url = urlutils.join(base_url, root_from)
            ret.append(ret[-1].add_directory(name, copyfrom_url, base_rev))

        return ret

    def _determine_texts_identity(self, new_root_id):
        # Store file ids
        def _dir_process_file_id(path, file_id):
            ret = []
            for child_name, child_ie in self._iter_new_children(file_id):
                new_child_path = path_join(path, child_name)
                if (not child_ie.file_id in self.old_inv or
                    self.old_inv.id2path(child_ie.file_id) != new_child_path or
                    self.old_inv[child_ie.file_id].revision != child_ie.revision or
                    self.old_inv[child_ie.file_id].parent_id != child_ie.parent_id):
                    ret.append((child_ie.file_id, new_child_path, child_ie.revision))

                if (child_ie.kind == 'directory' and new_child_path in self.visit_dirs):
                    ret += _dir_process_file_id(new_child_path, child_ie.file_id)
            return ret

        fileids = {}
        text_revisions = {}
        changes = []

        if (self.old_inv.root is None or new_root_id != self.old_inv.root.file_id):
            changes.append((new_root_id, "", self._get_new_ie(new_root_id).revision))

        changes += _dir_process_file_id("", new_root_id)

        # Find the "unusual" text revisions
        for id, path, revid in changes:
            fileids[path] = id
            if revid is not None and revid != self.base_revid and revid != self._new_revision_id:
                text_revisions[path] = revid
        return (fileids, text_revisions)

    def _get_parents_tuples(self):
        """Retrieve (inventory, URL, base revnum) tuples for the parents of
        this commit.
        """
        ret = []
        for p in self.parents:
            if p == self.old_inv.revision_id:
                ret.append((self.old_inv, self.base_url, self.base_revnum))
                continue
            try:
                (uuid, base_path, base_revnum), base_mapping = \
                    self.repository.lookup_bzr_revision_id(p,
                            foreign_sibling=self.base_foreign_revid)
            except NoSuchRevision:
                continue
            inv = None
            if self._parent_invs is not None:
                for pinv in self._parent_invs:
                    if pinv.revision_id == p:
                        inv = pinv
                        break
            if inv is None:
                inv = self.repository.get_inventory(p)
            ret.append((inv, urlutils.join(self.repository.transport.svn_url, base_path), base_revnum))
        return ret

    def _iter_new_children(self, file_id):
        ret = []
        for child in self._updated_children[file_id]:
            ret.append((self._updated[child].name, self._updated[child]))
        try:
            old_ie = self.old_inv[file_id]
        except NoSuchId:
            pass
        else:
            if old_ie.kind == 'directory':
                for name, child_ie in old_ie.children.iteritems():
                    if not child_ie.file_id in self._deleted_fileids and not child_ie.file_id in self._updated:
                        ret.append((name, child_ie))
        return ret

    def _get_new_ie(self, file_id):
        if file_id in self._deleted_fileids:
            return None
        if file_id in self._updated:
            return self._updated[file_id]
        if file_id in self.old_inv:
            return self.old_inv[file_id]
        return None

    def _get_author(self):
        try:
            return ",".join(self._revprops["authors"].split("\n")).encode("utf-8")
        except KeyError:
            try:
                return self._revprops["author"].encode("utf-8")
            except KeyError:
                return self._committer

    @convert_svn_error
    def commit(self, message):
        """Finish the commit.

        """
        bp_parts = self.branch_path.split("/")
        lock = self.repository.transport.lock_write(".")

        self._changed_fileprops = {}

        if self.push_metadata:
            (fileids, text_revisions) = self._determine_texts_identity(self.new_root_id)
            if self.set_custom_revprops:
                self.mapping.export_text_revisions_revprops(text_revisions,
                        self._svn_revprops)
                self.mapping.export_fileid_map_revprops(fileids,
                        self._svn_revprops)
            if self.set_custom_fileprops:
                self.mapping.export_text_revisions_fileprops(text_revisions,
                        self._svnprops)
                self.mapping.export_fileid_map_fileprops(fileids, self._svnprops)
        if self._config.get_log_strip_trailing_newline():
            if self.push_metadata:
                if self.set_custom_revprops:
                    self.mapping.export_message_revprops(message, self._svn_revprops)
                if self.set_custom_fileprops:
                    self.mapping.export_message_fileprops(message, self._svnprops)
            message = message.rstrip("\n")
        self._svn_revprops[properties.PROP_REVISION_LOG] = message.encode("utf-8")

        def done(*args):
            """Callback that is called by the Subversion commit editor
            once the commit finishes.

            :param revision_data: Revision metadata
            """
            self.revision_metadata = args

        conn = self.conn
        repository_latest_revnum = conn.get_latest_revnum()
        self.editor = convert_svn_error(conn.get_commit_editor)(
                self._svn_revprops, done)
        try:
            # Shortcut - no need to see if dir exists if our base
            # was the last revision in the repo. This situation
            # happens a lot when pushing multiple subsequent revisions.
            if (self.base_revnum == self.repository.get_latest_revnum() and
                self.base_path == self.branch_path):
                existing_bp_parts = bp_parts
            else:
                existing_bp_parts = check_dirs_exist(self.repository.transport,
                                              bp_parts, -1)
            self.revision_metadata = None
            for prop in self._svn_revprops:
                assert prop.split(":")[0] in ("bzr", "svk", "svn")
                if not properties.is_valid_property_name(prop):
                    trace.warning("Setting property %r with invalid characters in name", prop)
            if self.supports_custom_revprops:
                self._svn_revprops[properties.PROP_REVISION_ORIGINAL_DATE] = properties.time_to_cstring(1000000*self._timestamp)
            assert self.supports_custom_revprops or self._svn_revprops.keys() == [properties.PROP_REVISION_LOG], \
                    "revprops: %r" % self._svn_revprops.keys()
            replace_existing = False
            # See whether the base of the commit matches the lhs parent
            # if not, we need to replace the existing directory
            if len(bp_parts) == len(existing_bp_parts):
                if (self.base_path is None or
                    self.base_path.strip("/") != "/".join(bp_parts).strip("/")):
                    replace_existing = True
                    if self._append_revisions_only:
                        raise AppendRevisionsOnlyViolation(
                            urlutils.join(self.repository.base, self.branch_path))
                elif self.base_revnum < self.repository._log.find_latest_change(self.branch_path, repository_latest_revnum):
                    replace_existing = True
                    if self._append_revisions_only:
                        raise AppendRevisionsOnlyViolation(
                            urlutils.join(self.repository.base, self.branch_path))
                elif self.old_inv.root.file_id != self.new_root_id:
                    replace_existing = True

            if self.new_root_id in self.old_inv:
                root_from = self.old_inv.id2path(self.new_root_id)
            else:
                root_from = None

            try:
                root = self.editor.open_root(self.base_revnum)
                branch_editors = self.open_branch_editors(root, bp_parts,
                    existing_bp_parts, self.base_url, self.base_revnum,
                    root_from, replace_existing)

                changed = dir_editor_send_changes(
                        (self.old_inv, self.base_url, self.base_revnum),
                        self._get_parents_tuples(),
                        (self._iter_new_children, self._get_new_ie), "",
                        self.new_root_id, branch_editors[-1],
                        self.branch_path, self.modified_files, self.visit_dirs)

                # Set all the revprops
                if self.push_metadata and self._svnprops.is_loaded:
                    for prop, newvalue in self._svnprops.iteritems():
                        oldvalue = self._base_branch_props.get(prop)
                        if oldvalue == newvalue:
                            continue
                        self._changed_fileprops[prop] = (oldvalue, newvalue)

                if self._changed_fileprops == {}:
                    # Set dummy property, so Subversion will raise an error in case of
                    # clashes.
                    branch_editors[-1].change_prop(DUMMY_ROOT_PROPERTY_NAME, None)

                for prop, (oldvalue, newvalue) in self._changed_fileprops.iteritems():
                        if not properties.is_valid_property_name(prop):
                            trace.warning("Setting property %r with invalid characters in name", prop)
                        assert isinstance(newvalue, str)
                        self.mutter("Setting root file property %r -> %r", prop, newvalue)
                        branch_editors[-1].change_prop(prop, newvalue)

                for dir_editor in reversed(branch_editors):
                    dir_editor.close()
            except:
                self.editor.abort()
                self.repository.transport.add_connection(conn)
                raise

            self.editor.close()
            self.repository.transport.add_connection(conn)
        finally:
            lock.unlock()

        (result_revision, result_date, result_author) = self.revision_metadata
        self.result_foreign_revid = (self.repository.uuid, self.branch_path,
                result_revision)

        if result_author is not None:
            self._svn_revprops[properties.PROP_REVISION_AUTHOR] = result_author
        self._svn_revprops[properties.PROP_REVISION_DATE] = result_date

        self.repository._clear_cached_state(result_revision)

        self.mutter('commit %d finished. author: %r, date: %r',
               result_revision, result_author, result_date)

        if self._override_svn_revprops is not None:
            new_revprops = {}
            if (("%s=committer" % properties.PROP_REVISION_AUTHOR) in self._override_svn_revprops or
                properties.PROP_REVISION_AUTHOR in self._override_svn_revprops):
                new_revprops[properties.PROP_REVISION_AUTHOR] = self._committer.encode("utf-8")
            if "%s=author" % properties.PROP_REVISION_AUTHOR in self._override_svn_revprops:
                new_revprops[properties.PROP_REVISION_AUTHOR] = self._get_author()
            if properties.PROP_REVISION_DATE in self._override_svn_revprops:
                new_revprops[properties.PROP_REVISION_DATE] = properties.time_to_cstring(1000000*self._timestamp)
            set_svn_revprops(self.repository, result_revision, new_revprops)
            self._svn_revprops.update(new_revprops)
        logcache = getattr(self.repository._log, "cache", None)
        if logcache is not None:
            logcache.insert_revprops(result_revision, self._svn_revprops, True)

        self.revmeta = self.repository._revmeta_provider.get_revision(
                self.branch_path, result_revision,
                None, # FIXME: Generate changes dictionary
                revprops=self._svn_revprops,
                changed_fileprops=self._changed_fileprops,
                fileprops=self._svnprops,
                )
        revid = self.revmeta.get_revision_id(self.mapping)

        if self.push_metadata and self._new_revision_id not in (revid, None):
            raise AssertionError("Unexpected revision id %s != %s" %
                    (revid, self._new_revision_id))
        return revid

    def _visit_parent_dirs(self, path):
        """Add the parents of path to the list of paths to visit."""
        while path != "":
            if "/" in path:
                path, _ = path.rsplit("/", 1)
            else:
                path = ""
            if path in self.visit_dirs:
                return
            self.visit_dirs.add(path)

    def _get_text_revision(self, file_id, text_sha1, parent_invs):
        for pinv in parent_invs:
            if file_id in pinv and pinv[file_id].text_sha1 == text_sha1:
                return pinv[file_id].revision
        return None

    def record_delete(self, path, file_id):
        raise NotImplementedError(self.record_delete)

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
        parent_invs = [self.old_inv]
        for p in self.parents[1:]:
            try:
                parent_invs.append(self.repository.get_inventory(p))
            except NoSuchRevision:
                pass
        if self.base_revid != basis_revision_id:
            raise AssertionError("Invalid basis revision %s != %s" %
                (self.base_revid, basis_revision_id))
        def dummy_get_file_with_stat(file_id):
            return tree.get_file(file_id), None
        get_file_with_stat = getattr(tree, "get_file_with_stat",
                dummy_get_file_with_stat)
        for (file_id, (old_path, new_path), changed_content,
             (old_ver, new_ver), (old_parent_id, new_parent_id),
             (old_name, new_name), (old_kind, new_kind),
             (old_executable, new_executable)) in iter_changes:
            if file_id in self.old_inv:
                base_ie = self.old_inv[file_id]
            else:
                base_ie = None
            if new_path is None:
                self._basis_delta.append((old_path, None, file_id, None))
                self._deleted_fileids.add(file_id)
            else:
                new_ie = entry_factory[new_kind](file_id, new_name, new_parent_id)
                if new_kind == 'file':
                    new_ie.executable = new_executable
                    file_obj, stat_val = get_file_with_stat(file_id)
                    new_ie.text_size, new_ie.text_sha1 = osutils.size_sha_file(file_obj)
                    new_ie.revision = self._get_text_revision(file_id, new_ie.text_sha1, parent_invs)
                    self.modified_files[file_id] = get_svn_file_delta_transmitter(tree, base_ie, new_ie)
                    yield file_id, new_path, (new_ie.text_sha1, stat_val)
                elif new_kind == 'symlink':
                    new_ie.symlink_target = tree.get_symlink_target(file_id)
                    self.modified_files[file_id] = get_svn_file_delta_transmitter(tree, base_ie, new_ie)
                elif new_kind == 'directory':
                    self.visit_dirs.add(new_path)
                self._visit_parent_dirs(new_path)
                self._updated[file_id] = new_ie
                self._updated_children[new_parent_id].add(file_id)
                self._basis_delta.append((old_path, new_path, file_id, new_ie))
                if new_path == "":
                    self.new_root_id = file_id
            # Old path parent needs to be visited in case file_id was removed
            # from it but there were no other changes there.
            if old_path not in (None, new_path):
                self._visit_parent_dirs(old_path)
            self._any_changes = True

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



def send_svn_file_text_delta(tree, base_ie, ie, editor):
    contents = mapping.get_svn_file_contents(tree, ie.kind, ie.file_id)
    try:
        file_editor_send_content_changes(contents, editor)
        file_editor_send_prop_changes(base_ie, ie, editor)
    finally:
        contents.close()


def get_svn_file_delta_transmitter(tree, base_ie, ie):
    try:
        transmit_svn_file_deltas = getattr(tree, "transmit_svn_file_deltas")
    except AttributeError:
        return lambda editor: send_svn_file_text_delta(tree, base_ie, ie, editor)
    else:
        return lambda editor: transmit_svn_file_deltas(ie.file_id, editor)


def file_editor_send_prop_changes(base_ie, ie, editor):
    if base_ie:
        old_executable = base_ie.executable
        old_special = (base_ie.kind == 'symlink')
    else:
        old_special = False
        old_executable = False

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
