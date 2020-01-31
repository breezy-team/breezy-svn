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
"""Access to stored Subversion basis trees."""

from __future__ import absolute_import

from io import BytesIO
import os
import subvertpy
from subvertpy import (
    delta,
    properties,
    wc,
    )
import urllib

from breezy import (
    errors,
    rules,
    osutils,
    urlutils,
    )
from breezy.tree import (
    TreeDirectory,
    TreeFile,
    TreeLink,
    )
from breezy.branch import Branch
from breezy.bzr.inventory import (
    Inventory,
    InventoryDirectory,
    TreeReference,
    )
from breezy.bzr.inventorytree import find_ids_across_trees
from breezy.osutils import (
    md5,
    )
from breezy.revision import (
    CURRENT_REVISION,
    )
from breezy.revisiontree import (
    RevisionTree,
    )
from breezy.sixish import (
    text_type,
    )
from breezy.trace import mutter


class BasisTreeIncomplete(errors.BzrError):
    _fmt = "The Subversion basis tree can not be retrieved because it is incomplete."""
    internal_error = True


class SubversionTreeDirectory(TreeDirectory):
    """Describes a directory in a subversion tree."""

    kind = 'directory'

    def __init__(self, file_id, name, parent_id):
        self.file_id = file_id
        self.name = name
        self.parent_id = parent_id
        self.executable = False


class SubversionTreeLink(TreeLink):
    """Describes a link in a subversion tree."""

    kind = 'symlink'

    def __init__(self, file_id, name, parent_id):
        self.file_id = file_id
        self.name = name
        self.parent_id = parent_id
        self.symlink_target = None
        self.executable = False
        self.text_size = None
        self.text_sha1 = None


class SubversionTreeFile(TreeFile):
    """Describes a file in a subversion tree."""

    kind = 'file'

    def __init__(self, file_id, name, parent_id):
        self.file_id = file_id
        self.name = name
        self.parent_id = parent_id
        self.text_sha1 = None
        self.text_size = None
        self.text_id = None
        self.executable = False


class SubversionTree(object):

    def get_file_properties(self, path):
        raise NotImplementedError(self.get_file_properties)

    def supports_content_filtering(self):
        return True

    def lookup_id(self, path):
        """Lookup the file id and text revision for a path.

        :param path: Unicode path
        :raises KeyError: raised if path was not found
        :return: tuple with file id and text revision
        """
        assert isinstance(path, text_type) or isinstance(path, bytes), "was: %r" % path
        return self.id_map.lookup(self.mapping, path)[:2]

    def lookup_path(self, file_id):
        return self.id_map.reverse_lookup(self.mapping, file_id)

    def _get_rules_searcher(self, default_searcher):
        """Get the RulesSearcher for this tree given the default one."""
        return rules._StackedRulesSearcher(
            [SvnRulesSearcher(self), default_searcher])

    def paths2ids(self, paths, trees=[], require_versioned=True):
        """Return all the ids that can be reached by walking from paths.

        Each path is looked up in this tree and any extras provided in
        trees, and this is repeated recursively: the children in an extra tree
        of a directory that has been renamed under a provided path in this tree
        are all returned, even if none exist under a provided path in this
        tree, and vice versa.

        :param paths: An iterable of paths to start converting to ids from.
            Alternatively, if paths is None, no ids should be calculated and None
            will be returned. This is offered to make calling the api unconditional
            for code that *might* take a list of files.
        :param trees: Additional trees to consider.
        :param require_versioned: If False, do not raise NotVersionedError if
            an element of paths is not versioned in this tree and all of trees.
        """
        return find_ids_across_trees(paths, [self] + list(trees), require_versioned)


class SvnRevisionTreeCommon(SubversionTree,RevisionTree):

    def has_or_had_id(self, file_id):
        return self.has_id(file_id)

    def id2path(self, file_id):
        try:
            return self.lookup_path(file_id)
        except KeyError:
            raise errors.NoSuchId(self, file_id)

    def has_id(self, file_id):
        try:
            self.id2path(file_id)
        except errors.NoSuchId:
            return False
        else:
            return True

    def path2id(self, path):
        try:
            file_id, revision_id = self.lookup_id(path)
        except KeyError:
            return None
        else:
            return file_id

    def get_root_id(self):
        return self.path2id("")

    def _comparison_data(self, entry, path):
        # FIXME
        if entry is None:
            return None, False, None
        return entry.kind, entry.executable, None

    def get_file_mtime(self, path):
        """See Tree.get_file_mtime."""
        revid = self.get_file_revision(path)
        try:
            rev = self._repository.get_revision(revid)
        except errors.NoSuchRevision:
            raise errors.FileTimestampUnavailable(path)
        return rev.timestamp

    def is_executable(self, path):
        props = self.get_file_properties(path)
        if props.has_key(properties.PROP_SPECIAL):
            text = self.get_file_stream_by_path(path)
            if text.read(5) == "link ":
                return False
        return props.has_key(properties.PROP_EXECUTABLE)

    def get_symlink_target(self, path):
        props = self.get_file_properties(path)
        if not props.has_key(properties.PROP_SPECIAL):
            return None
        text = self.get_file_stream_by_path(path).read()
        if not text.startswith("link "):
            return None
        return text[len("link "):].decode("utf-8")

    def get_file_sha1(self, path, stat_value=None):
        return osutils.sha_string(self.get_file_text(path))

    def get_file_revision(self, path):
        file_id, file_revision = self.lookup_id(path)
        return file_revision

    def iter_files_bytes(self, desired_files):
        for path, identifier in desired_files:
            cur_file = (self.get_file_text(path),)
            yield identifier, cur_file

    def get_file_stream_by_path(self, path):
        raise NotImplementedError(self.get_file_stream_by_path)

    def iter_child_entries(self, path):
        entry = self.iter_entries_by_dir(specific_files=[path]).next()[1]
        return iter(getattr(entry, 'children', {}).values())

    def find_related_paths_across_trees(self, paths, trees=[],
            require_versioned=True):
        """Find related paths in tree corresponding to specified filenames in any
        of `lookup_trees`.

        All matches in all trees will be used, and all children of matched
        directories will be used.

        :param paths: The filenames to find related paths for (if None, returns
            None)
        :param trees: The trees to find paths within
        :param require_versioned: if true, all specified filenames must occur in
            at least one tree.
        :return: a set of paths for the specified filenames and their children
            in `tree`
        """
        # TODO(jelmer): Implement this without file ids
        if paths is None:
            return None
        file_ids = self.paths2ids(
                paths, trees, require_versioned=require_versioned)
        ret = set()
        for file_id in file_ids:
            try:
                ret.add(self.id2path(file_id))
            except errors.NoSuchId:
                pass
        return ret


# This maps SVN names for eol-styles to bzr names:
eol_style = {
    "native": "native",
    "CRLF": "crlf",
    "LF": "lf",
    "CR": "cr"
    }

class SvnRulesSearcher(object):

    def __init__(self, tree):
        self.tree = tree

    def _map_property(self, name, value):
        if name == "svn:eol-style":
            if value in eol_style:
                return ("eol", eol_style[value])
            mutter("Unknown svn:eol-style setting '%r'", value)
            return None
        elif name == "svn:keywords":
            return ("svn-keywords", value)
        else:
            # Unknown or boring setting
            return None

    def get_items(self, path):
        for k, v in self.tree.get_file_properties(path).items():
            prop = self._map_property(k, v)
            if prop is not None:
                yield prop

    def get_selected_items(self, path, names):
        return tuple([(k, v) for (k, v) in self.get_items(path) if k in names])


def inventory_add_external(inv, parent_id, path, revid, ref_revnum, url):
    """Add an svn:externals entry to an inventory as a tree-reference.

    :param inv: Inventory to add to.
    :param parent_id: File id of directory the entry was set on.
    :param path: Path of the entry, relative to entry with parent_id.
    :param revid: Revision to store in newly created inventory entries.
    :param ref_revnum: Referenced *svn* revision of tree that's being referenced, or
        None if no specific revision is being referenced.
    :param url: URL of referenced tree.
    """
    assert ref_revnum is None or isinstance(ref_revnum, int)
    assert revid is None or isinstance(revid, str)
    (dir, name) = os.path.split(path)
    parent = inv.get_entry(parent_id)
    if dir != "":
        for part in dir.split("/"):
            if parent.children.has_key(part):
                parent = parent.children[part]
            else:
                # Implicitly add directory if it doesn't exist yet
                # TODO: Generate a file id
                parent = inv.add(InventoryDirectory('someid', part,
                                 parent_id=parent.file_id))
                parent.revision = revid

    # FIXME: This can only be a svn branch, so use that fact.
    reference_branch = Branch.open(url)
    file_id = reference_branch.get_root_id()
    ie = TreeReference(file_id, name, parent.file_id, revision=revid)
    if ref_revnum is not None:
        ie.reference_revision = reference_branch.generate_revision_id(ref_revnum)
    else:
        ie.reference_revision = CURRENT_REVISION
    inv.add(ie)


class SvnRevisionTree(SvnRevisionTreeCommon):
    """A tree that existed in a historical Subversion revision."""

    def __init__(self, repository, revision_id):
        self._repository = repository
        self._revision_id = revision_id
        self._revmeta, self.mapping = repository._get_revmeta(revision_id)
        self._bzr_inventory = None
        self._rules_searcher = None
        self.id_map = repository.get_fileid_map(self._revmeta, self.mapping)
        self.file_properties = {}
        self.transport = repository.svn_transport.clone(
            self._revmeta.metarev.branch_path)

    @property
    def root_inventory(self):
        # FIXME
        if self._bzr_inventory is not None:
            return self._bzr_inventory
        self._bzr_inventory = Inventory()
        self._bzr_inventory.revision_id = self.get_revision_id()
        root_repos = self._repository.svn_transport.get_svn_repos_root()
        editor = TreeBuildEditor(self)
        conn = self._repository.svn_transport.get_connection()
        try:
            reporter = conn.do_switch(
                self._revmeta.metarev.revnum, "", True,
                urlutils.join(root_repos, self._revmeta.metarev.branch_path).rstrip("/"), editor)
            try:
                reporter.set_path("", 0, True, None)
                reporter.finish()
            except:
                reporter.abort()
                raise
        finally:
            self._repository.svn_transport.add_connection(conn)
        return self._bzr_inventory

    def all_file_ids(self):
        return set(self.root_inventory.iter_all_ids())

    def all_versioned_paths(self):
        return set(p for p, e in self.iter_entries_by_dir())

    def iter_entries_by_dir(self, specific_files=None):
        if specific_files is not None:
            specific_file_ids = []
            for path in specific_files:
                file_id = self.path2id(path)
                if file_id is not None:
                    specific_file_ids.append(file_id)
        else:
            specific_file_ids = None

        return self.root_inventory.iter_entries_by_dir(
            specific_file_ids=specific_file_ids)

    def kind(self, path):
        stream = BytesIO()
        try:
            (fetched_rev, props) = self.transport.get_file(path,
                    stream, self._revmeta.metarev.revnum)
        except subvertpy.SubversionException as e:
            msg, num = e.args
            if num == subvertpy.ERR_FS_NOT_FILE:
                return "directory"
            raise
        stream.seek(0)
        if props.has_key(properties.PROP_SPECIAL) and stream.read(5) == "link ":
            return "symlink"
        return "file"

    def get_file_size(self, path):
        file_id = self.path2id(path)
        # FIXME: More efficient implementation?
        return self.root_inventory.get_entry(file_id).text_size

    def list_files(self, include_root=False, from_dir=None, recursive=True):
        # FIXME
        # The only files returned by this are those from the version
        inv = self.root_inventory
        if from_dir is None:
            from_dir_id = None
        else:
            from_dir_id = inv.path2id(from_dir)
            if from_dir_id is None:
                # Directory not versioned
                return
        entries = inv.iter_entries(from_dir=from_dir_id, recursive=recursive)
        if inv.root is not None and not include_root and from_dir is None:
            # skip the root for compatability with the current apis.
            entries.next()
        for path, entry in entries:
            yield path, 'V', entry.kind, entry.file_id, entry

    def get_file(self, path):
        stream = BytesIO()
        (fetched_rev, props) = self.transport.get_file(path,
                stream, self._revmeta.metarev.revnum)
        if properties.PROP_SPECIAL in props and stream.read(5) == b"link ":
            return BytesIO()
        stream.seek(0)
        return stream

    def get_file_stream_by_path(self, path):
        stream = BytesIO()
        (fetched_rev, props) = self.transport.get_file(path,
                stream, self._revmeta.metarev.revnum)
        stream.seek(0)
        return stream

    def get_file_text(self, path):
        with self.get_file(path) as my_file:
            return my_file.read()

    def get_file_properties(self, path):
        try:
            (fetched_rev, props) = self.transport.get_file(path,
                    BytesIO(), self._revmeta.metarev.revnum)
        except subvertpy.SubversionException as e:
            msg, num = e.args
            if num == subvertpy.ERR_FS_NOT_FILE:
                (dirents, fetched_rev, props) = self.transport.get_dir(
                    path, self._revmeta.metarev.revnum)
            else:
                raise
        return props

    def has_filename(self, path):
        kind = self.transport.check_path(path, self._revmeta.metarev.revnum)
        return kind in (subvertpy.NODE_DIR, subvertpy.NODE_FILE)

    def __repr__(self):
        return "<%s for %r, revision_id=%s>" % (self.__class__.__name__,
            self._revmeta, self.get_revision_id())


class TreeBuildEditor(object):
    """Builds a tree given Subversion tree transform calls."""

    def __init__(self, tree):
        self.tree = tree
        self.repository = tree._repository
        self.last_revnum = {}

    def set_target_revision(self, revnum):
        self.revnum = revnum

    def open_root(self, revnum):
        file_id, revision_id = self.tree.lookup_id("")
        ie = self.tree._bzr_inventory.add_path("", 'directory', file_id)
        ie.revision = revision_id
        return DirectoryTreeEditor(self.tree, file_id)

    def close(self):
        pass

    def abort(self):
        pass


class DirectoryTreeEditor(object):

    def __init__(self, tree, file_id):
        self.tree = tree
        self.file_id = file_id
        self.tree.file_properties[file_id] = {}

    def add_directory(self, path, copyfrom_path=None, copyfrom_revnum=-1):
        path = path.decode("utf-8")
        file_id, revision_id = self.tree.lookup_id(path)
        ie = self.tree._bzr_inventory.add_path(path, 'directory', file_id)
        ie.revision = revision_id
        return DirectoryTreeEditor(self.tree, file_id)

    def change_prop(self, name, value):
        self.tree.file_properties[self.file_id][name] = value
        if name in (properties.PROP_ENTRY_COMMITTED_DATE,
                    properties.PROP_ENTRY_LAST_AUTHOR,
                    properties.PROP_ENTRY_LOCK_TOKEN,
                    properties.PROP_ENTRY_COMMITTED_REV,
                    properties.PROP_ENTRY_UUID,
                    properties.PROP_IGNORE,
                    properties.PROP_EXECUTABLE):
            pass
        elif name.startswith(properties.PROP_WC_PREFIX):
            pass
        elif name.startswith(properties.PROP_PREFIX):
            mutter('unsupported dir property %r', name)

    def add_file(self, path, copyfrom_path=None, copyfrom_revnum=-1):
        path = path.decode("utf-8")
        return FileTreeEditor(self.tree, path)

    def close(self):
        pass


class FileTreeEditor(object):

    def __init__(self, tree, path):
        self.tree = tree
        self.path = path
        self.is_executable = False
        self.is_special = None
        self.is_symlink = False
        self.last_file_rev = None
        self.file_id, self.revision_id = self.tree.lookup_id(self.path)
        self.tree.file_properties[self.file_id] = {}

    def change_prop(self, name, value):
        self.tree.file_properties[self.file_id][name] = value
        if name == properties.PROP_EXECUTABLE:
            self.is_executable = (value != None)
        elif name == properties.PROP_SPECIAL:
            self.is_special = (value is not None)
        elif name == properties.PROP_EXTERNALS:
            mutter('%r property on file!', name)
        elif name == properties.PROP_ENTRY_COMMITTED_REV:
            self.last_file_rev = int(value)
        elif name in (properties.PROP_ENTRY_COMMITTED_DATE,
                      properties.PROP_ENTRY_LAST_AUTHOR,
                      properties.PROP_ENTRY_LOCK_TOKEN,
                      properties.PROP_ENTRY_UUID,
                      properties.PROP_MIME_TYPE):
            pass
        elif name.startswith(properties.PROP_WC_PREFIX):
            pass
        elif name.startswith(properties.PROP_PREFIX):
            mutter('unsupported file property %r', name)

    def close(self, checksum=None):
        if self.file_stream:
            self.file_stream.seek(0)
            file_data = self.file_stream.read()
        else:
            file_data = ""

        if self.is_special is not None:
            self.is_symlink = (self.is_special and file_data.startswith("link "))

        if self.is_symlink:
            ie = self.tree._bzr_inventory.add_path(self.path, 'symlink',
                self.file_id)
        else:
            ie = self.tree._bzr_inventory.add_path(self.path, 'file',
                self.file_id)
        ie.revision = self.revision_id

        actual_checksum = md5(file_data).hexdigest()
        assert checksum is None or checksum == actual_checksum, \
                "checksum mismatch: %r != %r" % (checksum, actual_checksum)

        if self.is_symlink:
            ie.symlink_target = file_data[len("link "):]
        else:
            ie.text_sha1 = osutils.sha_string(file_data)
            ie.text_size = len(file_data)
            ie.executable = self.is_executable

        self.file_stream = None

    def apply_textdelta(self, base_checksum):
        self.file_stream = BytesIO()
        return delta.apply_txdelta_handler("", self.file_stream)


class SvnBasisTree(SvnRevisionTreeCommon):
    """Optimized version of SvnRevisionTree."""

    def __repr__(self):
        return "<%s for '%r'>" % (self.__class__.__name__, self.workingtree)

    def __init__(self, workingtree):
        mutter("opening basistree for %r", workingtree)
        self.workingtree = workingtree
        self._bzr_inventory = None
        self._repository = workingtree.branch.repository
        self.id_map = self.workingtree.basis_idmap
        self.mapping = self.workingtree.branch.mapping
        self._real_tree = None

    def get_file_verifier(self, path, file_id=None, stat_value=None):
        with self.workingtree._get_wc(write_lock=False) as root_adm:
            entry = self.workingtree._get_entry(root_adm, path)
            return ("MD5", entry.checksum)

    @property
    def root_inventory(self):
        if self._bzr_inventory is not None:
            return self._bzr_inventory
        self._bzr_inventory = Inventory(root_id=None)
        if self.get_root_id() is None:
            return self._bzr_inventory
        def add_file_to_inv(relpath, id, revid, adm):
            if not isinstance(relpath, text_type):
                raise TypeError(relpath)
            (propchanges, props) = adm.get_prop_diffs(
                self.workingtree.abspath(relpath).encode("utf-8"))
            if props.has_key(properties.PROP_SPECIAL):
                is_symlink = (self.get_file_stream_by_path(relpath).read(5) == "link ")
            else:
                is_symlink = False

            if is_symlink:
                ie = self._bzr_inventory.add_path(relpath, 'symlink', id)
                ie.symlink_target = self.get_file_stream_by_path(relpath).read()[len("link "):]
            else:
                ie = self._bzr_inventory.add_path(relpath, 'file', id)
                data = osutils.fingerprint_file(self.get_file_stream_by_path(relpath))
                ie.text_sha1 = data['sha1']
                ie.text_size = data['size']
                ie.executable = props.has_key(properties.PROP_EXECUTABLE)
            ie.revision = revid
            return ie

        def find_ids(entry):
            assert entry.url.startswith(self._repository.svn_transport.svn_url)
            relpath = urllib.unquote(entry.url[len(self._repository.svn_transport.svn_url):].strip("/"))
            assert isinstance(relpath, str)
            if entry.schedule in (wc.SCHEDULE_NORMAL,
                                  wc.SCHEDULE_DELETE,
                                  wc.SCHEDULE_REPLACE):
                return self.lookup_id(
                    self.workingtree.unprefix(relpath.decode("utf-8")))
            return (None, None)

        def add_dir_to_inv(relpath, adm, parent_id):
            if not isinstance(relpath, text_type):
                raise TypeError(relpath)
            entries = adm.entries_read(False)
            entry = entries[""]
            (id, revid) = find_ids(entry)
            if id is None:
                return

            # First handle directory itself
            ie = self._bzr_inventory.add_path(relpath, 'directory', id)
            ie.revision = revid
            if relpath == u"":
                self._bzr_inventory.revision_id = revid

            for name, entry in entries.items():
                if name == "":
                    continue

                if not isinstance(relpath, text_type):
                    raise TypeError(relpath)
                if not isinstance(name, bytes):
                    raise TypeError(name)

                subrelpath = os.path.join(relpath, name.decode("utf-8"))

                assert entry

                if entry.kind == subvertpy.NODE_DIR:
                    try:
                        subwc = self.workingtree._get_wc(subrelpath)
                    except subvertpy.SubversionException as e:
                        msg, num = e.args
                        if num == subvertpy.ERR_WC_NOT_DIRECTORY:
                            raise BasisTreeIncomplete()
                        raise
                    try:
                        add_dir_to_inv(subrelpath, subwc, id)
                    finally:
                        subwc.close()
                else:
                    (subid, subrevid) = find_ids(entry)
                    if subid is not None:
                        add_file_to_inv(subrelpath, subid, subrevid, adm)

        with self.workingtree._get_wc() as adm:
            add_dir_to_inv(u"", adm, None)
        return self._bzr_inventory

    def has_filename(self, path):
        try:
            self.lookup_id(path)
        except KeyError:
            return False
        else:
            return True

    def get_revision_id(self):
        """See Tree.get_revision_id()."""
        return self.workingtree.last_revision()

    def get_file_stream_by_path(self, name):
        """See Tree.get_file_stream_by_path()."""
        wt_path = self.workingtree.abspath(name)
        return wc.get_pristine_contents(wt_path)

    def kind(self, path):
        file_id = self.path2id(path)
        if file_id is None:
            raise errors.NoSuchFile(path, self)
        return self.root_inventory.get_entry(file_id).kind

    def get_file_text(self, path):
        """See Tree.get_file_text()."""
        f = self.get_file_stream_by_path(path)
        try:
            return f.read()
        finally:
            f.close()

    def get_file(self, path):
        return self.get_file_stream_by_path(path)

    def get_file_properties(self, path):
        """See SubversionTree.get_file_properties()."""
        abspath = self.workingtree.abspath(path)
        if not os.path.isdir(abspath.encode(osutils._fs_enc)):
            wc_path = urlutils.dirname(path)
        else:
            wc_path = path
        with self.workingtree._get_wc(wc_path) as wc:
            (_, orig_props) = wc.get_prop_diffs(abspath.encode("utf-8"))
        return orig_props

    def annotate_iter(self, path, default_revision=CURRENT_REVISION):
        from .annotate import Annotater
        annotater = Annotater(self.workingtree.branch.repository)
        annotater.check_file_revs(
            self.get_revision_id(), self.workingtree.get_branch_path(),
            self.workingtree.base_revnum, self.mapping, path)
        return annotater.get_annotated()

    @property
    def real_tree(self):
        if self._real_tree is not None:
            return self._real_tree
        self._real_tree = self.workingtree.revision_tree(self.get_revision_id())
        return self._real_tree

    def all_file_ids(self):
        return self.real_tree.all_file_ids()

    def iter_entries_by_dir(self, specific_files=None):
        # FIXME
        if specific_files is not None:
            specific_file_ids = []
            for path in specific_files:
                file_id = self.path2id(path)
                if file_id is not None:
                    specific_file_ids.append(file_id)
        else:
            specific_file_ids = None
        try:
            return self.root_inventory.iter_entries_by_dir(
                specific_file_ids=specific_file_ids)
        except BasisTreeIncomplete:
            return self.real_tree.iter_entries_by_dir(
                specific_files=specific_files)

    def find_related_paths_across_trees(self, paths, trees=[],
            require_versioned=True):
        """Find related paths in tree corresponding to specified filenames in any
        of `lookup_trees`.

        All matches in all trees will be used, and all children of matched
        directories will be used.

        :param paths: The filenames to find related paths for (if None, returns
            None)
        :param trees: The trees to find file_ids within
        :param require_versioned: if true, all specified filenames must occur in
            at least one tree.
        :return: a set of paths for the specified filenames and their children
            in `tree`
        """
        if paths is None:
            return None;
        file_ids = self.paths2ids(
                paths, trees, require_versioned=require_versioned)
        ret = set()
        for file_id in file_ids:
            try:
                ret.add(self.id2path(file_id))
            except errors.NoSuchId:
                pass
        return ret
