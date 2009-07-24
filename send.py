# Copyright (C) 2009 Lukas Lalinsky <lalinsky@gmail.com>
# Copyright (C) 2009 Jelmer Vernooij <jelmer@samba.org>

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

import time
from bzrlib import (
    branch as _mod_branch,
    diff as _mod_diff,
    errors,
    merge_directive,
    osutils,
    )

from cStringIO import StringIO

from subvertpy import (
    properties,
    )


class SvnDiffTree(_mod_diff.DiffTree):
    """Provides a text representation between two trees, formatted for svn."""

    def _get_svn_rev_info(self, tree, file_id):
        try:
            rev = tree.inventory[file_id].revision
        except errors.NoSuchId:
            return '(revision 0)'
        if rev is None:
            return '(working copy)'
        try:
            info = self.repository.lookup_revision_id(rev)
        except errors.NoSuchRevision:
            return '(working copy)'
        return '(revision %d)' % info[0][2]

    def _write_contents_diff(self, path, old_version, old_contents, new_version, new_contents):
        if path is None:
            return 
        self.to_file.write("Index: %s\n" % path)
        self.to_file.write("=" * 67 + "\n")
        old_label = '%s\t%s' % (path, old_version)
        new_label = '%s\t%s' % (path, new_version)
        _mod_diff.internal_diff(old_label, old_contents,
                                new_label, new_contents,
                                self.to_file)

    def _write_properties_diff(self, path, old_properties, new_properties):
        if new_properties is None:
            return
        if old_properties is None:
            old_properties = {}
        changed = []
        for name in set(old_properties.keys() + new_properties.keys()):
            oldval = old_properties.get(name)
            newval = new_properties.get(name)
            if oldval != newval:
                changed.append((name, oldval, newval))
        if changed == []:
            return
        self.to_file.write("Property changes on: %s\n" % path)
        self.to_file.write("_" * 67 + "\n")
        for (name, old_value, new_value) in changed:
            if old_value is None:
                self.to_file.write("Added: %s\n\t+%s\n" % (name, new_value))
            elif new_value is None:
                self.to_file.write("Removed: %s\n\t-%s\n" % (name, old_value))
            else:
                self.to_file.write("Changed: %s\n\t-%s\n\t+%s\n" % (name, old_value, new_value))

    def _get_file_properties(self, tree, path, kind, executable):
        if kind in (None, "directory"):
            return None
        ret = {}
        if executable:
            ret[properties.PROP_EXECUTABLE] = properties.PROP_EXECUTABLE_VALUE
        if kind == "symlink":
            ret[properties.PROP_SPECIAL] = properties.PROP_SPECIAL_VALUE
        return ret

    def _get_file_contents(self, tree, file_id, path, kind):
        if kind in (None, "directory"):
            return None
        if kind == "symlink":
            return ["link %s" % tree.get_symlink_target(file_id)]
        return tree.get_file(file_id).readlines()

    def _show_diff(self, specific_files, extra_trees):
        iterator = self.new_tree.iter_changes(self.old_tree,
                                               specific_files=specific_files,
                                               extra_trees=extra_trees,
                                               require_versioned=True)
        has_changes = 0
        def get_encoded_path(path):
            if path is not None:
                return path.encode(self.path_encoding, "replace")
        for (file_id, paths, changed_content, versioned, parent, name, kind,
             executable) in iterator:
            # The root does not get diffed, and items with no known kind (that
            # is, missing) in both trees are skipped as well.
            if parent == (None, None) or kind == (None, None):
                continue
            oldpath, newpath = paths
            oldpath_encoded = get_encoded_path(paths[0])
            newpath_encoded = get_encoded_path(paths[1])
            old_present = (kind[0] is not None and versioned[0])
            new_present = (kind[1] is not None and versioned[1])
            renamed = (parent[0], name[0]) != (parent[1], name[1])
            old_properties = self._get_file_properties(self.old_tree, oldpath_encoded, kind[0], executable[0])
            new_properties = self._get_file_properties(self.new_tree, newpath_encoded, kind[1], executable[1])
            old_version = self._get_svn_rev_info(self.old_tree, file_id)
            new_version = self._get_svn_rev_info(self.new_tree, file_id)

            if oldpath_encoded == newpath_encoded:
                if changed_content:
                    old_contents = self._get_file_contents(self.old_tree, file_id, oldpath_encoded, kind[0])
                    new_contents = self._get_file_contents(self.new_tree, file_id, newpath_encoded, kind[1])
                    self._write_contents_diff(oldpath_encoded, old_version, old_contents, new_version, new_contents)
                self._write_properties_diff(oldpath_encoded, old_properties, new_properties)
            else:
                old_contents = self._get_file_contents(self.old_tree, file_id, oldpath_encoded, kind[0])
                new_contents = self._get_file_contents(self.new_tree, file_id, newpath_encoded, kind[1])
                self._write_contents_diff(oldpath_encoded, old_version, old_contents, new_version, [])
                self._write_contents_diff(newpath_encoded, old_version, [], new_version, new_contents)
                self._write_properties_diff(newpath_encoded, {}, new_properties)

            has_changes = (changed_content or renamed)

        return has_changes


class SvnMergeDirective(merge_directive._BaseMergeDirective):

    def to_lines(self):
        return self.patch.splitlines(True)

    @classmethod
    def _generate_diff(cls, repository, svn_repository, revision_id, ancestor_id):
        tree_1 = repository.revision_tree(ancestor_id)
        tree_2 = repository.revision_tree(revision_id)
        s = StringIO()
        differ = SvnDiffTree.from_trees_options(tree_1, tree_2, s, 'utf8', None,
            '', '', None)
        differ.repository = svn_repository
        differ.show_diff(None, None)
        return s.getvalue()

    @classmethod
    def from_objects(cls, repository, revision_id, time, timezone,
                     target_branch, local_target_branch=None,
                     public_branch=None, message=None):
        from bzrlib.plugins.svn.repository import SvnRepository
        submit_branch = _mod_branch.Branch.open(target_branch)
        if not isinstance(submit_branch.repository, SvnRepository):
            raise errors.BzrError("Not a Subversion repository")

        submit_branch.lock_read()
        try:
            submit_revision_id = submit_branch.last_revision()
            repository.fetch(submit_branch.repository, submit_revision_id)
            graph = repository.get_graph()
            ancestor_id = graph.find_unique_lca(revision_id,
                                                submit_revision_id)
            patch = cls._generate_diff(repository, submit_branch.repository,
                                    revision_id, ancestor_id)
        finally:
            submit_branch.unlock()
        return cls(revision_id, None, time, timezone, target_branch,
            patch, None, public_branch, message)


def send_svn(branch, revision_id, submit_branch, public_branch,
              no_patch, no_bundle, message, base_revision_id):
    return SvnMergeDirective.from_objects(
        branch.repository, revision_id, time.time(),
        osutils.local_time_offset(), submit_branch,
        public_branch=public_branch, message=message)
