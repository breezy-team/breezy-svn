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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Subversion Tags Dictionary."""

import subvertpy
from subvertpy import properties

from bzrlib import (
    urlutils,
    )
from bzrlib.errors import (
    InvalidRevisionId,
    NoSuchRevision,
    NoSuchTag,
    )
from bzrlib.tag import BasicTags
from bzrlib.trace import mutter

from bzrlib.plugins.svn import (
    commit,
    errors as svn_errors,
    mapping,
    )

def reverse_dict(orig):
    ret = {}
    for k, v in orig.iteritems():
        ret.setdefault(v, []).append(k)
    return ret


class SubversionTags(BasicTags):
    """Subversion tags object."""

    def __init__(self, branch):
        self.branch = branch
        self.repository = branch.repository
        self._parent_exists = set()

    def _ensure_tag_parent_exists(self, parent):
        """Make sure that the container of these tags exists.

        :param parent: Parent path
        :return: True if the container already existed, False if it had to 
            be created.
        """
        if parent in self._parent_exists:
            return True
        assert isinstance(parent, str)
        bp_parts = parent.split("/")
        existing_bp_parts = commit._check_dirs_exist(
                self.repository.transport, 
                bp_parts, self.repository.get_latest_revnum())
        if existing_bp_parts == bp_parts:
            self._parent_exists.add(parent)
            return True
        commit.create_branch_prefix(self.repository, 
                self._revprops("Add tags base directory."),
                bp_parts, existing_bp_parts)
        self._parent_exists.add(parent)
        return False

    def set_tag(self, tag_name, tag_target):
        """Set a new tag in a Subversion repository."""
        path = self.branch.layout.get_tag_path(tag_name, self.branch.project)
        assert isinstance(path, str)
        parent = urlutils.dirname(path)
        try:
            (from_uuid, from_bp, from_revnum), mapping = self.repository.lookup_revision_id(tag_target, project=self.branch.project)
        except NoSuchRevision:
            mutter("not setting tag %s; unknown revision %s", tag_name, tag_target)
            return
        self._ensure_tag_parent_exists(parent)
        # FIXME: Make sure that path doesn't point at (from_uuid, from_bp, from_revnum) yet. (342824)
        conn = self.repository.transport.get_connection(parent)
        deletefirst = (conn.check_path(urlutils.basename(path), self.repository.get_latest_revnum()) != subvertpy.NODE_NONE)
        try:
            ci = svn_errors.convert_svn_error(conn.get_commit_editor)(
                    self._revprops("Add tag %s" % tag_name.encode("utf-8"),
                    {tag_name.encode("utf-8"): tag_target}))
            try:
                root = ci.open_root()
                if deletefirst:
                    root.delete_entry(urlutils.basename(path))
                tag_dir = root.add_directory(urlutils.basename(path), urlutils.join(self.repository.base, from_bp), from_revnum)
                tag_dir.close()
                root.close()
            except:
                ci.abort()
                raise
            ci.close()
        finally:
            self.repository.transport.add_connection(conn)

    def _revprops(self, message, tags_dict=None):
        """Create a revprops dictionary.

        Optionally sets bzr:skip to slightly optimize fetching of this revision later.
        """
        revprops = {properties.PROP_REVISION_LOG: message, }
        if self.repository.transport.has_capability("commit-revprops"):
            revprops[mapping.SVN_REVPROP_BZR_SKIP] = ""
        return revprops

    def lookup_tag(self, tag_name):
        try:
            return self.get_tag_dict()[tag_name]
        except KeyError:
            raise NoSuchTag(tag_name)

    def _get_tag_dict_revmeta(self, from_revnum=None, to_revnum=None):
        if from_revnum is None or from_revnum == 0:
            return self.repository.find_tags(project=self.branch.project, 
                    layout=self.branch.layout,
                    mapping=self.branch.mapping,
                    revnum=self.branch._revnum)
        else:
            return self.repository.find_tags_between(project=self.branch.project,
                    layout=self.branch.layout,
                    mapping=self.branch.mapping,
                    from_revnum=from_revnum,
                    to_revnum=to_revnum)

    def _resolve_reverse_tags_fallback(self, reverse_tag_revmetas):
        """Determine the revids for tags that were not found in the branch 
        ancestry.
        """
        ret = {}
        # For anything that's not in the branches' ancestry, just use 
        # the latest mapping
        for (revmeta, names) in reverse_tag_revmetas.iteritems():
            mapping = revmeta.get_original_mapping() or self.branch.mapping
            try:
                revid = revmeta.get_revision_id(mapping)
            except subvertpy.SubversionException, (_, ERR_FS_NOT_DIRECTORY):
                continue
            for name in names:
                assert isinstance(name, basestring)
                ret[name] = revid
        return ret

    def _resolve_tags_svn_ancestry(self, tag_revmetas):
        """Resolve a name -> revmeta dictionary to a name -> revid dict.

        The tricky bit here is figuring out what mapping to use. Preferably, 
        we should be using whatever mapping makes the tag useful to the user, 
        which generally means using the revid that is in the ancestry of the 
        branch.

        As fallback, we will use the mapping used by the branch. That will 
        however cause question marks to show up rather than revno's in 
        "bzr tags".
        """
        if len(tag_revmetas) == 0:
            return {}
        reverse_tag_revmetas = reverse_dict(tag_revmetas)
        ret = {}
        # Try to find the tags that are in the ancestry of this branch
        # and use their appropriate mapping
        for (revmeta, mapping) in self.branch._iter_revision_meta_ancestry():
            if revmeta not in reverse_tag_revmetas:
                continue
            if len(reverse_tag_revmetas) == 0:
                # No more tag revmetas to resolve, just return immediately
                return ret
            for name in reverse_tag_revmetas[revmeta]:
                assert isinstance(name, basestring)
                ret[name] = revmeta.get_revision_id(mapping)
            del reverse_tag_revmetas[revmeta]
        ret.update(self._resolve_reverse_tags_fallback(reverse_tag_revmetas))
        return ret

    def _resolve_tags_ancestry(self, tag_revmetas, graph, last_revid):
        """Resolve a name -> revmeta dictionary using the ancestry of a branch.
        """
        ret = {}
        reverse_tag_revmetas = reverse_dict(tag_revmetas)
        foreign_revid_map = {}
        for revmeta in reverse_tag_revmetas:
            foreign_revid_map[revmeta.get_foreign_revid()] = revmeta
        for revid, _ in graph.iter_ancestry([last_revid]):
            if len(reverse_tag_revmetas) == 0:
                # No more tag revmetas to resolve, just return immediately
                return ret
            try:
                foreign_revid, m = mapping.mapping_registry.parse_revision_id(revid)
            except InvalidRevisionId:
                continue
            if not foreign_revid in foreign_revid_map:
                continue
            revmeta = foreign_revid_map[foreign_revid]
            for name in reverse_tag_revmetas[revmeta]:
                assert isinstance(name, basestring)
                ret[name] = revid
            del reverse_tag_revmetas[revmeta]
        ret.update(self._resolve_reverse_tags_fallback(reverse_tag_revmetas))
        return ret

    def get_tag_dict(self):
        tag_revmetas = self._get_tag_dict_revmeta()
        return self._resolve_tags_svn_ancestry(tag_revmetas)

    def get_reverse_tag_dict(self):
        """Returns a dict with revisions as keys
           and a list of tags for that revision as value"""
        return reverse_dict(self.get_tag_dict())

    def delete_tag(self, tag_name):
        path = self.branch.layout.get_tag_path(tag_name, self.branch.project)
        parent = urlutils.dirname(path)
        conn = self.repository.transport.get_connection(parent)
        try:
            if conn.check_path(urlutils.basename(path), self.repository.get_latest_revnum()) != subvertpy.NODE_DIR:
                raise NoSuchTag(tag_name)
            ci = svn_errors.convert_svn_error(conn.get_commit_editor)(self._revprops("Remove tag %s" % tag_name.encode("utf-8"),
                                        {tag_name: ""}))
            try:
                root = ci.open_root()
                root.delete_entry(urlutils.basename(path))
                root.close()
            except:
                ci.abort()
                raise
            ci.close()
        finally:
            assert not conn.busy
            self.repository.transport.add_connection(conn)

    def _set_tag_dict(self, dest_dict):
        cur_dict = self.get_tag_dict()
        for k, v in dest_dict.iteritems():
            if cur_dict.get(k) != v:
                self.set_tag(k, v)
        for k in cur_dict:
            if k not in dest_dict:
                self.delete_tag(k)

    def merge_to(self, to_tags, overwrite=False, _from_revnum=None, _to_revnum=None):
        """Copy tags between repositories if necessary and possible.
        
        This method has common command-line behaviour about handling 
        error cases.

        All new definitions are copied across, except that tags that already
        exist keep their existing definitions.

        :param to_tags: Branch to receive these tags
        :param overwrite: Overwrite conflicting tags in the target branch

        :returns: A list of tags that conflicted, each of which is 
            (tagname, source_target, dest_target), or None if no copying was
            done.
        """
        if self.branch == to_tags.branch:
            return
        if not self.branch.supports_tags():
            # obviously nothing to copy
            return
        tag_revmetas = self._get_tag_dict_revmeta(_from_revnum, _to_revnum)
        if len(tag_revmetas) == 0:
            # no tags in the source, and we don't want to clobber anything
            # that's in the destination
            return
        to_tags.branch.lock_write()
        try:
            graph = to_tags.branch.repository.get_graph()
            source_dict = self._resolve_tags_ancestry(tag_revmetas, 
                graph, to_tags.branch.last_revision())
            dest_dict = to_tags.get_tag_dict()
            result, conflicts = self._reconcile_tags(source_dict, dest_dict,
                                                     overwrite)
            if result != dest_dict:
                to_tags._set_tag_dict(result)
        finally:
            to_tags.branch.unlock()
        return conflicts

