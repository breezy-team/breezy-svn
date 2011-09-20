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
from subvertpy import (
    NODE_NONE,
    properties,
    )

from bzrlib import (
    errors as bzr_errors,
    ui,
    urlutils,
    )
from bzrlib.tag import BasicTags
from bzrlib.trace import mutter

from bzrlib.plugins.svn import (
    errors as svn_errors,
    )
from bzrlib.plugins.svn.mapping import (
    SVN_REVPROP_BZR_SKIP,
    mapping_registry,
    )
from bzrlib.plugins.svn.transport import (
    check_dirs_exist,
    create_branch_prefix,
    )


GhostTagsNotSupported = getattr(bzr_errors, "GhostTagsNotSupported", None)


def reverse_dict(orig):
    ret = {}
    for k, v in orig.iteritems():
        ret.setdefault(v, []).append(k)
    return ret


def _resolve_reverse_tags_fallback(branch, reverse_tag_revmetas):
    """Determine the revids for tags that were not found in the branch
    ancestry.
    """
    # For anything that's not in the branches' ancestry, just use
    # the latest mapping
    for (revmeta, names) in reverse_tag_revmetas.iteritems():
        mapping = revmeta.get_original_mapping() or branch.mapping
        try:
            revid = revmeta.get_revision_id(mapping)
        except subvertpy.SubversionException, (_, ERR_FS_NOT_DIRECTORY,):
            continue
        for name in names:
            assert isinstance(name, unicode)
            yield (name, (revmeta, mapping, revid))


def resolve_tags_svn_ancestry(branch, tag_revmetas):
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
    pb = ui.ui_factory.nested_progress_bar()
    try:
        for (revmeta, hidden, mapping) in branch._iter_revision_meta_ancestry(
            pb=pb):
            if revmeta not in reverse_tag_revmetas:
                continue
            if hidden:
                mutter("tagged hidden revision %r", revmeta)
                continue # This is bad.
            if len(reverse_tag_revmetas) == 0:
                # No more tag revmetas to resolve, just return immediately
                return ret
            for name in reverse_tag_revmetas[revmeta]:
                assert isinstance(name, unicode)
                ret[name] = (revmeta, mapping, revmeta.get_revision_id(mapping))
            del reverse_tag_revmetas[revmeta]
    finally:
        pb.finished()
    ret.update(_resolve_reverse_tags_fallback(branch, reverse_tag_revmetas))
    return ret


class ReverseTagDict(object):

    def __init__(self, branch, repository, tags, project):
        self.branch = branch
        self.repository = repository
        self.project = project
        self._by_foreign_revid = {}
        self._tags = tags
        for name, revmeta in tags.iteritems():
            self._by_foreign_revid.setdefault(revmeta.metarev.get_foreign_revid(), []).append(name)

    def _lookup_revid(self, revid):
        return self.repository.lookup_bzr_revision_id(revid,
            project=self.project)

    def has_key(self, revid):
        foreign_revid, mapping = self._lookup_revid(revid)
        return self._by_foreign_revid.has_key(foreign_revid)

    __contains__ = has_key

    def get(self, revid, default=None):
        foreign_revid, mapping = self._lookup_revid(revid)
        return self._by_foreign_revid.get(foreign_revid, default)

    def __getitem__(self, key):
        foreign_revid, mapping = self._lookup_revid(key)
        return self._by_foreign_revid[foreign_revid]

    def items(self):
        d = resolve_tags_svn_ancestry(self.branch, self._tags)
        rev = {}
        for key, (revmeta, mapping, revid) in d.iteritems():
            rev.setdefault(revid, []).append(key)
        return rev.items()

    def iteritems(self):
        return iter(self.items())

    def __iter__(self):
        return self.iterkeys()

    def iterkeys(self):
        return (k for (k, v) in self.iteritems())

    def keys(self):
        return list(self.iterkeys())


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
        existing_bp_parts = check_dirs_exist(
                self.repository.transport,
                bp_parts, self.repository.get_latest_revnum())
        if existing_bp_parts == bp_parts:
            self._parent_exists.add(parent)
            return True
        create_branch_prefix(self.repository.transport,
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
            (from_uuid, from_bp, from_revnum), mapping = self.repository.lookup_bzr_revision_id(tag_target, project=self.branch.project)
        except bzr_errors.NoSuchRevision:
            mutter("not setting tag %s; unknown revision %s", tag_name, tag_target)
            if GhostTagsNotSupported is not None:
                raise GhostTagsNotSupported(self.branch._format)
            return
        self._ensure_tag_parent_exists(parent)
        try:
            current_from_foreign_revid = self._lookup_tag_revmeta(path).metarev.get_foreign_revid()
            deletefirst = True
        except KeyError:
            current_from_foreign_revid = None
            deletefirst = False
        if current_from_foreign_revid == (from_uuid, from_bp, from_revnum):
            # Already present
            return
        mutter("setting tag %s from %r (deletefirst: %r)", path,
               (from_uuid, from_bp, from_revnum), deletefirst)
        conn = self.repository.transport.get_connection(parent)
        try:
            ci = svn_errors.convert_svn_error(conn.get_commit_editor)(
                    self._revprops("Add tag %s" % tag_name.encode("utf-8"),
                    {tag_name.encode("utf-8"): tag_target}))
            try:
                root = ci.open_root()
                if deletefirst:
                    root.delete_entry(urlutils.basename(path))
                tag_dir = root.add_directory(urlutils.basename(path),
                    urlutils.join(self.repository.base, from_bp), from_revnum)
                tag_dir.close()
                root.close()
            except:
                ci.abort()
                raise
            ci.close()
            # FIXME: This shouldn't have to remove the entire cache, just update it
            self.repository._clear_cached_state()
        finally:
            self.repository.transport.add_connection(conn)

    def _revprops(self, message, tags_dict=None):
        """Create a revprops dictionary.

        Optionally sets bzr:skip to slightly optimize fetching of this
        revision later.
        """
        revprops = {properties.PROP_REVISION_LOG: message, }
        if self.repository.transport.has_capability("commit-revprops"):
            revprops[SVN_REVPROP_BZR_SKIP] = ""
        return revprops

    def _lookup_tag_revmeta(self, path):
        revnum = self.repository.get_latest_revnum()
        if self.repository.transport.check_path(path, revnum) == NODE_NONE:
            raise KeyError
        tip, hidden, mapping = self.repository._revmeta_provider._iter_reverse_revmeta_mapping_history(
            path, revnum, to_revnum=0, mapping=self.branch.mapping).next()
        assert not hidden
        return tip.get_tag_revmeta(mapping)

    def lookup_tag(self, tag_name):
        # Note that this can't use _lookup_tag_revmeta, as there can
        # be multiple tag container directories
        try:
            return self.get_tag_dict()[tag_name]
        except KeyError:
            raise bzr_errors.NoSuchTag(tag_name)

    def _get_tag_dict_revmeta(self, from_revnum=None, to_revnum=None):
        """Get a name -> revmeta dictionary."""
        if from_revnum is None or from_revnum == 0:
            return self.repository.find_tags(
                    project=self.branch.project,
                    layout=self.branch.layout,
                    mapping=self.branch.mapping,
                    revnum=to_revnum)
        elif from_revnum <= to_revnum:
            return self.repository.find_tags_between(
                    project=self.branch.project,
                    layout=self.branch.layout,
                    mapping=self.branch.mapping,
                    from_revnum=from_revnum,
                    to_revnum=to_revnum)
        else:
            return {}

    def _resolve_tags_ancestry(self, tag_revmetas, graph, last_revid):
        """Resolve a name -> revmeta dictionary using the ancestry of a branch.

        :param tag_revmetas: Dictionary mapping names to revmeta objects
        :param graph: Graph object
        :param last_revid: Branch last revid
        :return: Dictionary mapping unicode tag names to revision ids
        """
        ret = {}
        reverse_tag_revmetas = reverse_dict(tag_revmetas)
        foreign_revid_map = {}
        for revmeta in reverse_tag_revmetas:
            foreign_revid_map[revmeta.metarev.get_foreign_revid()] = revmeta
        for revid, _ in graph.iter_ancestry([last_revid]):
            if len(reverse_tag_revmetas) == 0:
                # No more tag revmetas to resolve, just return immediately
                return ret
            try:
                foreign_revid, m = mapping_registry.parse_revision_id(revid)
            except bzr_errors.InvalidRevisionId:
                continue
            if not foreign_revid in foreign_revid_map:
                continue
            revmeta = foreign_revid_map[foreign_revid]
            for name in reverse_tag_revmetas[revmeta]:
                ret[name] = (revmeta, m, revid)
            del reverse_tag_revmetas[revmeta]
        ret.update(_resolve_reverse_tags_fallback(self.branch,
                                                  reverse_tag_revmetas))
        return ret

    def get_tag_dict(self):
        tag_revmetas = self._get_tag_dict_revmeta()
        d = resolve_tags_svn_ancestry(self.branch, tag_revmetas)
        return dict([(k, v[2]) for (k, v) in d.iteritems()])

    def get_reverse_tag_dict(self):
        """Returns a dict with revisions as keys
           and a list of tags for that revision as value"""
        return ReverseTagDict(self.branch, self.repository,
                              self._get_tag_dict_revmeta(),
                              self.branch.project)

    def delete_tag(self, tag_name):
        path = self.branch.layout.get_tag_path(tag_name, self.branch.project)
        parent = urlutils.dirname(path)
        conn = self.repository.transport.get_connection(parent)
        try:
            if conn.check_path(urlutils.basename(path),
                    self.repository.get_latest_revnum()) != subvertpy.NODE_DIR:
                raise bzr_errors.NoSuchTag(tag_name)
            ci = svn_errors.convert_svn_error(conn.get_commit_editor)(
                    self._revprops("Remove tag %s" % tag_name.encode("utf-8"),
                    {tag_name: ""}))
            try:
                root = ci.open_root()
                root.delete_entry(urlutils.basename(path))
                root.close()
            except:
                ci.abort()
                raise
            ci.close()
            # FIXME: This shouldn't have to remove the entire cache, just update it
            self.repository._clear_cached_state()
        finally:
            assert not conn.busy
            self.repository.transport.add_connection(conn)

    def _set_tag_dict(self, dest_dict):
        cur_dict = self.get_tag_dict()
        for k, v in dest_dict.iteritems():
            if cur_dict.get(k) != v:
                try:
                    self.set_tag(k, v)
                except GhostTagsNotSupported:
                    # Silently ignore..
                    pass
        for k in cur_dict:
            if k not in dest_dict:
                self.delete_tag(k)

    def merge_to(self, to_tags, overwrite=False, _from_revnum=None,
                 _to_revnum=None):
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
            ret = self._reconcile_tags(
                dict([(k, v[2]) for (k, v) in source_dict.items()]),
                dest_dict, overwrite)
            if ret[0] != dest_dict:
                to_tags._set_tag_dict(ret[0])
            # bzr < 2.5 returns a 2-tuple, >= 2.5 returns a 3-tuple
            if len(ret) == 3:
                return (ret[1], ret[2])
            else:
                return ret[1]
        finally:
            to_tags.branch.unlock()
