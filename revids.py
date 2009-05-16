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

"""Revision id generation and caching."""

import subvertpy

from bzrlib import ui
from bzrlib.errors import (
    InvalidRevisionId,
    NoSuchRevision,
    )
from bzrlib.lru_cache import LRUCache

from bzrlib.plugins.svn.errors import (
    InvalidBzrSvnRevision,
    InvalidPropertyValue,
    )
from bzrlib.plugins.svn.mapping import (
    BzrSvnMapping,
    SVN_PROP_BZR_REVISION_ID,
    find_mapping_revprops,
    find_new_lines,
    is_bzr_revision_revprops,
    mapping_registry,
    parse_revid_property,
    )

class RevidMap(object):
    
    def __init__(self, repos):
        self.repos = repos

    def get_branch_revnum(self, revid, layout, project=None):
        """Find the (branch, revnum) tuple for a revision id.
        
        :return: Tuple with foreign revision id and mapping.
        """
        last_revnum = self.repos.get_latest_revnum()
        fileprops_to_revnum = last_revnum
        pb = ui.ui_factory.nested_progress_bar()
        try:
            for entry_revid, branch, revnum, mapping in self.discover_revprop_revids(last_revnum, 0, pb=pb):
                if revid == entry_revid:
                    return (self.repos.uuid, branch, revnum), mapping
                fileprops_to_revnum = min(fileprops_to_revnum, revnum)

            for entry_revid, branch, min_revno, max_revno, mapping in self.discover_fileprop_revids(layout, 0, fileprops_to_revnum, project, pb=pb):
                if revid == entry_revid:
                    (foreign_revid, mapping_name) = self.bisect_revid_revnum(revid, branch, min_revno, max_revno)
                    return (foreign_revid, mapping_name)
        finally:
            pb.finished()
        raise NoSuchRevision(self, revid)

    def discover_revprop_revids(self, from_revnum, to_revnum, pb=None):
        """Discover bzr-svn revision properties between from_revnum and to_revnum.

        :return: First revision number on which a revision property was found, or None
        """
        if self.repos.transport.has_capability("log-revprops") != True:
            return
        for (paths, revnum, revprops) in self.repos._log.iter_changes(None, from_revnum, to_revnum):
            if pb is not None:
                pb.update("discovering revprop revisions", from_revnum-revnum, from_revnum-to_revnum)
            if is_bzr_revision_revprops(revprops):
                mapping = find_mapping_revprops(revprops)
                assert mapping is not None
                branch_path = mapping.get_branch_root(revprops)
                if branch_path is None:
                    continue
                revno, revid, hidden = mapping.get_revision_id_revprops(revprops)
                if revid is not None:
                    yield (revid, branch_path.strip("/"), revnum, mapping)

    def discover_fileprop_revids(self, layout, from_revnum, to_revnum, project=None, pb=None):
        reuse_policy = self.repos.get_config().get_reuse_revisions()
        assert reuse_policy in ("other-branches", "removed-branches", "none") 
        check_removed = (reuse_policy == "removed-branches")
        # TODO: Some sort of progress indication
        for (branch, revno, exists) in self.repos.find_fileprop_paths(layout, from_revnum, to_revnum, project, check_removed=check_removed):
            if pb is not None:
                pb.update("finding fileprop revids", revno-from_revnum, to_revnum-from_revnum)
            assert isinstance(branch, str)
            assert isinstance(revno, int)
            # Look at their bzr:revision-id-vX
            revids = set()
            try:
                revmeta = self.repos._revmeta_provider.lookup_revision(branch, self.repos._log.find_latest_change(branch, revno))
                if revmeta.consider_bzr_fileprops():
                    for revid, bzr_revno, mapping_name in revmeta.get_roundtrip_ancestor_revids():
                        revids.add(((bzr_revno, revid), mapping_name))
            except subvertpy.SubversionException, (_, num):
                if num == subvertpy.ERR_FS_NOT_DIRECTORY:
                    continue
                raise

            # If there are any new entries that are not yet in the cache, 
            # add them
            for ((entry_revno, entry_revid), mapping_name) in revids:
                try:
                    mapping = mapping_registry.parse_mapping_name("svn-" + mapping_name)
                except KeyError:
                    pass
                else:
                    yield (entry_revid, branch, 0, revno, mapping)

    def bisect_revid_revnum(self, revid, branch_path, min_revnum, max_revnum):
        """Find out what the actual revnum was that corresponds to a revid.

        :param revid: Revision id to search for
        :param branch_path: Branch path at which to start searching
        :param min_revnum: Last revnum to check
        :param max_revnum: First revnum to check
        :return: Tuple with foreign revision id and mapping
        """
        assert min_revnum <= max_revnum
        # Find the branch property between min_revnum and max_revnum that 
        # added revid
        for revmeta in self.repos._revmeta_provider.iter_reverse_branch_changes(branch_path, max_revnum, min_revnum):
            for propname, (oldpropvalue, propvalue) in revmeta.get_changed_fileprops().iteritems():
                if not propname.startswith(SVN_PROP_BZR_REVISION_ID):
                    continue
                try:
                    new_lines = find_new_lines((oldpropvalue, propvalue))
                    if len(new_lines) != 1:
                        continue
                except ValueError:
                    # Don't warn about encountering an invalid property, 
                    # that will already have happened earlier
                    continue
                try:
                    (entry_revno, entry_revid) = parse_revid_property(
                        new_lines[0])
                except InvalidPropertyValue:
                    # Don't warn about encountering an invalid property, 
                    # that will already have happened earlier
                    continue
                if entry_revid == revid:
                    mapping_name = propname[len(SVN_PROP_BZR_REVISION_ID):]
                    mapping = mapping_registry.parse_mapping_name("svn-" + mapping_name)
                    assert mapping.is_branch_or_tag(revmeta.branch_path)
                    return (revmeta.get_foreign_revid(), mapping)

        raise InvalidBzrSvnRevision(revid)


class MemoryCachingRevidMap(object):

    def __init__(self, actual):
        self.actual = actual
        self._cache = LRUCache()
        self._nonexistant_revnum = None
        self._nonexistant = set()

    def get_branch_revnum(self, revid, layout, project=None):
        if revid in self._cache:
            return self._cache[revid]

        last_revnum = self.actual.repos.get_latest_revnum()
        if self._nonexistant_revnum is not None:
            if last_revnum <= self._nonexistant_revnum:
                if revid in self._nonexistant:
                    raise NoSuchRevision(self, revid)

        try:
            ret = self.actual.get_branch_revnum(revid, layout, project)
        except NoSuchRevision:
            if self._nonexistant_revnum != last_revnum:
                self._nonexistant_revnum = last_revnum
                self._nonexistant = set()
            self._nonexistant.add(revid)
            raise
        else:
            self._cache[revid] = ret
            return ret


class DiskCachingRevidMap(object):

    def __init__(self, actual, cache):
        self.cache = cache
        self.actual = actual
        self.revid_seen = set()

    def remember_entry(self, entry_revid, branch, min_revnum, max_revnum, 
                       mappingname):
        if entry_revid not in self.revid_seen:
            self.cache.insert_revid(entry_revid, branch, min_revnum, max_revnum,
                                    mappingname)
            self.revid_seen.add(entry_revid)

    def get_branch_revnum(self, revid, layout, project=None):
        # Check the record out of the cache, if it exists
        try:
            (branch_path, min_revnum, max_revnum, \
                    mapping) = self.cache.lookup_revid(revid)
            assert isinstance(branch_path, str)
            assert isinstance(mapping, str)
            # Entry already complete?
            assert min_revnum <= max_revnum
            if min_revnum == max_revnum:
                return (self.actual.repos.uuid, branch_path, min_revnum), mapping_registry.parse_mapping_name("svn-" + mapping)
        except NoSuchRevision, e:
            last_revnum = self.actual.repos.get_latest_revnum()
            last_checked = self.cache.last_revnum_checked(repr((layout, project)))
            if (last_revnum <= last_checked):
                # All revision ids in this repository for the current 
                # layout have already been discovered. No need to 
                # check again.
                raise e
            found = None
            fileprops_to_revnum = last_revnum
            pb = ui.ui_factory.nested_progress_bar()
            try:
                for entry_revid, branch, revnum, mapping in self.actual.discover_revprop_revids(last_revnum, last_checked, pb=pb):
                    fileprops_to_revnum = min(fileprops_to_revnum, revnum)
                    if entry_revid == revid:
                        found = (branch, revnum, revnum, mapping)
                    self.remember_entry(entry_revid, branch, revnum, 
                                            revnum, mapping.name)
                for entry_revid, branch, min_revno, max_revno, mapping in self.actual.discover_fileprop_revids(layout, last_checked, fileprops_to_revnum, project, pb):
                    min_revno = max(last_checked, min_revno)
                    if entry_revid == revid:
                        found = (branch, min_revno, max_revno, mapping)
                    self.remember_entry(entry_revid, branch, min_revno, 
                                        max_revno, mapping.name)
            finally:
                pb.finished()
            # We've added all the revision ids for this layout in the
            # repository, so no need to check again unless new revisions got 
            # added
            self.cache.set_last_revnum_checked(repr((layout, project)), last_revnum)
            if found is None:
                raise e
            (branch_path, min_revnum, max_revnum, mapping) = found
            assert min_revnum <= max_revnum
            assert isinstance(branch_path, str)

        ((uuid, branch_path, revnum), mapping) = self.actual.bisect_revid_revnum(revid, 
            branch_path, min_revnum, max_revnum)
        self.remember_entry(revid, branch_path, revnum, revnum, mapping.name)
        return (uuid, branch_path, revnum), mapping


class RevisionIdMapCache(object):
    """Revision id mapping store. 

    Stores mapping from revid -> (path, revnum, mapping)
    """
    def set_last_revnum_checked(self, layout, revnum):
        """Remember the latest revision number that has been checked
        for a particular layout.

        :param layout: Repository layout.
        :param revnum: Revision number.
        """
        raise NotImplementedError(self.set_last_revnum_checked)

    def last_revnum_checked(self, layout):
        """Retrieve the latest revision number that has been checked 
        for revision ids for a particular layout.

        :param layout: Repository layout.
        :return: Last revision number checked or 0.
        """
        raise NotImplementedError(self.last_revnum_checked)

    def lookup_revid(self, revid):
        """Lookup the details for a particular revision id.

        :param revid: Revision id.
        :return: Tuple with path inside repository, minimum revision number, maximum revision number and 
            mapping.
        """
        raise NotImplementedError(self.lookup_revid)

    def lookup_branch_revnum(self, revnum, path, mapping):
        """Lookup a revision by revision number, branch path and mapping.

        :param revnum: Subversion revision number.
        :param path: Subversion branch path.
        :param mapping: Mapping
        """
        raise NotImplementedError(self.lookup_branch_revnum)

    def insert_revid(self, revid, branch, min_revnum, max_revnum, mapping):
        """Insert a revision id into the revision id cache.

        :param revid: Revision id for which to insert metadata.
        :param branch: Branch path at which the revision was seen
        :param min_revnum: Minimum Subversion revision number in which the 
                           revid was found
        :param max_revnum: Maximum Subversion revision number in which the 
                           revid was found
        :param mapping: Name of the mapping with which the revision 
                       was found
        """
        raise NotImplementedError(self.insert_revid)


class RevisionInfoCache(object):

    def insert_revision(self, foreign_revid, mapping, revinfo, 
            original_mapping, stored_lhs_parent_revid):
        """Insert a revision to the cache.

        :param foreign_revid: Foreign revision id
        :param mapping: Mapping used
        :param revinfo: Tuple with (revno, revid, hidden)
        :param original_mapping: Original mapping used
        :param stored_lhs_parent_revid: Stored lhs parent revision
        """
        raise NotImplementedError(self.insert_revision)

    def get_revision(self, foreign_revid, mapping):
        """Get the revision metadata info for a (foreign_revid, mapping) tuple.

        :param foreign_revid: Foreign reviasion id
        :param mapping: Mapping
        :return: Tuple with (stored revno, revid, hidden), original_mapping, 
            stored_lhs_parent_revid
        """
        raise NotImplementedError(self.get_revision)

    def get_original_mapping(self, foreign_revid):
        """Find the original mapping for a revision.

        :param foreign_revid: Foreign revision id
        :return: Mapping object or None
        """
        raise NotImplementedError(self.get_original_mapping)
