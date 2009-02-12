# Copyright (C) 2006-2009 Jelmer Vernooij <jelmer@samba.org>

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

"""Revision id generation and caching."""

from bzrlib import ui
from bzrlib.errors import InvalidRevisionId, NoSuchRevision
from bzrlib.lru_cache import LRUCache

from bzrlib.plugins.svn.cache import CacheTable
from bzrlib.plugins.svn.errors import (
        InvalidBzrSvnRevision,
        InvalidPropertyValue,
        )
from bzrlib.plugins.svn.mapping import (
        BzrSvnMapping,
        SVN_PROP_BZR_REVISION_ID,
        find_new_lines,
        is_bzr_revision_revprops,
        mapping_registry,
        parse_revid_property,
        )
from bzrlib.plugins.svn.mapping3.scheme import UnknownBranchingScheme

import subvertpy
from subvertpy import ERR_FS_NOT_DIRECTORY

class RevidMap(object):
    
    def __init__(self, repos):
        self.repos = repos

    def get_branch_revnum(self, revid, layout, project=None):
        """Find the (branch, revnum) tuple for a revision id.
        
        :return: Tuple with foreign revision id and mapping.
        """
        last_revnum = self.repos.get_latest_revnum()
        fileprops_to_revnum = last_revnum
        for entry_revid, branch, revnum, mapping in self.discover_revprop_revids(layout, last_revnum, 0):
            if revid == entry_revid:
                return (self.repos.uuid, branch, revnum), mapping
            fileprops_to_revnum = min(fileprops_to_revnum, revnum)

        for entry_revid, branch, min_revno, max_revno, mapping in self.discover_fileprop_revids(layout, 0, fileprops_to_revnum, project):
            if revid == entry_revid:
                (foreign_revid, mapping_name) = self.bisect_revid_revnum(revid, branch, min_revno, max_revno)
                return (foreign_revid, mapping_name)
        raise NoSuchRevision(self, revid)

    def discover_revprop_revids(self, layout, from_revnum, to_revnum, pb=None):
        """Discover bzr-svn revision properties between from_revnum and to_revnum.

        :return: First revision number on which a revision property was found, or None
        """
        if self.repos.transport.has_capability("log-revprops") != True:
            return
        for revmeta in self.repos._revmeta_provider.iter_all_revisions(layout, None, from_revnum, to_revnum):
            if pb is not None:
                pb.update("discovering revprop revisions", from_revnum-revmeta.revnum, from_revnum-to_revnum)
            if is_bzr_revision_revprops(revmeta.get_revprops()):
                mapping = revmeta.get_original_mapping()
                assert mapping is not None
                revid = revmeta.get_revision_id(mapping)
                if revid is not None:
                    yield (revid, mapping.get_branch_root(revmeta.get_revprops()).strip("/"), revmeta.revnum, mapping)

    def discover_fileprop_revids(self, layout, from_revnum, to_revnum, project=None):
        reuse_policy = self.repos.get_config().get_reuse_revisions()
        assert reuse_policy in ("other-branches", "removed-branches", "none") 
        check_removed = (reuse_policy == "removed-branches")
        for (branch, revno, exists) in self.repos.find_fileprop_paths(layout, from_revnum, to_revnum, project, check_removed=check_removed):
            assert isinstance(branch, str)
            assert isinstance(revno, int)
            # Look at their bzr:revision-id-vX
            revids = set()
            try:
                revmeta = self.repos._revmeta_provider.lookup_revision(branch, self.repos._log.find_latest_change(branch, revno))
                if revmeta.consider_bzr_fileprops():
                    for revid, bzr_revno, mapping_name in revmeta.get_roundtrip_ancestor_revids():
                        revids.add(((bzr_revno, revid), mapping_name))
            except subvertpy.SubversionException, (_, ERR_FS_NOT_DIRECTORY):
                continue

            # If there are any new entries that are not yet in the cache, 
            # add them
            for ((entry_revno, entry_revid), mapping_name) in revids:
                try:
                    yield (entry_revid, branch, 0, revno, mapping_registry.parse_mapping_name("svn-" + mapping_name))
                except UnknownBranchingScheme:
                    pass

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

    def __init__(self, actual, cachedb=None):
        self.cache = RevisionIdMapCache(cachedb)
        self.actual = actual
        self.revid_seen = set()

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
                for entry_revid, branch, revnum, mapping in self.actual.discover_revprop_revids(layout, last_revnum, last_checked, pb=pb):
                    fileprops_to_revnum = min(fileprops_to_revnum, revnum)
                    if entry_revid == revid:
                        found = (branch, revnum, revnum, mapping)
                    if entry_revid not in self.revid_seen:
                        self.cache.insert_revid(entry_revid, branch, revnum, revnum, mapping.name)
                        self.revid_seen.add(entry_revid)
            finally:
                pb.finished()
            for entry_revid, branch, min_revno, max_revno, mapping in self.actual.discover_fileprop_revids(layout, last_checked, fileprops_to_revnum, project):
                min_revno = max(last_checked, min_revno)
                if entry_revid == revid:
                    found = (branch, min_revno, max_revno, mapping)
                if entry_revid not in self.revid_seen:
                    self.cache.insert_revid(entry_revid, branch, min_revno, max_revno, mapping.name)
                    self.revid_seen.add(entry_revid)
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
        self.cache.insert_revid(revid, branch_path, revnum, revnum, mapping.name)
        return (uuid, branch_path, revnum), mapping


class RevisionIdMapCache(CacheTable):
    """Revision id mapping store. 

    Stores mapping from revid -> (path, revnum, mapping)
    """
    def _create_table(self):
        self.cachedb.executescript("""
        create table if not exists revmap (revid text, path text, min_revnum integer, max_revnum integer, mapping text);
        create index if not exists revid on revmap (revid);
        create unique index if not exists revid_path_mapping on revmap (revid, path, mapping);
        drop index if exists lookup_branch_revnum;
        create index if not exists lookup_branch_revnum_non_unique on revmap (max_revnum, min_revnum, path, mapping);
        create table if not exists revids_seen (layout text, max_revnum int);
        create unique index if not exists layout on revids_seen (layout);
        """)
        # Revisions ids are quite expensive
        self._commit_interval = 100

    def set_last_revnum_checked(self, layout, revnum):
        """Remember the latest revision number that has been checked
        for a particular layout.

        :param layout: Repository layout.
        :param revnum: Revision number.
        """
        self.cachedb.execute("replace into revids_seen (layout, max_revnum) VALUES (?, ?)", (layout, revnum))
        self.commit_conditionally()

    def last_revnum_checked(self, layout):
        """Retrieve the latest revision number that has been checked 
        for revision ids for a particular layout.

        :param layout: Repository layout.
        :return: Last revision number checked or 0.
        """
        self.mutter("last revnum checked %r", layout)
        ret = self.cachedb.execute(
            "select max_revnum from revids_seen where layout = ?", (layout,)).fetchone()
        if ret is None:
            return 0
        return int(ret[0])
    
    def lookup_revid(self, revid):
        """Lookup the details for a particular revision id.

        :param revid: Revision id.
        :return: Tuple with path inside repository, minimum revision number, maximum revision number and 
            mapping.
        """
        assert isinstance(revid, str)
        self.mutter("lookup revid %r", revid)
        ret = self.cachedb.execute(
            "select path, min_revnum, max_revnum, mapping from revmap where revid=? order by abs(min_revnum-max_revnum) asc", (revid,)).fetchone()
        if ret is None:
            raise NoSuchRevision(self, revid)
        (path, min_revnum, max_revnum, mapping) = (ret[0].encode("utf-8"), int(ret[1]), int(ret[2]), ret[3].encode("utf-8"))
        if min_revnum > max_revnum:
            return (path, max_revnum, min_revnum, mapping)
        else:
            return (path, min_revnum, max_revnum, mapping)

    def lookup_branch_revnum(self, revnum, path, mapping):
        """Lookup a revision by revision number, branch path and mapping.

        :param revnum: Subversion revision number.
        :param path: Subversion branch path.
        :param mapping: Mapping
        """
        assert isinstance(revnum, int)
        assert isinstance(path, str)
        assert isinstance(mapping, str)
        row = self.cachedb.execute(
                "select revid from revmap where max_revnum=? and min_revnum=? and path=? and mapping=?", (revnum, revnum, path, mapping)).fetchone()
        if row is not None:
            ret = str(row[0])
        else:
            ret = None
        self.mutter("lookup branch,revnum %r:%r -> %r", path, revnum, ret)
        return ret

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
        assert revid is not None and revid != ""
        assert isinstance(mapping, str)
        assert isinstance(branch, str)
        assert isinstance(min_revnum, int) and isinstance(max_revnum, int)
        assert min_revnum <= max_revnum
        self.mutter("insert revid %r:%r-%r -> %r", branch, min_revnum, max_revnum, revid)
        if min_revnum == max_revnum:
            cursor = self.cachedb.execute(
                "update revmap set min_revnum = ?, max_revnum = ? WHERE revid=? AND path=? AND mapping=?",
                (min_revnum, max_revnum, revid, branch, mapping))
        else:
            cursor = self.cachedb.execute(
                "update revmap set min_revnum = MAX(min_revnum,?), max_revnum = MIN(max_revnum, ?) WHERE revid=? AND path=? AND mapping=?",
                (min_revnum, max_revnum, revid, branch, mapping))
        if cursor.rowcount == 0:
            self.cachedb.execute(
                "insert into revmap (revid,path,min_revnum,max_revnum,mapping) VALUES (?,?,?,?,?)",
                (revid, branch, min_revnum, max_revnum, mapping))


class RevisionInfoCache(CacheTable):

    def _create_table(self):
        self.cachedb.executescript("""
        create table if not exists revinfo (revid text, path text, revnum integer, mapping text);
        create unique index if not exists revid_path_mapping on revmap (revnum, path, mapping);
        """)
        # Revisions ids are quite expensive
        self._commit_interval = 100

    def insert_revision(self, foreign_revid, mapping, revid, revno, hidden, original_mapping, stored_lhs_parent_revid):
        pass # FIXME

    def get_revision(self, foreign_revid, mapping):
        # Will raise KeyError if not present
        # returns tuple with (revid, revno, hidden, original_mapping, stored_lhs_parent_revid)
        raise KeyError((foreign_revid, mapping))

