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


"""Generation of file-ids."""


try:
    from collections import defaultdict
except ImportError:
    from bzrlib.plugins.svn.pycompat import defaultdict

import urllib

from bzrlib import (
    ui,
    )
from bzrlib.errors import (
    RevisionNotPresent,
    )
from bzrlib.knit import (
    make_file_factory,
    )
from bzrlib.revision import (
    NULL_REVISION,
    )
from bzrlib.trace import (
    mutter,
    )
from bzrlib.versionedfile import (
    ConstantMapper,
    )

from bzrlib.plugins.svn import (
    changes,
    errors,
    )

# idmap: dictionary mapping unicode paths to tuples with file id,
#   revision id and the foreign_revid it was introduced in, if it
#   can have implicit children (None otherwise)
# idmap delta: dictionary mapping unicode paths to new file id assignments
# text revision map: dictionary mapping unicode paths to text revisions (usually revision ids)
# implicit children: children that don't show up in the svn log when a copy is done, e.g.:
#
# = rev1
#    A /foo
#    A /foo/bar
# = rev 2
#    A /bla (from /foo:1)

def idmap_lookup(get, mapping, path):
    """Lookup a path in an idmap.

    :param get: Function to access idmap
    :param mapping: Mapping
    :param path: Path to look up
    :return: Tuple with file id and text revision
    """
    try:
        return get(path)
    except KeyError:
        base = path
        while base != "":
            if u"/" in base:
                base = base.rsplit("/", 1)[0]
            else:
                base = u""
            try:
                create_revid = get(base)[2]
                if create_revid is None:
                    raise AssertionError("Inconsistency; child %s appeared while parent was never copied" % path)
                return (mapping.generate_file_id(create_revid, path),
                        mapping.revision_id_foreign_to_bzr(create_revid),
                        create_revid)
            except:
                pass
        raise KeyError("Unable to determine file id for %r" % path)


def idmap_reverse_lookup(idmap, mapping, fileid):
    """Lookup a file id in an idmap.

    :param idmap: The idmap to lookup in.
    :param mapping: Mapping
    :param fileid: The file id to look up
    :return: Path
    """
    if mapping.parseable_file_ids:
        try:
            (uuid, revnum, path) = mapping.parse_file_id(fileid)
        except errors.InvalidFileId:
            uuid = None
    else:
        uuid = None
    # Unfortunately, the map is the other way around
    for k in sorted(idmap.iterkeys()):
        (v, ck, child_create_foreign_revid) = idmap[k]
        if v == fileid:
            return k
        if (child_create_foreign_revid is not None and
            child_create_foreign_revid[0] == uuid and
            child_create_foreign_revid[2] == revnum and
            path.startswith("%s/" % child_create_foreign_revid[1])):
            inv_p = path[len(child_create_foreign_revid[1]):].strip("/").decode("utf-8")
            if mapping.generate_file_id((uuid, child_create_foreign_revid[1], revnum), inv_p) == fileid:
                return inv_p

    raise KeyError(fileid)


def determine_text_revisions(changes, default_revid, specific_revids):
    """Create a text revision map.

    :param changes: Local changes dictionary
    :param default_revid: Default revision id, if none is explicitly specified
    :param specific_revids: Dictionary with explicit text revisions to use
    :return: text revision map
    """
    ret = {}
    ret.update(specific_revids)
    for p, (action, copy_from) in changes.iteritems():
        assert isinstance(p, unicode)
        # The root changes often because of file properties, so we don't
        # consider it really changing.
        if action in ('A', 'R', 'M') and p not in ret and p != u"":
            ret[p] = default_revid
    return ret


def get_local_changes(paths, branch):
    """Obtain all of the changes relative to a particular path
    (usually a branch path).

    :param paths: Changes
    :param branch: Path under which to select changes
    """
    new_paths = {}
    for p in sorted(paths.keys(), reverse=False):
        if not changes.path_is_child(branch, p):
            continue
        data = paths[p]
        new_p = p[len(branch):].strip("/")
        if data[1] is not None:
            # Branch copy
            if new_p == "":
                data = ('M', None)
            else:
                data = (data[0], (data[1], data[2]))
        else:
            data = (data[0], None)
        new_paths[new_p.decode("utf-8")] = data
    return new_paths


FILEIDMAP_VERSION = 3
FILEID_MAP_SAVE_INTERVAL = 1000

def simple_apply_changes(new_file_id, changes):
    """Simple function that generates a dictionary with file id changes.

    Does not track renames. """
    delta = {}
    for p in sorted(changes.keys(), reverse=False):
        (action, copy_from) = changes[p]
        assert isinstance(p, unicode)
        # Only generate new file ids for root if it's new
        if action in ('A', 'R') and (p != u"" or copy_from is None):
            delta[p] = new_file_id(p)
    return delta


class FileIdMap(object):

    def has_fileid(self, fileid):
        raise NotImplementedError(self.has_fileid)

    def as_dict(self):
        raise NotImplementedError(self.as_dict)

    def lookup(self, mapping, path):
        raise NotImplementedError(self.lookup)

    def reverse_lookup(self, mapping, fileid):
        raise NotImplementedError(self.reverse_lookup)

    def apply_delta(self, text_revisions, delta, changes, default_revid,
                      mapping, foreign_revid):
        raise NotImplementedError(self.apply_delta)


class DictFileIdMap(FileIdMap):

    def __init__(self, data):
        self.data = data

    def apply_delta(self, text_revisions, delta, changes, default_revid,
                      mapping, foreign_revid):
        """Update a file id map.

        :param text_revisions: Text revisions for the map
        :param delta: Id map delta.
        :param changes: Changes for the revision in question.
        """
        assert "" in self.data or self.data == {}, "'' missing in %r" % self.data
        for p, (action, copy_from) in changes.iteritems():
            if action in ('D', 'R'):
                for xp in self.data.keys():
                    if (p == xp or xp.startswith(u"%s/" % p)) and not xp in delta:
                        del self.data[xp]

        for x in sorted(text_revisions.keys() + delta.keys()):
            assert isinstance(x, unicode)
            if x in delta and (not x in self.data or self.data[x][0] != delta[x]):
                if x in changes and changes[x][1] is not None:
                    # if this was a copy from somewhere else there can be
                    # implicit children
                    child_create_revid = foreign_revid
                else:
                    child_create_revid = None
                self.data[x] = (delta[x], text_revisions.get(x) or default_revid, child_create_revid)
            else:
                try:
                    prev_entry = self.lookup(mapping, x)
                except KeyError:
                    raise AssertionError("Unable to find old fileid for %s in %r" % (x, foreign_revid))
                self.data[x] = (prev_entry[0], text_revisions.get(x) or default_revid, prev_entry[2])

        if not "" in self.data:
            raise AssertionError("root no longer exists after %r in %r" % (foreign_revid, self.data))

    def has_fileid(self, fileid):
        for fid, _ in self.data.itervalues():
            if fid == fileid:
                return True
        return False

    def as_dict(self):
        return self.data

    def lookup(self, mapping, path):
        return idmap_lookup(self.data.__getitem__, mapping, path)

    def reverse_lookup(self, mapping, fileid):
        return idmap_reverse_lookup(self.data, mapping, fileid)


class FileIdMapStore(object):
    """File id store.

    Keeps a map

    revnum -> branch -> path -> fileid
    """
    def __init__(self, apply_changes_fn, repos):
        self.apply_changes_fn = apply_changes_fn
        self.repos = repos

    def get_idmap_delta(self, changes, revmeta, mapping):
        """Change file id map to incorporate specified changes.

        :param changes: Dictionary with (local) changes
        :param revmeta: RevisionMetadata object for revision with changes
        :param mapping: Mapping
        """
        foreign_revid = revmeta.get_foreign_revid()
        def new_file_id(x):
            return mapping.generate_file_id(foreign_revid, x)

        idmap = self.apply_changes_fn(new_file_id, changes)
        idmap.update(revmeta.get_fileid_overrides(mapping))
        return idmap

    def update_idmap(self, map, revmeta, mapping):
        local_changes = get_local_changes(revmeta.get_paths(), revmeta.branch_path)
        idmap = self.get_idmap_delta(local_changes, revmeta, mapping)
        revid = revmeta.get_revision_id(mapping)
        text_revisions = determine_text_revisions(local_changes, revid,
                revmeta.get_text_revisions(mapping))
        map.apply_delta(text_revisions, idmap, local_changes, revid,
                          mapping, revmeta.get_foreign_revid())

    def get_map(self, foreign_revid, mapping):
        """Make sure the map is up to date until revnum."""
        (uuid, branch, revnum) = foreign_revid
        todo = []
        next_parent_revs = []
        if mapping.is_branch(""):
            map = DictFileIdMap({u"": (mapping.generate_file_id((uuid, "", 0), u""), mapping.revision_id_foreign_to_bzr((uuid, "", 0)), None)})
        else:
            map = DictFileIdMap({})

        if revnum == 0:
            assert branch == ""
            return map

        # No history -> empty map
        todo = list(self.repos._iter_reverse_revmeta_mapping_history(branch, revnum, to_revnum=0, mapping=mapping))
        pb = ui.ui_factory.nested_progress_bar()
        try:
            for i, (revmeta, mapping) in enumerate(reversed(todo)):
                pb.update('generating file id map', i, len(todo))
                if revmeta.is_hidden(mapping):
                    continue
                self.update_idmap(map, revmeta, mapping)
        finally:
            pb.finished()
        return map



class FileIdMapCache(object):

    def __init__(self, cache_transport):
        mapper = ConstantMapper("fileidmap-v%d" % FILEIDMAP_VERSION)
        self.idmap_knit = make_file_factory(True, mapper)(cache_transport)

    def save(self, revid, parent_revids, _map):
        mutter('saving file id map for %r', revid)

        lines = []

        for path in sorted(_map.keys()):
            (id, changed_revid, created_revid) = _map[path]
            assert isinstance(path, unicode)
            assert isinstance(id, str)
            assert isinstance(changed_revid, str)
            assert created_revid is None or isinstance(created_revid, tuple)
            if created_revid is None:
                optional_child_create_revid = ""
            else:
                optional_child_create_revid = "\t%s:%d:%s" % (created_revid[0], created_revid[2], created_revid[1])
            lines.append("%s\t%s\t%s%s\n" % (urllib.quote(path.encode("utf-8")),
                                           urllib.quote(id),
                                           urllib.quote(changed_revid),
                                           optional_child_create_revid))

        self.idmap_knit.add_lines((revid,), [(r, ) for r in parent_revids],
                                  lines)

    def load(self, revid):
        map = {}
        for ((from_revid,), line) in self.idmap_knit.annotate((revid,)):
            entries = line.rstrip("\n").split("\t", 4)
            if len(entries) == 3:
                (filename, id, changed_revid) = entries
                child_create_revid = None
            else:
                (filename, id, changed_revid, child_create_text) = entries
                (uuid, revnum, bp) = child_create_text.split(":", 3)
                child_create_revid = (uuid, bp, int(revnum))
            map[urllib.unquote(filename).decode("utf-8")] = (urllib.unquote(id), urllib.unquote(changed_revid), child_create_revid)
            assert isinstance(map[urllib.unquote(filename).decode("utf-8")][0], str)

        return map


class CachingFileIdMapStore(object):
    """A file id map that uses a cache."""

    def __init__(self, cache, actual):
        self.cache = cache
        self.actual = actual
        self.repos = actual.repos
        self.get_idmap_delta = actual.get_idmap_delta

    def get_map(self, (uuid, branch, revnum), mapping):
        """Make sure the map is up to date until revnum."""
        if revnum == 0:
            return self.actual.get_map((uuid, branch, revnum), mapping)
        todo = []
        next_parent_revs = []

        # No history -> empty map
        try:
            pb = ui.ui_factory.nested_progress_bar()
            for revmeta, mapping in self.repos._iter_reverse_revmeta_mapping_history(branch, revnum, to_revnum=0, mapping=mapping):
                pb.update("fetching changes for file ids", revnum-revmeta.revnum, revnum)
                if revmeta.is_hidden(mapping):
                    continue
                revid = revmeta.get_revision_id(mapping)
                try:
                    map = DictFileIdMap(self.cache.load(revid))
                    # found the nearest cached map
                    next_parent_revs = [revid]
                    break
                except RevisionNotPresent:
                    todo.append((revmeta, mapping))
        finally:
            pb.finished()

        # target revision was present
        if len(todo) == 0:
            return map

        if len(next_parent_revs) == 0:
            if mapping.is_branch(""):
                map = DictFileIdMap({u"": (mapping.generate_file_id((uuid, "", 0), u""), NULL_REVISION, None)})
            else:
                map = DictFileIdMap({})

        pb = ui.ui_factory.nested_progress_bar()

        try:
            for i, (revmeta, mapping) in enumerate(reversed(todo)):
                pb.update('generating file id map', i, len(todo))
                revid = revmeta.get_revision_id(mapping)
                self.actual.update_idmap(map, revmeta, mapping)
                parent_revs = next_parent_revs
                if revnum % FILEID_MAP_SAVE_INTERVAL == 0 or revnum == revmeta.revnum:
                    self.cache.save(revid, parent_revs, map.as_dict())
                next_parent_revs = [revid]
        finally:
            pb.finished()
        return map

