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

from __future__ import absolute_import

from breezy import (
    ui,
    urlutils,
    )
from breezy.errors import (
    RevisionNotPresent,
    )
from breezy.bzr.knit import (
    make_file_factory,
    )
from breezy.revision import (
    NULL_REVISION,
    )
from six import (
    text_type,
    )
from breezy.trace import (
    mutter,
    )
from breezy.bzr.versionedfile import (
    ConstantMapper,
    )

from . import (
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
        while base != u"":
            if u"/" in base:
                base = base.rsplit(u"/", 1)[0]
            else:
                base = u""
            create_revid = get(base)[2]
            if create_revid is None:
                raise AssertionError(
                    "Inconsistency; child %s appeared "
                    "while parent was never copied" % path)
            return (mapping.generate_file_id(create_revid, path),
                    mapping.revision_id_foreign_to_bzr(create_revid),
                    create_revid)
        raise KeyError("Unable to determine file id for %r" % path)


def idmap_reverse_lookup(idmap, mapping, fileid):
    """Lookup a file id in an idmap.

    :param idmap: The idmap to lookup in.
    :param mapping: Mapping
    :param fileid: The file id to look up
    :return: Path
    """
    assert type(fileid) is bytes
    if mapping.parseable_file_ids:
        try:
            (uuid, revnum, path) = mapping.parse_file_id(fileid)
        except errors.InvalidFileId:
            uuid = None
    else:
        uuid = None
    # Unfortunately, the map is the other way around
    for k in sorted(idmap.keys()):
        (v, ck, child_create_foreign_revid) = idmap[k]
        if v == fileid:
            return k
        if (child_create_foreign_revid is not None and
            child_create_foreign_revid[0] == uuid and
            child_create_foreign_revid[2] == revnum and
            path.startswith("%s/" % child_create_foreign_revid[1])):
            inv_p = path[len(child_create_foreign_revid[1]):].strip("/").decode("utf-8")
            foreign_revid = (uuid, child_create_foreign_revid[1], revnum)
            if mapping.generate_file_id(foreign_revid, inv_p) == fileid:
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
    for p, (action, copy_from) in changes.items():
        if not isinstance(p, text_type):
            raise TypeError(p)
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
        new_p = p[len(branch):].strip(u"/")
        if data[1] is not None:
            # Branch copy
            if new_p == u"":
                data = ('M', None)
            else:
                data = (data[0], (data[1], data[2]))
        else:
            data = (data[0], None)
        new_paths[new_p] = data
    return new_paths


FILEIDMAP_VERSION = 3
FILEID_MAP_SAVE_INTERVAL = 1000

def simple_apply_changes(new_file_id, changes):
    """Simple function that generates a dictionary with file id changes.

    Does not track renames. """
    delta = {}
    for p in sorted(changes.keys(), reverse=False):
        (action, copy_from) = changes[p]
        if not isinstance(p, text_type):
            raise TypeError(p)
        # Only generate new file ids for root if it's new
        if action in ('A', 'R') and (p != u"" or copy_from is None):
            delta[p] = new_file_id(p)
    return delta


class FileIdMap(object):
    """Map from file ids to names."""

    __slots__ = ()

    def has_fileid(self, fileid):
        """Does this file id appear in the map?"""
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

    __slots__ = ('data')

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
        for p, (action, copy_from) in changes.items():
            if action in ('D', 'R'):
                for xp in self.data.keys():
                    if ((p == xp or xp.startswith(u"%s/" % p)) and
                            xp not in delta):
                        del self.data[xp]

        for x in sorted(list(text_revisions.keys()) + list(delta.keys())):
            if not isinstance(x, text_type):
                raise TypeError(x)
            # Entry was added
            if (x in delta and (
                    x not in self.data or self.data[x][0] != delta[x])):
                if x in changes and changes[x][1] is not None:
                    # if this was a copy from somewhere else there can be
                    # implicit children
                    child_create_revid = foreign_revid
                else:
                    child_create_revid = None
                self.data[x] = (
                    delta[x], text_revisions.get(x, default_revid),
                    child_create_revid)
            else:
                # Entry already existed
                try:
                    prev_entry = self.lookup(mapping, x)
                except KeyError:
                    raise AssertionError(
                        "Unable to find old fileid for %s in %r" %
                        (x, foreign_revid))
                self.data[x] = (
                    prev_entry[0],
                    text_revisions.get(x, default_revid), prev_entry[2])

        if "" not in self.data:
            raise AssertionError(
                "root no longer exists after %r in %r" %
                (foreign_revid, self.data))

    def has_fileid(self, fileid):
        for fid, _ in self.data.values():
            if fid == fileid:
                return True
        return False

    def as_dict(self):
        return self.data

    def lookup(self, mapping, path):
        ret = idmap_lookup(self.data.__getitem__, mapping, path)
        if not isinstance(ret, tuple):
            raise TypeError(ret)
        return ret

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
        foreign_revid = revmeta.metarev.get_foreign_revid()

        def new_file_id(x):
            return mapping.generate_file_id(foreign_revid, x)

        idmap = self.apply_changes_fn(new_file_id, changes)
        idmap.update(revmeta.get_fileid_overrides(mapping))
        return idmap

    def update_idmap(self, map, revmeta, mapping):
        local_changes = get_local_changes(
            revmeta.metarev.paths, revmeta.metarev.branch_path)
        parentrevmeta = revmeta.get_lhs_parent_revmeta(mapping)
        if revmeta.get_lhs_parent_revid(mapping, parentrevmeta) == NULL_REVISION:
            # Special case - a NULL_REVISION officially doesn't have a root
            local_changes[u''] = ('A', None)
        idmap = self.get_idmap_delta(local_changes, revmeta, mapping)
        revid = revmeta.get_revision_id(mapping)
        text_revisions = determine_text_revisions(local_changes, revid,
                revmeta.get_text_revisions(mapping))
        map.apply_delta(text_revisions, idmap, local_changes, revid,
                          mapping, revmeta.metarev.get_foreign_revid())

    def get_map(self, foreign_revid, mapping):
        """Make sure the map is up to date until revnum."""
        (uuid, branch, revnum) = foreign_revid
        todo = []
        if mapping.is_branch("") and branch == "":
            map = DictFileIdMap({
                u"": (mapping.generate_file_id((uuid, u"", 0), u""),
                      mapping.revision_id_foreign_to_bzr((uuid, u"", 0)),
                      None)
                })
        else:
            map = DictFileIdMap({})

        if revnum == 0:
            assert branch == ""
            return map

        # No history -> empty map
        todo = list(self.repos._revmeta_provider._iter_reverse_revmeta_mapping_history(
            branch, revnum, to_revnum=0, mapping=mapping))
        with ui.ui_factory.nested_progress_bar() as pb:
            for i, (revmeta, hidden, mapping) in enumerate(reversed(todo)):
                pb.update('generating file id map', i, len(todo))
                if hidden:
                    continue
                self.update_idmap(map, revmeta, mapping)
        return map


class FileIdMapCache(object):

    __slots__ = ('idmap_knit')

    def __init__(self, cache_transport):
        mapper = ConstantMapper("fileidmap-v%d" % FILEIDMAP_VERSION)
        self.idmap_knit = make_file_factory(True, mapper)(cache_transport)

    def save(self, revid, parent_revids, _map):
        mutter('saving file id map for %r', revid)

        lines = []

        for path in sorted(_map.keys()):
            (id, changed_revid, created_revid) = _map[path]
            if not isinstance(path, text_type):
                raise TypeError(path)
            if not isinstance(id, bytes):
                raise TypeError(id)
            if not isinstance(changed_revid, bytes):
                raise TypeError(changed_revid)
            assert created_revid is None or isinstance(created_revid, tuple)
            if created_revid is None:
                optional_child_create_revid = b""
            else:
                optional_child_create_revid = b"\t%s:%d:%s" % (
                    created_revid[0], created_revid[2], created_revid[1].encode('utf-8'))
            lines.append(b"%s\t%s\t%s%s\n" % (
                urlutils.quote(path.encode("utf-8")).encode('ascii'),
                urlutils.quote(id).encode('ascii'),
                urlutils.quote(changed_revid).encode('ascii'),
                optional_child_create_revid))

        self.idmap_knit.add_lines((revid,), [(r, ) for r in parent_revids],
                                  lines)

    def load(self, revid):
        map = {}
        for ((from_revid,), line) in self.idmap_knit.annotate((revid,)):
            entries = line.rstrip(b"\n").split(b"\t", 4)
            if len(entries) == 3:
                (filename, id, changed_revid) = entries
                child_create_revid = None
            else:
                (filename, id, changed_revid, child_create_text) = entries
                (uuid, revnum, bp) = child_create_text.split(b":", 3)
                child_create_revid = (uuid, bp, int(revnum))
            map[urlutils.unquote(filename).decode("utf-8")] = (
                urlutils.unquote(id), urlutils.unquote(changed_revid),
                child_create_revid)
            assert isinstance(map[urlutils.unquote(filename).decode("utf-8")][0], str)

        return map


class CachingFileIdMapStore(object):
    """A file id map that uses a cache."""

    def __init__(self, cache, actual):
        self.cache = cache
        self.actual = actual
        self.repos = actual.repos
        self.get_idmap_delta = actual.get_idmap_delta

    def get_map(self, foreign_revid, mapping):
        """Make sure the map is up to date until revnum."""
        (uuid, branch, revnum) = foreign_revid
        todo = []
        next_parent_revs = []

        # No history -> empty map
        with ui.ui_factory.nested_progress_bar() as pb:
            for revmeta, hidden, mapping in self.repos._revmeta_provider._iter_reverse_revmeta_mapping_history(branch, revnum, to_revnum=0, mapping=mapping):
                pb.update("fetching changes for file ids",
                    revnum-revmeta.metarev.revnum, revnum)
                if hidden:
                    continue
                revid = revmeta.get_revision_id(mapping)
                try:
                    map = DictFileIdMap(self.cache.load(revid))
                    # found the nearest cached map
                    next_parent_revs = [revid]
                    break
                except RevisionNotPresent:
                    todo.append((revmeta, mapping))

        # target revision was present
        if len(todo) == 0:
            return self.actual.get_map((uuid, branch, revnum), mapping)

        if len(next_parent_revs) == 0:
            if mapping.is_branch("") and branch == "":
                map = DictFileIdMap({
                    u"": (mapping.generate_file_id((uuid, u"", 0), u""),
                          NULL_REVISION, None)})
            else:
                map = DictFileIdMap({})

        with ui.ui_factory.nested_progress_bar() as pb:
            for i, (revmeta, mapping) in enumerate(reversed(todo)):
                pb.update('generating file id map', i, len(todo))
                revid = revmeta.get_revision_id(mapping)
                self.actual.update_idmap(map, revmeta, mapping)
                parent_revs = next_parent_revs
                if (revnum % FILEID_MAP_SAVE_INTERVAL == 0 or
                    revnum == revmeta.metarev.revnum):
                    self.cache.save(revid, parent_revs, map.as_dict())
                next_parent_revs = [revid]
        return map

