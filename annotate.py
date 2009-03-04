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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Annotate support."""

from cStringIO import StringIO
from subvertpy.delta import apply_txdelta_window

from bzrlib.annotate import reannotate

from bzrlib.plugins.svn import changes
from bzrlib.plugins.svn.fileids import idmap_lookup


class Annotater(object):

    def __init__(self, repository, mapping, fileid, revid):
        self._text = ""
        self._annotated = []
        self._mapping = mapping
        self._repository = repository
        self.fileid = fileid
        self._related_revs = {}
        (_, branch_path, revnum), mapping = self._repository.lookup_revision_id(revid)
        # FIXME: Cope with restarts
        for (revmeta, mapping) in self._repository._iter_reverse_revmeta_mapping_history(branch_path, revnum, 
                to_revnum=0, mapping=mapping):
            self._related_revs[revmeta.revnum] = revmeta, mapping

    def get_annotated(self):
        return self._annotated

    def check_file_revs(self, path, revnum):
        self._text = ""
        self._repository.transport.get_file_revs(path, -1, revnum, 
            self._handler)

    def _get_ids(self, path, rev, revprops):
        revmeta, mapping = self._related_revs[rev]
        revmeta._revprops = revprops
        path = path.strip("/")
        if not changes.path_is_child(revmeta.branch_path, path):
            raise KeyError(path)
        ip = path[len(revmeta.branch_path):].strip("/")
        idmap = self._repository.get_fileid_map(revmeta, mapping)
        return idmap_lookup(idmap, mapping, ip)

    def _handler(self, path, rev, revprops):
        try:
            fileid, revid, _ = self._get_ids(path, rev, revprops)
        except KeyError:
            # Not interested, doesn't appear to be related
            return None
        assert revid is not None
        stream = StringIO()
        def apply_window(window):
            if window is None:
                stream.seek(0)
                lines = stream.readlines()
                self._text = "".join(lines)
                if fileid == self.fileid:
                    # FIXME: Right hand side parents
                    self._annotated = reannotate([self._annotated], 
                        lines, revid)
                return
            stream.write(apply_txdelta_window(self._text, window))
        return apply_window


