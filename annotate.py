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

from __future__ import absolute_import

import subvertpy

from cStringIO import (
    StringIO,
    )
from subvertpy.delta import (
    apply_txdelta_window,
    )

from breezy import (
    urlutils,
    )
from breezy.annotate import (
    reannotate,
    )
from breezy.errors import (
    NoSuchId,
    )

from breezy.plugins.svn import (
    changes,
    )


class Annotater(object):

    def __init__(self, repository, fileid):
        self._text = ""
        self._annotated = []
        self._repository = repository
        self.fileid = fileid
        self._related_revs = {}

    def get_annotated(self):
        return self._annotated

    def check_file_revs(self, revid, branch_path, revnum, mapping, relpath=None):
        self._annotated = []
        for (revmeta, hidden, mapping) in self._repository._revmeta_provider._iter_reverse_revmeta_mapping_history(
                branch_path, revnum, to_revnum=0, mapping=mapping):
            if hidden:
                continue
            self._related_revs[revmeta.metarev.revnum] = revmeta, mapping
        self._text = ""
        if relpath is None:
            tree = self._repository.revision_tree(revid)
            try:
                relpath = tree.id2path(self.fileid)
            except NoSuchId:
                return []
        path = urlutils.join(branch_path, relpath.encode("utf-8")).strip("/")
        try:
            self._repository.transport.get_file_revs(path, -1, revnum,
                self._handler, include_merged_revisions=True)
        except subvertpy.SubversionException, (msg, num):
            if num == subvertpy.ERR_FS_NOT_FILE:
                return []
            raise
        return self._annotated

    def _get_ids(self, path, rev, revprops):
        revmeta, mapping = self._related_revs[rev]
        revmeta.metarev._revprops = revprops
        path = path.strip("/")
        if not changes.path_is_child(revmeta.metarev.branch_path, path):
            raise KeyError(path)
        ip = path[len(revmeta.metarev.branch_path):].strip("/")
        idmap = self._repository.get_fileid_map(revmeta, mapping)
        return idmap.lookup(mapping, ip)

    def check_file(self, lines, revid, parent_lines):
        return reannotate(parent_lines, lines, revid)

    def _handler(self, path, rev, revprops, result_of_merge=None):
        try:
            fileid, revid, _ = self._get_ids(path, rev, revprops)
            assert revid is not None
        except KeyError:
            # Related file in Subversion but not in Bazaar
            # We still apply the delta since we'll need the fulltext later
            fileid = None
        stream = StringIO()
        def apply_window(window):
            if window is None:
                stream.seek(0)
                lines = stream.readlines()
                self._text = "".join(lines)
                if fileid == self.fileid:
                    self._annotated = self.check_file(lines, revid, [self._annotated])
                return
            stream.write(apply_txdelta_window(self._text, window))
        return apply_window


