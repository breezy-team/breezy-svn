# Copyright (C) 2005-2009 Jelmer Vernooij <jelmer@samba.org>
 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Fake versionedfiles implementation for Subversion."""

from bzrlib import osutils, urlutils
from bzrlib.revision import NULL_REVISION
from bzrlib.trace import warning
from bzrlib.versionedfile import FulltextContentFactory, VersionedFiles

from cStringIO import StringIO
import subvertpy

from bzrlib.plugins.svn.errors import convert_svn_error
from bzrlib.plugins.svn.fileids import idmap_reverse_lookup, idmap_lookup

_warned_experimental = False

def warn_stacking_experimental():
    global _warned_experimental
    if not _warned_experimental:
        warning("stacking support in bzr-svn is experimental.")
        _warned_experimental = True


class SvnTexts(VersionedFiles):
    """Subversion texts backend."""

    def __init__(self, repository):
        self.repository = repository

    def check(self, progressbar=None):
        return True

    def add_mpdiffs(self, records):
        raise NotImplementedError(self.add_mpdiffs)

    @convert_svn_error
    def get_record_stream(self, keys, ordering, include_delta_closure):
        warn_stacking_experimental()
        # TODO: there may be valid text revisions that only exist as 
        # ghosts in the repository itself. This function will 
        # not be able to report them.
        # TODO: Sort keys by file id and issue just one get_file_revs() call 
        # per file-id ?
        for (fileid, revid) in list(keys):
            revmeta, mapping = self.repository._get_revmeta(revid)
            map = self.repository.get_fileid_map(revmeta, mapping)
            path = idmap_reverse_lookup(map, mapping, fileid)
            try:
                stream = StringIO()
                self.repository.transport.get_file(urlutils.join(revmeta.branch_path, path).strip("/"), stream, revmeta.revnum)
                stream.seek(0)
                lines = stream.readlines()
            except subvertpy.SubversionException, (_, num):
                if num == subvertpy.ERR_FS_NOT_FILE:
                    lines = []
                else:
                    raise
            yield FulltextContentFactory((fileid, revid), None, 
                        sha1=osutils.sha_strings(lines),
                        text=''.join(lines))

    def _get_parent(self, fileid, revid):
        revmeta, mapping = self.repository._get_revmeta(revid)
        fileidmap = self.repository.get_fileid_map(revmeta, mapping)
        try:
            path = idmap_reverse_lookup(fileidmap, mapping, fileid)
        except KeyError:
            return None

        text_parents = revmeta.get_text_parents(mapping)
        if path in text_parents:
            return text_parents[path]

        # Not explicitly recorded - so just return the text revisions 
        # present in the parents of the mentioned revision.
        ret = []
        rev_parent_revids = revmeta.get_parent_ids(mapping)
        for revid in rev_parent_revids:
            if revid == NULL_REVISION:
                continue # Nothing exists in NULL_REVISION
            revmeta, mapping = self.repository._get_revmeta(revid)
            fileidmap = self.repository.get_fileid_map(revmeta, mapping)
            try:
                path = idmap_reverse_lookup(fileidmap, mapping, fileid)
            except KeyError:
                pass
            else:
                text_parent = idmap_lookup(fileidmap, mapping, path)[1]
                if text_parent not in ret:
                    ret.append(text_parent)

        if ret == []:
            return (NULL_REVISION,)
        else:
            return tuple(ret)

    def get_parent_map(self, keys):
        invs = {}

        # First, figure out the revision number/path
        ret = {}
        for (fileid, revid) in keys:
            ret[(fileid, revid)] = self._get_parent(fileid, revid)
        return ret

    # TODO: annotate, get_sha1s, iter_lines_added_or_present_in_keys, keys



