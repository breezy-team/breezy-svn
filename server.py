# Copyright (C) 2006-2007 Jelmer Vernooij <jelmer@samba.org>

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
"""Subversion server implementation."""

from bzrlib.branch import Branch

from subvertpy import NODE_DIR
from subvertpy.server import SVNServer, ServerBackend, ServerRepositoryBackend
from subvertpy.properties import time_to_cstring

import os, time

def determine_changed_paths(repository, branch_path, rev, revno):
    def fixpath(p):
        return "%s/%s" % (branch_path, p.encode("utf-8"))
    changes = {}
    changes[branch_path] = ("M", None, -1) # Always changes
    delta = repository.get_revision_delta(rev.revision_id)
    for (path, id, kind) in delta.added:
        changes[fixpath(path)] = ("A", None, -1)
    for (path, id, kind) in delta.removed:
        changes[fixpath(path)] = ("D", None, -1)
    for (oldpath, newpath, id, kind, text_modified, meta_modified) in delta.renamed:
        changes[fixpath(newpath)] = ("A", fixpath(oldpath), revno-1)
        changes[fixpath(oldpath)] = ("D", None, -1)
    for (path, id, kind, text_modified, meta_modified) in delta.modified:
        changes[fixpath(path)] = ("M", None, -1)
    return changes


class RepositoryBackend(ServerRepositoryBackend):

    def __init__(self, branch):
        self.branch = branch

    def get_uuid(self):
        config = self.branch.get_config()
        uuid = config.get_user_option('svn_uuid')
        if uuid is None:
            import uuid
            uuid = uuid.uuid4()
            config.set_user_option('svn_uuid', uuid)
        return str(uuid)

    def get_latest_revnum(self):
        return self.branch.revno()

    def log(self, send_revision, target_path, start_rev, end_rev, report_changed_paths,
            strict_node, limit):
        i = 0
        revno = start_rev
        self.branch.repository.lock_read()
        try:
            # FIXME: check whether start_rev and end_rev actually exist
            while revno != end_rev:
                #TODO: Honor target_path, strict_node, changed_paths
                if end_rev > revno:
                    revno+=1
                else:
                    revno-=1
                if limit != 0 and i == limit:
                    break
                if revno > 0:
                    rev = self.branch.repository.get_revision(self.branch.get_rev_id(revno))
                    if report_changed_paths:
                        changes = determine_changed_paths(self.branch.repository, "/trunk", rev, revno)
                    else:
                        changes = None
                    send_revision(revno, 
                            rev.committer, time.strftime("%Y-%m-%dT%H:%M:%S.00000Z", time.gmtime(rev.timestamp)),
                            rev.message, changed_paths=changes)
        finally:
            self.branch.repository.unlock()

    def rev_proplist(self, revnum):
        return {}

    def update(self, editor, revnum, target_path, recurse=True):
        editor.set_target_revision(revnum)
        root = editor.open_root()
        # FIXME
        root.close()
        editor.close()

    def check_path(self, path, revnum):
        return NODE_DIR

    def get_locations(self, path, peg_revnum, revnums):
        if path.strip() in ("trunk", ""):
            return dict([(rev, path) for rev in revnums])
        raise NotImplementedError
    

class BzrServerBackend(ServerBackend):

    def __init__(self, rootdir):
        self.rootdir = rootdir

    def open_repository(self, path):
        (branch, relpath) = Branch.open_containing(os.path.join(self.rootdir, path))
        return RepositoryBackend(branch), relpath
