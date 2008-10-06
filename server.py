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

from subvertpy.server import SVNServer, ServerBackend, ServerRepositoryBackend

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


class BzrServerBackend(ServerBackend):

    def open_repository(self, path):
        (branch, relpath) = Branch.open_containing(os.path.join(self.rootdir, location))
        return RepositoryBackend(branch), relpath
