# Copyright (C) 2005-2007 Jelmer Vernooij <jelmer@samba.org>
 
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

from bzrlib.errors import InvalidRevisionId

from bzrlib.plugins.svn import mapping
from bzrlib.plugins.svn.foreign import ForeignRevision

def show_subversion_properties(rev):
    """Custom log displayer for Subversion revisions.

    :param rev: Revision object.
    """
    data = None
    ret = {}
    if isinstance(rev, ForeignRevision):
        return rev.mapping.show_foreign_revid(rev.foreign_revid)
    else:
        try:
            foreign_revid, mapp = mapping.mapping_registry.parse_revision_id(rev.revision_id)
        except InvalidRevisionId:
            pass
        else:
            return mapp.show_foreign_revid(foreign_revid)

    return {}


