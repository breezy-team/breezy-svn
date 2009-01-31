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

from bzrlib.plugins.svn.layout.standard import InverseTrunkLayout, TrunkLayout

class KDELayout(InverseTrunkLayout):
    """Layout for the KDE repository."""

    def __init__(self):
        InverseTrunkLayout.__init__(self, 1)


class ApacheLayout(TrunkLayout):
    """Layout for the Apache repository."""


