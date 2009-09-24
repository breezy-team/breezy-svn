#!/usr/bin/env python
# API Info for bzr-svn

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

bzr_plugin_name = "svn"

subvertpy_minimum_version = (0, 6, 1)

# versions ending in 'exp' mean experimental mappings
# versions ending in 'dev' mean development version
# versions ending in 'final' mean release (well tested, etc)
bzr_plugin_version = (1, 0, 0, 'final', 0)

bzr_commands = ["svn-import", "svn-layout"]

bzr_transports = ["svn://", "svn+"]

bzr_compatible_versions = [(1, x, 0) for x in [16, 17, 18]] + \
                          [(2, x, 0) for x in [0]]

bzr_minimum_version = bzr_compatible_versions[0]

bzr_maximum_version = bzr_compatible_versions[-1]

bzr_control_formats = {"Subversion":{'.svn/': None}}


