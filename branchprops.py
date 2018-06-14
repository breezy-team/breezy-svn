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


"""Branch property access and caching."""

from __future__ import absolute_import

from subvertpy import (
        ERR_FS_NO_SUCH_REVISION,
        SubversionException,
        )

from breezy.errors import (
        NoSuchRevision,
        )

from breezy.plugins.svn import (
        util,
        )


class PathPropertyProvider(object):

    def __init__(self, log):
        self.log = log

    def get_properties(self, path, revnum):
        """Obtain all the directory properties set on a path/revnum pair.

        :param path: Subversion path
        :param revnum: Subversion revision number
        :return: Dictionary with properties
        """
        path = path.lstrip("/")
        return util.lazy_dict({}, self._real_get_properties, path, revnum)

    def _real_get_properties(self, path, revnum):
        try:
            (_, _, props) = self.log._transport.get_dir(path, revnum)
        except SubversionException as e:
            msg, num = e.args
            if num == ERR_FS_NO_SUCH_REVISION:
                raise NoSuchRevision(self, revnum)
            raise
        return props
