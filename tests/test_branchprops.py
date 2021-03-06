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

"""Branch property access tests."""

from ..branchprops import (
    PathPropertyProvider,
    )
from ..logwalker import (
    LogWalker,
    )
from ..transport import (
    SvnRaTransport,
    )
from . import (
    SubversionTestCase,
    )


class TestBranchProps(SubversionTestCase):

    def setUp(self):
        super(TestBranchProps, self).setUp()

    def get_log_walker(self, transport):
        return LogWalker(transport=transport)

    def test_get_old_properties(self):
        repos_url = self.make_svn_repository('d')

        dc = self.get_commit_editor(repos_url)
        dc.change_prop("myprop", "data")
        dc.close()

        dc = self.get_commit_editor(repos_url)
        dc.add_file("foo").modify()
        dc.close()

        logwalk = self.get_log_walker(transport=SvnRaTransport(repos_url))

        bp = PathPropertyProvider(logwalk)

        self.assertEqual("data", bp.get_properties("", 2)["myprop"])

    def test_get_properties(self):
        repos_url = self.make_svn_repository('d')

        dc = self.get_commit_editor(repos_url)
        dc.change_prop("myprop", "data")
        dc.close()

        transport = SvnRaTransport(repos_url)
        logwalk = self.get_log_walker(transport=transport)

        bp = PathPropertyProvider(logwalk)
        props = bp.get_properties("", 1)
        self.assertEqual("data", props["myprop"])
        self.assertEqual(transport.get_uuid(), props["svn:entry:uuid"])
        self.assertEqual('1', props["svn:entry:committed-rev"])
        self.assertTrue("svn:entry:last-author" in props)
        self.assertTrue("svn:entry:committed-date" in props)


