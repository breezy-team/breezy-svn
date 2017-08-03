# Copyright (C) 2012 Martin Packman <martin.packman@canonical.com>

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

"""Tests for authentication module"""

from breezy import (
    tests,
    ui,
    )
from breezy.plugins.svn.auth import (
    SubversionAuthenticationConfig,
    )


class TestSubversionAuthenticationConfig(tests.TestCase):
    """Subversion specific authentication.conf handling tests"""

    def override_ui(self, stdin):
        stdout = tests.StringIOWrapper()
        stderr = tests.StringIOWrapper()
        ui.ui_factory = tests.TestUIFactory(stdin=stdin, stdout=stdout,
            stderr=stderr)
        return stdout, stderr

    def test_get_svn_simple(self):
        username, password = "a-user", "a-secret"
        realm = "REALM" # what does this mean in the context of svn?
        stdout, stderr = self.override_ui(password + "\n")
        conf = SubversionAuthenticationConfig("svn", username, None, None)
        got_user, got_pass, _ = conf.get_svn_simple(realm, username, True)
        self.assertIsInstance(got_user, str)
        self.assertEquals(username, got_user)
        self.assertIsInstance(got_pass, str)
        self.assertEquals(password, got_pass)
        self.assertEquals("%s %s password: " % (realm, username),
            stderr.getvalue())
        self.assertEquals("", stdout.getvalue())
