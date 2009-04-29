# Copyright (C) 2005-2009 Jelmer Vernooij <jelmer@samba.org>
 
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

"""Branch tests."""

from bzrlib import urlutils
from bzrlib.branch import Branch
from bzrlib.bzrdir import BzrDir
from bzrlib.errors import NoSuchFile, NoSuchRevision, NoSuchTag
from bzrlib.repository import Repository
from bzrlib.revision import NULL_REVISION
from bzrlib.trace import mutter

import os
from unittest import TestCase

import subvertpy
from bzrlib.plugins.svn.branch import SvnBranchFormat
from bzrlib.plugins.svn.convert import load_dumpfile
from bzrlib.plugins.svn.mapping import SVN_PROP_BZR_REVISION_ID, mapping_registry
from bzrlib.plugins.svn.tests import SubversionTestCase

class WorkingSubversionBranch(SubversionTestCase):

    def setUp(self):
        super(WorkingSubversionBranch, self).setUp()
        self._old_mapping = mapping_registry._get_default_key()
        mapping_registry.set_default(self.mapping_name)

    def tearDown(self):
        super(WorkingSubversionBranch, self).tearDown()
        mapping_registry.set_default(self._old_mapping)

