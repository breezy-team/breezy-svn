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

from bzrlib.errors import InvalidRevisionId
from bzrlib.revision import Revision
from bzrlib.tests import TestCase, TestNotApplicable

from bzrlib.plugins.svn.mapping import mapping_registry, parse_svn_revprops
from subvertpy import properties

def changed_props(props):
    return properties.diff(props, {})

class RoundtripMappingTests(TestCase):

    def setUp(self):
        super(RoundtripMappingTests, self).setUp()
        self.mapping = mapping_registry.get(self.mapping_name).get_test_instance()

    def test_newer_than_self(self):
        # A mapping can never be newer than itself
        self.assertEquals(False, self.mapping.newer_than(self.mapping))

    def test_roundtrip_revision(self):
        revid = self.mapping.revision_id_foreign_to_bzr(("myuuid", "path", 42))
        (uuid, path, revnum), mapping = self.mapping.revision_id_bzr_to_foreign(revid)
        self.assertEquals(uuid, "myuuid")
        self.assertEquals(revnum, 42)
        self.assertEquals(path, "path")
        self.assertEquals(mapping, self.mapping)

    def test_fileid_map_fileprops(self):
        if not self.mapping.roundtripping:
            raise TestNotApplicable
        if not self.mapping.can_use_fileprops:
            raise TestNotApplicable
        fileids = {"": "some-id", "bla/blie": "other-id"}
        fileprops = {}
        self.mapping.export_revision_fileprops(432432432.0, 0, "somebody", {}, "arevid", 4, ["merge1"], fileprops)
        self.mapping.export_fileid_map_fileprops(fileids, fileprops)
        self.assertEquals(fileids, 
                self.mapping.import_fileid_map_fileprops(changed_props(fileprops)))

    def test_fileid_map_revprops(self):
        if not self.mapping.roundtripping:
            raise TestNotApplicable
        if not self.mapping.can_use_revprops:
            raise TestNotApplicable
        fileids = {"": "some-id", "bla/blie": "other-id"}
        revprops = {}
        revprops["svn:date"] = "2008-11-03T09:33:00.716938Z"
        self.mapping.export_revision_revprops("someuuid", "branchp", 432432432.0, 0, "somebody", {}, "arevid", 4, ["merge1"], revprops)
        self.mapping.export_fileid_map_revprops(fileids, revprops)
        self.assertEquals(fileids, 
                self.mapping.import_fileid_map_revprops(revprops))

    def test_text_revisions_fileprops(self):
        if not self.mapping.roundtripping:
            raise TestNotApplicable
        if not self.mapping.can_use_fileprops:
            raise TestNotApplicable
        fileprops = {}
        text_revisions = {"bla": "bloe", "ll": "12"}
        self.mapping.export_text_revisions_fileprops(text_revisions, fileprops)
        self.assertEquals(text_revisions,
            self.mapping.import_text_revisions_fileprops(changed_props(fileprops)))

    def test_text_revisions_revprops(self):
        if not self.mapping.roundtripping:
            raise TestNotApplicable
        if not self.mapping.can_use_revprops:
            raise TestNotApplicable
        revprops = {}
        text_revisions = {"bla": "bloe", "ll": "12"}
        self.mapping.export_text_revisions_revprops(text_revisions, revprops)
        self.assertEquals(text_revisions,
            self.mapping.import_text_revisions_revprops(revprops))

    def test_message_fileprops(self):
        if not self.mapping.roundtripping:
            raise TestNotApplicable
        fileprops = {}
        self.mapping.export_revision_fileprops(432432432.0, 0, "somebody", 
                                     {"arevprop": "val"}, "arevid", 4, ["merge1"], fileprops)
        try:
            self.mapping.export_message_fileprops("My Commit message", fileprops)
        except NotImplementedError:
            raise TestNotApplicable
        targetrev = Revision(None)
        self.mapping.import_revision_fileprops(changed_props(fileprops), targetrev)
        self.assertEquals("My Commit message", targetrev.message)

    def test_message_revprops(self):
        if not self.mapping.roundtripping:
            raise TestNotApplicable
        if not self.mapping.can_use_revprops:
            raise TestNotApplicable
        revprops = {}
        self.mapping.export_revision_revprops("someuuid", "branchp", 432432432.0, 0, "somebody", 
                                     {"arevprop": "val"}, "arevid", 4, ["merge1"], revprops)
        revprops["svn:date"] = "2008-11-03T09:33:00.716938Z"
        try:
            self.mapping.export_message_revprops("My Commit message", revprops)
        except NotImplementedError:
            raise TestNotApplicable
        targetrev = Revision(None)
        self.mapping.import_revision_revprops(revprops, targetrev)
        self.assertEquals("My Commit message", targetrev.message)

    def test_revision_fileprops(self):
        if not self.mapping.roundtripping:
            raise TestNotApplicable
        if not self.mapping.can_use_fileprops:
            raise TestNotApplicable
        revprops = {}
        fileprops = {}
        self.mapping.export_revision_fileprops(432432432.0, 0, "somebody", 
                                     {"arevprop": "val" }, "arevid", 4, ["parent", "merge1"], fileprops)
        targetrev = Revision(None)
        self.mapping.import_revision_fileprops(changed_props(fileprops), targetrev)
        self.assertEquals(targetrev.committer, "somebody")
        self.assertEquals(targetrev.properties, {"arevprop": "val"})
        self.assertEquals(targetrev.timestamp, 432432432.0)
        self.assertEquals(targetrev.timezone, 0)

    def test_revision_revprops(self):
        if not self.mapping.roundtripping:
            raise TestNotApplicable
        if not self.mapping.can_use_revprops:
            raise TestNotApplicable
        revprops = {}
        self.mapping.export_revision_revprops("someuuid", "branchp", 432432432.0, 0, "somebody", 
                                     {"arevprop": "val" }, "arevid", 4, ["parent", "merge1"], revprops)
        targetrev = Revision(None)
        revprops["svn:date"] = "2008-11-03T09:33:00.716938Z"
        parse_svn_revprops(revprops, targetrev)
        self.mapping.import_revision_revprops(revprops, targetrev)
        self.assertEquals(targetrev.committer, "somebody")
        self.assertEquals(targetrev.properties, {"arevprop": "val"})
        self.assertEquals(targetrev.timestamp, 432432432.0)
        self.assertEquals(targetrev.timezone, 0)

    def test_revision_id_fileprops(self):
        if not self.mapping.roundtripping:
            raise TestNotApplicable
        if not self.mapping.can_use_fileprops:
            raise TestNotApplicable
        fileprops = {}
        self.mapping.export_revision_fileprops(432432432.0, 0, "somebody", {}, "arevid", 4, ["parent", "merge1"], fileprops)
        self.assertEquals((4, "arevid", False), self.mapping.get_revision_id_fileprops(changed_props(fileprops)))

    def test_revision_id_revprops(self):
        if not self.mapping.roundtripping:
            raise TestNotApplicable
        if not self.mapping.can_use_revprops:
            raise TestNotApplicable
        revprops = {}
        self.mapping.export_revision_revprops("someuuid", "branchp", 432432432.0, 0, "somebody", {}, "arevid", 4, ["parent", "merge1"], revprops)
        self.assertEquals((4, "arevid", False), self.mapping.get_revision_id_revprops(revprops))
    
    def test_revision_id_none(self):
        if not self.mapping.roundtripping:
            raise TestNotApplicable
        if self.mapping.can_use_fileprops:
            self.assertEquals((None, None, False), self.mapping.get_revision_id_fileprops({}))
        if self.mapping.can_use_revprops:
            self.assertEquals((None, None, False), self.mapping.get_revision_id_revprops({}))

    def test_parse_revision_id_unknown(self):
        self.assertRaises(InvalidRevisionId, 
                lambda: self.mapping.revision_id_bzr_to_foreign("bla"))

    def test_parse_revision_id(self):
        self.assertEquals((("myuuid", "bla", 5), self.mapping), 
            self.mapping.revision_id_bzr_to_foreign(
                self.mapping.revision_id_foreign_to_bzr(("myuuid", "bla", 5))))



