# -*- coding: utf-8 -*-

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

from bzrlib.errors import (
    InvalidRevisionId,
    )
from bzrlib.osutils import (
    sha,
    )
from bzrlib.revision import (
    Revision,
    )
from bzrlib.tests import (
    TestCase,
    )

from bzrlib.plugins.svn import mapping
from bzrlib.plugins.svn.errors import (
    InvalidPropertyValue,
    )
from bzrlib.plugins.svn.layout.standard import (
    TrunkLayout,
    )
from bzrlib.plugins.svn.mapping import (
    estimate_bzr_ancestors,
    escape_svn_path,
    generate_revision_metadata,
    get_roundtrip_ancestor_revids,
    is_bzr_revision_fileprops,
    is_bzr_revision_revprops,
    mapping_registry,
    parse_merge_property,
    parse_revision_metadata,
    parse_revid_property,
    unescape_svn_path,
    )
from bzrlib.plugins.svn.mapping2 import (
    BzrSvnMappingv1,
    BzrSvnMappingv2,
    )
from bzrlib.plugins.svn.mapping3.base import (
    BzrSvnMappingv3,
    )
from bzrlib.plugins.svn.mapping3.scheme import (
    TrunkBranchingScheme,
    )
from bzrlib.plugins.svn.mapping4 import (
    BzrSvnMappingv4,
    )


class MetadataMarshallerTests(TestCase):

    def test_generate_revision_metadata_none(self):
        self.assertEquals("",
                generate_revision_metadata(None, None, None, None))

    def test_generate_revision_metadata_committer(self):
        self.assertEquals("committer: bla\n",
                generate_revision_metadata(None, None, "bla", None))

    def test_generate_revision_metadata_timestamp(self):
        self.assertEquals("timestamp: 2005-06-30 17:38:52.350850105 +0000\n",
                generate_revision_metadata(1120153132.350850105, 0,
                    None, None))

    def test_generate_revision_metadata_properties(self):
        self.assertEquals("properties: \n" +
                "\tpropbla: bloe\n" +
                "\tpropfoo: bla\n",
                generate_revision_metadata(None, None,
                    None, {"propbla": "bloe", "propfoo": "bla"}))

    def test_generate_revision_metadata_properties_newline(self):
        self.assertEquals("properties: \n" +
                "\tpropbla: bloe\n" +
                "\tpropbla: bla\n",
                generate_revision_metadata(None, None,
                    None, {"propbla": "bloe\nbla"}))

    def test_parse_revision_metadata_empty(self):
        parse_revision_metadata("", None)

    def test_parse_revision_metadata_committer(self):
        rev = Revision('someid')
        parse_revision_metadata("committer: somebody\n", rev)
        self.assertEquals("somebody", rev.committer)

    def test_parse_revision_metadata_with_colon(self):
        rev = Revision('someid')
        parse_revision_metadata("committer: some: body\n", rev)
        self.assertEquals(u"some: body", rev.committer)

    def test_parse_revision_metadata_timestamp(self):
        rev = Revision('someid')
        parse_revision_metadata("timestamp: 2005-06-30 12:38:52.350850105 -0500\n", rev)
        self.assertEquals(1120153132.3508501, rev.timestamp)
        self.assertEquals(-18000, rev.timezone)

    def test_parse_revision_metadata_timestamp_day(self):
        rev = Revision('someid')
        parse_revision_metadata("timestamp: Thu 2005-06-30 12:38:52.350850105 -0500\n", rev)
        self.assertEquals(1120153132.3508501, rev.timestamp)
        self.assertEquals(-18000, rev.timezone)

    def test_parse_revision_metadata_properties(self):
        rev = Revision('someid')
        parse_revision_metadata("properties: \n" +
                                "\tfoo: bar\n" +
                                "\tha: ha\n", rev)
        self.assertEquals({"foo": "bar", "ha": "ha"}, rev.properties)

    def test_parse_revision_metadata_properties_newline(self):
        rev = Revision('someid')
        parse_revision_metadata("properties: \n" +
                                "\tfoo: bar\n" +
                                "\tfoo: bar2\n" +
                                "\tha: ha\n", rev)
        self.assertEquals({"foo": "bar\nbar2", "ha": "ha"}, rev.properties)

    def test_parse_revision_metadata_no_colon(self):
        rev = Revision('someid')
        self.assertRaises(InvalidPropertyValue,
                lambda: parse_revision_metadata("bla", rev))

    def test_parse_revision_metadata_specialchar(self):
        rev = Revision('someid')
        parse_revision_metadata("committer: Adeodato Simó <dato@net.com.org.es>", rev)
        self.assertEquals(u"Adeodato Simó <dato@net.com.org.es>", rev.committer)

    def test_parse_revision_metadata_invalid_name(self):
        rev = Revision('someid')
        self.assertRaises(InvalidPropertyValue,
                lambda: parse_revision_metadata("bla: b", rev))

    def test_parse_revid_property(self):
        self.assertEquals((1, "bloe"), parse_revid_property("1 bloe"))

    def test_parse_revid_property_space(self):
        self.assertEquals((42, "bloe bla"), parse_revid_property("42 bloe bla"))

    def test_parse_revid_property_invalid(self):
        self.assertRaises(InvalidPropertyValue,
                lambda: parse_revid_property("blabla"))

    def test_parse_revid_property_empty_revid(self):
        self.assertRaises(InvalidPropertyValue,
                lambda: parse_revid_property("2 "))

    def test_parse_revid_property_newline(self):
        self.assertRaises(InvalidPropertyValue,
                lambda: parse_revid_property("foo\nbar"))


class ParseMergePropertyTestCase(TestCase):
    def test_parse_merge_space(self):
        self.assertEqual((), parse_merge_property("bla bla"))

    def test_parse_merge_empty(self):
        self.assertEqual((), parse_merge_property(""))

    def test_parse_merge_simple(self):
        self.assertEqual(("bla", "bloe"), parse_merge_property("bla\tbloe"))



def sha1(text):
    return sha(text).hexdigest()


class GenerateRevisionIdTests(TestCase):

    def test_v4_space(self):
        self.assertEqual("svn-v4:uuid:trunk%20l:1",
            BzrSvnMappingv4().revision_id_foreign_to_bzr(("uuid", "trunk l", 1)))

    def test_v4(self):
        self.assertEqual("svn-v4:uuid:trunk:1",
            BzrSvnMappingv4().revision_id_foreign_to_bzr(("uuid", "trunk", 1)))

    def test_v4_slash(self):
        self.assertEqual("svn-v4:uuid:project/trunk:1",
            BzrSvnMappingv4().revision_id_foreign_to_bzr(("uuid", "project/trunk", 1)))


class TestamentTests(TestCase):
    """Make sure that only v4 mappings set the testament revision property."""

    def setUp(self):
        super(TestamentTests, self).setUp()
        class DummyTestament(object):
            """Testament."""

            def as_short_text(self):
                return "testament 1\nfoo bar\n"

        self.testament = DummyTestament()

    def _generate_revprops(self, mapping):
        revprops = {}
        mapping.export_revision_revprops(revprops, "someuuid", "branchp",
                432432432.0, 0, "somebody", {}, "arevid", 4, ["merge1"],
                testament=self.testament)
        return revprops

    def test_v4(self):
        revprops = self._generate_revprops(BzrSvnMappingv4())
        self.assertEquals(self.testament.as_short_text(),
            revprops[mapping.SVN_REVPROP_BZR_TESTAMENT])

    def test_v3(self):
        revprops = self._generate_revprops(BzrSvnMappingv3(TrunkBranchingScheme()))
        self.assertEquals(self.testament.as_short_text(),
            revprops[mapping.SVN_REVPROP_BZR_TESTAMENT])


class ParseRevisionIdTests(TestCase):

    def test_v4(self):
        self.assertEqual((("uuid", "trunk", 1), BzrSvnMappingv4()),
                mapping_registry.parse_revision_id("svn-v4:uuid:trunk:1"))

    def test_v3(self):
        self.assertEqual((("uuid", "trunk", 1), BzrSvnMappingv3(TrunkBranchingScheme())),
                mapping_registry.parse_revision_id("svn-v3-trunk0:uuid:trunk:1"))

    def test_v3_undefined(self):
        self.assertEqual((("uuid", "trunk", 1), BzrSvnMappingv3(TrunkBranchingScheme())),
                mapping_registry.parse_revision_id("svn-v3-undefined:uuid:trunk:1"))

    def test_v2(self):
        self.assertEqual((("uuid", "trunk", 1), BzrSvnMappingv2(TrunkLayout())),
                         mapping_registry.parse_revision_id("svn-v2:1@uuid-trunk"))

    def test_v1(self):
        self.assertEqual((("uuid", "trunk", 1), BzrSvnMappingv1(TrunkLayout())),
                         mapping_registry.parse_revision_id("svn-v1:1@uuid-trunk"))

    def test_except(self):
        self.assertRaises(KeyError,
                         mapping_registry.parse_revision_id, "svn-v0:1@uuid-trunk")

    def test_except_nonsvn(self):
        self.assertRaises(InvalidRevisionId,
                         mapping_registry.parse_revision_id, "blah")


class EscapeTest(TestCase):

    def test_escape_svn_path_none(self):
        self.assertEqual("", escape_svn_path(""))

    def test_escape_svn_path_simple(self):
        self.assertEqual("ab", escape_svn_path("ab"))

    def test_escape_svn_path_percent(self):
        self.assertEqual("a%25b", escape_svn_path("a%b"))

    def test_escape_svn_path_whitespace(self):
        self.assertEqual("foobar%20", escape_svn_path("foobar "))

    def test_escape_svn_path_slash(self):
        self.assertEqual("foobar%2F", escape_svn_path("foobar/"))

    def test_escape_svn_path_special_char(self):
        self.assertEqual("foobar%8A", escape_svn_path("foobar\x8a"))

    def test_unescape_svn_path_slash(self):
        self.assertEqual("foobar/", unescape_svn_path("foobar%2F"))

    def test_unescape_svn_path_none(self):
        self.assertEqual("foobar", unescape_svn_path("foobar"))

    def test_unescape_svn_path_percent(self):
        self.assertEqual("foobar%b", unescape_svn_path("foobar%25b"))

    def test_escape_svn_path_nordic(self):
        self.assertEqual("foobar%C3%A6", escape_svn_path(u"foobar\xe6".encode("utf-8")))


class EstimateBzrAncestorsTests(TestCase):

    def test_no_fileprops(self):
        self.assertEquals(0, estimate_bzr_ancestors({}))

    def test_one(self):
        self.assertEquals(2, estimate_bzr_ancestors({"bzr:revision-id:v42": "bla\nblie\n"}))

    def test_multiple(self):
        self.assertEquals(2, estimate_bzr_ancestors({"bzr:revision-id:v42": "bla\n",
            "bzr:revision-id:v50": "blie\nblie\n"}))


class IsBzrRevisionTests(TestCase):

    def test_no_fileprops(self):
        self.assertEquals(None, is_bzr_revision_fileprops({}))

    def test_fileprops(self):
        self.assertEquals(True, is_bzr_revision_fileprops({"bzr:bla": "bloe"}))

    def test_revprops(self):
        self.assertEquals(True, is_bzr_revision_revprops({mapping.SVN_REVPROP_BZR_MAPPING_VERSION: "42"}))

    def test_revprops_no(self):
        self.assertEquals(False, is_bzr_revision_revprops({mapping.SVN_REVPROP_BZR_SKIP: ""}))

    def test_revprops_unknown(self):
        self.assertEquals(None, is_bzr_revision_revprops({}))


class RoundTripAncestorRevids(TestCase):

    def test_none(self):
        self.assertEquals([], list(get_roundtrip_ancestor_revids({})))

    def test_simple(self):
        self.assertEquals([("arevid", 42, "v42-scheme")],
                list(get_roundtrip_ancestor_revids({mapping.SVN_PROP_BZR_REVISION_ID+"v42-scheme": "42 arevid\n"})))

    def test_multiple(self):
        self.assertEquals(set([("arevid", 42, "v42-scheme"), ("brevid", 50, "v90-ll")]),
                set(get_roundtrip_ancestor_revids({
                    mapping.SVN_PROP_BZR_REVISION_ID+"v42-scheme": "42 arevid\n",
                    mapping.SVN_PROP_BZR_REVISION_ID+"v90-ll": "50 brevid\n",
                    "otherrevprop": "fsldds",
                    })))


class TestParseSvnProps(TestCase):

    def test_import_revision_svnprops(self):
        rev = Revision(None)
        revprops = {"svn:log": "A log msg", "svn:author": "Somebody",
                "svn:date": "2008-11-03T09:33:00.716938Z"}
        mapping.parse_svn_revprops(revprops,  rev)
        self.assertEquals("Somebody", rev.committer)
        self.assertEquals("A log msg", rev.message)
        self.assertEquals({}, rev.properties)
        self.assertEquals(0.0, rev.timezone)
        self.assertEquals(1225704780.716938, rev.timestamp,
                "parsing %s" % revprops["svn:date"])

