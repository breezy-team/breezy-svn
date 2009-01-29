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

from bzrlib import errors

from bzrlib.plugins.svn import mapping
from bzrlib.plugins.svn.errors import InvalidFileId

supported_features = set()


class BzrSvnMappingv4(mapping.BzrSvnMappingFileProps, mapping.BzrSvnMappingRevProps, mapping.BzrSvnMapping):
    """Mapping between Subversion and Bazaar, introduced in bzr-svn 0.5.

    Tries to use revision properties when possible.

    TODO: Add variable with required features.
    """
    revid_prefix = "svn-v4"
    upgrade_suffix = "-svn4"
    experimental = False
    roundtripping = True
    can_use_revprops = True
    can_use_fileprops = True
    supports_hidden = True
    restricts_branch_paths = False
    parseable_file_ids = True

    def __init__(self):
        mapping.BzrSvnMapping.__init__(self)
        self.name = "v4"

    def __eq__(self, other):
        return type(self) == type(other)

    def __ne__(self, other):
        return type(self) != type(other)

    def __hash__(self):
        return hash(type(self))

    @classmethod
    def from_repository(cls, repository, _hinted_branch_path=None):
        if _hinted_branch_path == "":
            return cls()
        else:
            return cls()

    @classmethod
    def from_revprops(cls, revprops):
        return cls()

    @classmethod
    def revision_id_bzr_to_foreign(cls, revid):
        assert isinstance(revid, str)

        if not revid.startswith(cls.revid_prefix):
            raise errors.InvalidRevisionId(revid, "")

        try:
            (version, uuid, branch_path, srevnum) = revid.split(":")
        except ValueError:
            raise errors.InvalidRevisionId(revid, "")

        branch_path = mapping.unescape_svn_path(branch_path)

        return (uuid, branch_path, int(srevnum)), cls()

    def revision_id_foreign_to_bzr(self, (uuid, path, revnum)):
        return "svn-v4:%s:%s:%d" % (uuid, path, revnum)

    def generate_file_id(self, (uuid, branch, revnum), inv_path):
        return "%d@%s:%s" % (revnum, uuid, mapping.escape_svn_path("%s/%s" % (branch, inv_path.encode("utf-8"))))

    def parse_file_id(self, fileid):
        try:
            (revnum_str, rest) = fileid.split("@", 1)
            revnum = int(revnum_str)
            (uuid, path_escaped) = rest.split(":", 1)
        except ValueError:
            raise InvalidFileId(fileid)
        path = mapping.unescape_svn_path(path_escaped).strip("/")
        return (uuid, revnum, path)

    def is_branch(self, branch_path):
        return True

    def is_tag(self, tag_path):
        return True

    def __eq__(self, other):
        return type(self) == type(other)

    def __ne__(self, other):
        return type(self) != type(other)

    def get_lhs_parent_fileprops(self, fileprops):
        return None

    def import_revision_revprops(self, svn_revprops, rev):
        if svn_revprops.has_key(mapping.SVN_REVPROP_BZR_REQUIRED_FEATURES):
            features = mapping.parse_required_features_property(svn_revprops[mapping.SVN_REVPROP_BZR_REQUIRED_FEATURES])
            assert features.issubset(supported_features), "missing feature: %r" % features.difference(supported_features)
        return mapping.BzrSvnMappingRevProps.import_revision_revprops(self, svn_revprops, rev)

    def import_revision_fileprops(self, fileprops, rev):
        if fileprops.has_key(mapping.SVN_PROP_BZR_REQUIRED_FEATURES):
            features = mapping.parse_required_features_property(fileprops[mapping.SVN_PROP_BZR_REQUIRED_FEATURES])
            assert features.issubset(supported_features), "missing feature: %r" % features.difference(supported_features)
        return mapping.BzrSvnMappingFileProps.import_revision_fileprops(self, fileprops, rev)

    def get_mandated_layout(self, repository):
        return None

    def is_bzr_revision_hidden_revprops(self, revprops):
        return revprops.has_key(mapping.SVN_REVPROP_BZR_HIDDEN)

    def is_bzr_revision_hidden_fileprops(self, changed_fileprops):
        return changed_fileprops.has_key(mapping.SVN_PROP_BZR_HIDDEN)

    def get_hidden_lhs_ancestors_count(self, fileprops):
        return int(fileprops.get(mapping.SVN_PROP_BZR_HIDDEN, "0"))

    def export_hidden_revprops(self, branch_root, revprops):
        revprops[mapping.SVN_REVPROP_BZR_HIDDEN] = ""
        revprops[mapping.SVN_REVPROP_BZR_ROOT] = branch_root

    def export_hidden_fileprops(self, fileprops):
        old_value = fileprops.get(mapping.SVN_PROP_BZR_HIDDEN, "0")
        fileprops[mapping.SVN_PROP_BZR_HIDDEN] = str(int(old_value)+1)

    def supports_tags(self):
        return True

    def export_revprop_redirect(self, revnum, fileprops):
        if not mapping.SVN_PROP_BZR_REVPROP_REDIRECT in fileprops:
            fileprops[mapping.SVN_PROP_BZR_REVPROP_REDIRECT] = str(revnum)
