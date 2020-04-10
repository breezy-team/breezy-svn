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

"""bzr<->svn mapping format 2."""

from __future__ import absolute_import

from breezy.errors import (
    InvalidRevisionId,
    )
from breezy.bzr.inventory import (
    ROOT_ID,
    )
from six import text_type

from .errors import (
    LayoutUnusable,
    NotSvnBranchPath,
    NoLayoutTagSetSupport,
    )
from .layout import (
    RepositoryLayout,
    get_root_paths,
    )
from .layout.standard import (
    RootLayout,
    TrunkLayout,
    )
from .mapping import (
    BzrSvnMapping,
    escape_svn_path,
    unescape_svn_path,
    )

SVN_PROP_BZR_MERGE = 'bzr:merge'

class BzrSvnMappingv1(BzrSvnMapping):
    """This was the initial version of the mappings as used by bzr-svn
    0.2.

    It does not support pushing revisions to Subversion as-is, but only
    as part of a merge.
    """
    name = "v1"
    roundtripping = False
    revid_prefix = b"svn-v1"
    restricts_branch_paths = True
    can_use_fileprops= True

    def __init__(self, layout):
        super(BzrSvnMappingv1, self).__init__()
        self._layout = layout

    @classmethod
    def revision_id_bzr_to_foreign(cls, revid):
        if not revid.startswith("svn-v1:"):
            raise InvalidRevisionId(revid, "")
        revid = revid[len("svn-v1:"):]
        at = revid.index("@")
        fash = revid.rindex("-")
        uuid = revid[at+1:fash]
        branch_path = unescape_svn_path(revid[fash+1:])
        revnum = int(revid[0:at])
        assert revnum >= 0
        return (uuid, branch_path, revnum), cls(LegacyLayout.from_branch_path(branch_path))

    @classmethod
    def revision_id_foreign_to_bzr(cls, foreign_revid):
        (uuid, path, revnum) = foreign_revid
        assert isinstance(path, str)
        return b"svn-v1:%d@%s-%s" % (revnum, uuid, escape_svn_path(path))

    def __eq__(self, other):
        return type(self) == type(other)

    def __ne__(self, other):
        return type(self) != type(other)

    def __hash__(self):
        return hash(type(self))

    def is_branch(self, branch_path):
        return self._layout.is_branch(branch_path)

    def is_tag(self, tag_path):
        return False

    def import_revision_fileprops(self, fileprops, rev):
        return True

    def generate_file_id(self, foreign_revid, inv_path):
        if inv_path == u"":
            return ROOT_ID
        revid = self.revision_id_foreign_to_bzr(foreign_revid)
        return b"%s-%s" % (revid, escape_svn_path(inv_path.encode("utf-8")))

    def import_fileid_map_fileprops(self, fileprops):
        return {}

    def import_text_parents_fileprops(self, fileprops):
        return {}

    def import_text_revisions_fileprops(self, fileprops):
        return {}

    def get_rhs_parents_fileprops(self, fileprops):
        value = fileprops.get(SVN_PROP_BZR_MERGE, "")
        if value == "":
            return ()
        return (value.splitlines()[-1])

    @classmethod
    def from_repository(cls, repository, _hinted_branch_path=None):
        if _hinted_branch_path is None:
            return cls(TrunkLegacyLayout())

        return cls(LegacyLayout.from_branch_path(_hinted_branch_path))

    @classmethod
    def get_test_instance(cls):
        return cls(TrunkLegacyLayout())

    def get_guessed_layout(self, repository):
        return self._layout

    def check_layout(self, repository, layout):
        if isinstance(layout, RootLayout):
            self._layout = RootLegacyLayout()
        elif isinstance(layout, TrunkLayout):
            self._layout = TrunkLegacyLayout(layout.level or 0)
        else:
            raise LayoutUnusable(layout, self)

    def supports_tags(self):
        return False


class BzrSvnMappingv2(BzrSvnMappingv1):
    """The second version of the mappings as used in the 0.3.x series.

    It does not support pushing revisions to Subversion as-is, but only
    as part of a merge.
    """
    name = "v2"
    roundtripping = False
    revid_prefix = b"svn-v2"
    restricts_branch_paths = True
    can_use_fileprops = True

    @classmethod
    def revision_id_bzr_to_foreign(cls, revid):
        if not revid.startswith("svn-v2:"):
            raise InvalidRevisionId(revid, "")
        revid = revid[len("svn-v2:"):]
        at = revid.index("@")
        fash = revid.rindex("-")
        uuid = revid[at+1:fash]
        branch_path = unescape_svn_path(revid[fash+1:])
        revnum = int(revid[0:at])
        assert revnum >= 0
        return (uuid, branch_path, revnum), cls(LegacyLayout.from_branch_path(branch_path))

    def revision_id_foreign_to_bzr(self, foreign_revid):
        (uuid, path, revnum) = foreign_revid
        assert isinstance(path, str)
        return b"svn-v2:%d@%s-%s" % (revnum, uuid, escape_svn_path(path))

    def __eq__(self, other):
        return type(self) == type(other)

    def __ne__(self, other):
        return type(self) != type(other)

    def __hash__(self):
        return hash(type(self))


class LegacyLayout(RepositoryLayout):

    def get_tag_path(self, name, project=""):
        raise NoLayoutTagSetSupport(self)

    def get_branch_path(self, name, project=""):
        return None

    @classmethod
    def from_branch_path(cls, path):
        parts = path.strip("/").split("/")
        for i in range(0,len(parts)):
            if parts[i] == "trunk" or \
               parts[i] == "branches" or \
               parts[i] == "tags":
                return TrunkLegacyLayout(level=i)

        return RootLegacyLayout()


class TrunkLegacyLayout(LegacyLayout):

    def __init__(self, level=0):
        super(TrunkLegacyLayout, self).__init__()
        self.level = level

    def parse(self, path):
        assert isinstance(path, text_type)
        parts = path.strip(u"/").split(u"/")
        if len(parts) == 0 or self.level >= len(parts):
            raise NotSvnBranchPath(path, self)

        if parts[self.level] == u"trunk" or parts[self.level] == u"hooks":
            return ("branch",
                    u"/".join(parts[0:self.level]),
                    u"/".join(parts[0:self.level+1]).strip(u"/"),
                    u"/".join(parts[self.level+1:]).strip(u"/"))
        elif ((parts[self.level] == u"tags" or parts[self.level] == u"branches") and
              len(parts) >= self.level+2):
            return ("branch",
                    u"/".join(parts[0:self.level]),
                    u"/".join(parts[0:self.level+2]).strip(u"/"),
                    u"/".join(parts[self.level+2:]).strip(u"/"))
        else:
            raise NotSvnBranchPath(path, self)

    def is_branch(self, path, project=None):
        parts = path.strip(u"/").split(u"/")
        if len(parts) == self.level+1 and parts[self.level] == u"trunk":
            return True

        if len(parts) == self.level+2 and \
           (parts[self.level] == u"branches" or parts[self.level] == u"tags"):
            return True

        return False

    def get_branches(self, repository, revnum, project=u"", pb=None):
        return get_root_paths(repository,
             [(u"*/" * self.level) + x for x in [u"branches/*", u"tags/*", u"trunk"]],
             revnum, self.is_branch, project)

    def get_tags(self, repository, revnum, project=u"", pb=None):
        return []


class RootLegacyLayout(LegacyLayout):

    def parse(self, path):
        assert isinstance(path, text_type)
        return ("branch", u"", u"", path)

    def is_branch(self, path, project=None):
        return path == u""

    def get_branches(self, repository, revnum, project="", pb=None):
        return [(u"", u"", u"trunk", True)]

    def get_tags(self, repository, revnum, project="", pb=None):
        return []
