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

from bzrlib import (
        errors as bzr_errors,
        osutils,
        ui,
        )
from bzrlib.commands import Command
from bzrlib.option import Option
from bzrlib.trace import info, mutter

from subvertpy import properties
from subvertpy.ra import DIRENT_KIND

from bzrlib.plugins.svn import errors, mapping
from bzrlib.plugins.svn.layout.guess import GUESS_SAMPLE_SIZE
from bzrlib.plugins.svn.layout import RepositoryLayout, get_root_paths
from bzrlib.plugins.svn.mapping3.scheme import (
        BranchingScheme,
        guess_scheme_from_branch_path, 
        guess_scheme_from_history, 
        ListBranchingScheme, 
        scheme_from_branch_list,
        scheme_from_layout,
        parse_list_scheme_text,
        NoBranchingScheme,
        TrunkBranchingScheme,
        ListBranchingScheme,
        InvalidSvnBranchPath,
        )

SVN_PROP_BZR_BRANCHING_SCHEME = 'bzr:branching-scheme'

class SchemeDerivedLayout(RepositoryLayout):
    def __init__(self, repository, scheme):
        self.repository = repository
        self.scheme = scheme

    def parse(self, path):
        try:
            (proj, bp, rp) = self.scheme.unprefix(path)
        except InvalidSvnBranchPath, e:
            raise errors.NotSvnBranchPath(e.path)
        if self.scheme.is_tag(bp):
            type = "tag"
        else:
            type = "branch"
        return (type, proj, bp, rp)

    def get_tag_name(self, path, project=None):
        return path.split("/")[-1]

    def supports_tags(self):
        return (self.scheme.tag_list != [])

    def get_branches(self, repository, revnum, project=None, pb=None):
        return get_root_paths(repository, self.scheme.branch_list, revnum, self.scheme.is_branch, project, pb)

    def get_tags(self, repository, revnum, project=None, pb=None):
        return get_root_paths(repository, self.scheme.tag_list, revnum, self.scheme.is_tag, project, pb)

    def get_tag_path(self, name, project=""):
        return self.scheme.get_tag_path(name, project)

    def get_branch_path(self, name, project=""):
        return self.scheme.get_branch_path(name, project)

    def is_branch_parent(self, path, project=None):
        # Na, na, na...
        return self.scheme.is_branch_parent(path, project)

    def is_tag_parent(self, path, project=None):
        # Na, na, na...
        return self.scheme.is_tag_parent(path, project)

    def push_merged_revisions(self, project=""):
        try:
            self.scheme.get_branch_path("somebranch")
            return True
        except NotImplementedError:
            return False

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, repr(self.scheme))



def get_stored_scheme(repository):
    """Retrieve the stored branching scheme, either in the repository 
    or in the configuration file.
    """
    scheme = repository.get_config().get_branching_scheme()
    guessed_scheme = repository.get_config().get_guessed_branching_scheme()
    is_mandatory = repository.get_config().branching_scheme_is_mandatory()
    if scheme is not None:
        return (scheme, guessed_scheme, is_mandatory)

    last_revnum = repository.get_latest_revnum()
    scheme = get_property_scheme(repository, last_revnum)
    if scheme is not None:
        return (scheme, None, True)

    return (None, guessed_scheme, False)


def get_property_scheme(repository, revnum=None):
    if revnum is None:
        revnum = repository.get_latest_revnum()
    text = repository.branchprop_list.get_properties("", revnum).get(SVN_PROP_BZR_BRANCHING_SCHEME, None)
    if text is None:
        return None
    return ListBranchingScheme(parse_list_scheme_text(text))


def set_property_scheme(repository, scheme):
    conn = repository.transport.get_connection()
    try:
        editor = conn.get_commit_editor(
            {properties.PROP_REVISION_LOG: "Updating branching scheme for Bazaar."},
            None, None, False)
        root = editor.open_root()
        root.change_prop(SVN_PROP_BZR_BRANCHING_SCHEME, 
                "".join(map(lambda x: x+"\n", scheme.branch_list)).encode("utf-8"))
        root.close()
        editor.close()
    finally:
        repository.transport.add_connection(conn)


def repository_guess_scheme(repository, last_revnum, branch_path=None):
    pb = ui.ui_factory.nested_progress_bar()
    try:
        (guessed_scheme, scheme) = guess_scheme_from_history(
            repository._log.iter_changes(None, last_revnum, max(0, last_revnum-GUESS_SAMPLE_SIZE)), last_revnum, branch_path)
    finally:
        pb.finished()
    mutter("Guessed branching scheme: %r, guess scheme to use: %r" % 
            (guessed_scheme, scheme))
    return (guessed_scheme, scheme)


def config_set_scheme(repository, scheme, guessed_scheme, mandatory=False):
    if guessed_scheme is None:
        guessed_scheme_str = None
    else:
        guessed_scheme_str = str(guessed_scheme)
    repository.get_config().set_branching_scheme(str(scheme), 
                            guessed_scheme_str, mandatory=mandatory)


class BzrSvnMappingv3(mapping.BzrSvnMappingFileProps, mapping.BzrSvnMappingRevProps, mapping.BzrSvnMapping):
    """The third version of the mappings as used in the 0.4.x series.

    Relies exclusively on file properties, though 
    bzr-svn 0.4.11 and up will set some revision properties 
    as well if possible.
    """
    experimental = False
    upgrade_suffix = "-svn3"
    revid_prefix = "svn-v3-"
    roundtripping = True
    can_use_fileprops = True
    must_use_fileprops = True
    can_use_revprops = True
    restricts_branch_paths = True
    parseable_file_ids = True

    def __init__(self, scheme, guessed_scheme=None):
        mapping.BzrSvnMapping.__init__(self)
        if isinstance(scheme, str):
            self.scheme = BranchingScheme.find_scheme(scheme)
        else:
            self.scheme = scheme
        self.guessed_scheme = guessed_scheme
        self.name = "v3-" + str(scheme)

    def __eq__(self, other):
        return (type(self) == type(other) and self.name == other.name)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash((type(self), self.name))

    @classmethod
    def from_revprops(cls, revprops):
        return cls()

    def get_mandated_layout(self, repository):
        return SchemeDerivedLayout(repository, self.scheme)

    def get_guessed_layout(self, repository):
        return SchemeDerivedLayout(repository, self.guessed_scheme or self.scheme)

    def check_layout(self, repository, layout):
        self.scheme = scheme_from_layout(layout)
        config_set_scheme(repository, self.scheme, self.scheme)

    @classmethod
    def from_repository(cls, repository, _hinted_branch_path=None):
        (actual_scheme, guessed_scheme, mandatory) = get_stored_scheme(repository)
        if mandatory:
            return cls(actual_scheme, guessed_scheme) 

        if actual_scheme is not None:
            if (_hinted_branch_path is None or 
                actual_scheme.is_branch(_hinted_branch_path)):
                return cls(actual_scheme, guessed_scheme)

        last_revnum = repository.get_latest_revnum()
        (guessed_scheme, actual_scheme) = repository_guess_scheme(repository, last_revnum, _hinted_branch_path)
        if last_revnum > 20:
            config_set_scheme(repository, actual_scheme, guessed_scheme, mandatory=False)

        return cls(actual_scheme, guessed_scheme)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.scheme)

    def generate_file_id(self, (uuid, branch, revnum), inv_path):
        assert isinstance(uuid, str)
        assert isinstance(revnum, int)
        assert isinstance(branch, str)
        assert isinstance(inv_path, unicode)
        inv_path = inv_path.encode("utf-8")
        ret = "%d@%s:%s:%s" % (revnum, uuid, mapping.escape_svn_path(branch), 
                               mapping.escape_svn_path(inv_path))
        if len(ret) > 150:
            ret = "%d@%s:%s;%s" % (revnum, uuid, 
                                mapping.escape_svn_path(branch),
                                osutils.sha(inv_path).hexdigest())
        assert isinstance(ret, str)
        return osutils.safe_file_id(ret)

    def parse_file_id(self, file_id):
        try:
            (revnum_str, rest) = file_id.split("@", 1)
            revnum = int(revnum_str)
            (uuid, bp, ip) = rest.split(":", 2)
        except ValueError:
            raise errors.InvalidFileId(file_id)
        return (uuid, revnum, ("%s/%s" % (mapping.unescape_svn_path(bp), mapping.unescape_svn_path(ip))).strip("/"))

    @classmethod
    def _parse_revision_id(cls, revid):
        assert isinstance(revid, str)

        if not revid.startswith(cls.revid_prefix):
            raise bzr_errors.InvalidRevisionId(revid, "")

        try:
            (version, uuid, branch_path, srevnum) = revid.split(":")
        except ValueError:
            raise bzr_errors.InvalidRevisionId(revid, "")

        scheme = version[len(cls.revid_prefix):]

        branch_path = mapping.unescape_svn_path(branch_path)

        return (uuid, branch_path, int(srevnum), scheme)

    @classmethod
    def revision_id_bzr_to_foreign(cls, revid):
        (uuid, branch_path, srevnum, scheme) = cls._parse_revision_id(revid)
        # Some older versions of bzr-svn 0.4 did not always set a branching
        # scheme but set "undefined" instead.
        if scheme == "undefined":
            scheme = guess_scheme_from_branch_path(branch_path)
        else:
            scheme = BranchingScheme.find_scheme(scheme)

        return (uuid, branch_path, srevnum), cls(scheme)

    def is_branch(self, branch_path):
        return self.scheme.is_branch(branch_path)

    def is_tag(self, tag_path):
        return self.scheme.is_tag(tag_path)

    def supports_tags(self):
        try:
            self.scheme.get_tag_path(u"foo")
            return True
        except NotImplementedError:
            return False

    @classmethod
    def _generate_revision_id(cls, uuid, revnum, path, scheme):
        assert isinstance(revnum, int)
        assert isinstance(path, str)
        assert revnum >= 0
        assert revnum > 0 or path == "", \
                "Trying to generate revid for (%r,%r)" % (path, revnum)
        return "%s%s:%s:%s:%d" % (cls.revid_prefix, scheme, uuid, \
                       mapping.escape_svn_path(path.strip("/")), revnum)

    def revision_id_foreign_to_bzr(self, (uuid, path, revnum)):
        return self._generate_revision_id(uuid, revnum, path, self.scheme)

    def __eq__(self, other):
        return type(self) == type(other) and self.scheme == other.scheme

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, repr(self.scheme))

    @classmethod
    def get_test_instance(cls):
        return cls(NoBranchingScheme())


class cmd_svn_branching_scheme(Command):
    """Show or change the branching scheme for a Subversion repository.

    This command is deprecated in favour of "bzr svn-layout". It should
    only be used if you need compatibility with bzr-svn 0.4.x.

    See 'bzr help svn-branching-schemes' for details.
    """
    takes_args = ['location?']
    takes_options = [
        Option('set', help="Change the branching scheme. "),
        Option('repository-wide', 
            help="Act on repository-wide setting rather than local.")
        ]
    hidden = True

    def run(self, location=".", set=False, repository_wide=False):
        from bzrlib.bzrdir import BzrDir
        from bzrlib.msgeditor import edit_commit_message
        from bzrlib.plugins.svn.repository import SvnRepository
        def scheme_str(scheme):
            if scheme is None:
                return ""
            return "".join(map(lambda x: x+"\n", scheme.to_lines()))
        dir = BzrDir.open_containing(location)[0]
        repos = dir.find_repository()
        if not isinstance(repos, SvnRepository):
            raise bzr_errors.BzrCommandError("Not a Subversion repository: %s" % location)
        if repository_wide:
            scheme = get_property_scheme(repos)
        else:
            scheme = BzrSvnMappingv3.from_repository(repos).scheme
        if set:
            schemestr = edit_commit_message("", 
                                            start_message=scheme_str(scheme))
            scheme = scheme_from_branch_list(
                map(lambda x:x.strip("\n"), schemestr.splitlines()))
            if repository_wide:
                set_property_scheme(repos, scheme)
            else:
                config_set_scheme(repos, scheme, None, mandatory=True)
        elif scheme is not None:
            info(scheme_str(scheme))



