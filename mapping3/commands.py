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

from bzrlib.commands import Command
from bzrlib.option import Option

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
        from bzrlib import errors as bzr_errors
        from bzrlib.bzrdir import BzrDir
        from bzrlib.msgeditor import edit_commit_message
        from bzrlib.trace import info

        from bzrlib.plugins.svn.repository import SvnRepository
        from bzrlib.plugins.svn.mapping3.base import (
            BzrSvnMappingv3,
            config_set_scheme, 
            get_property_scheme,
            set_property_scheme,
            )
        from bzrlib.plugins.svn.mapping3.scheme import (
            scheme_from_branch_list,
            )
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
