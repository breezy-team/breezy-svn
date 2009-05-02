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
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA


"""Subversion-specific Bazaar command line subcommands."""


from bzrlib.commands import (
    Command,
    )
from bzrlib.option import (
    Option,
    RegistryOption,
    )


def get_layout(layoutname):
    """Parse layout name and return a layout.
    
    :param layout: Name of the layout to retrieve.
    """
    if isinstance(layoutname, unicode):
        layoutname = layoutname.encode("ascii")
    from bzrlib.plugins.svn.layout import layout_registry
    from bzrlib.errors import BzrCommandError
    
    try:
        ret = layout_registry.get(layoutname)()
    except KeyError:
        raise BzrCommandError('No such repository layout %r' % layoutname)
    return ret


class cmd_svn_import(Command):
    """Convert a Subversion repository to a Bazaar repository.
    
    To save disk space, only branches will be created by default 
    (no working trees). To create a tree for a branch, run "bzr co" in 
    it.

    This command is resumable; any previously imported revisions will be 
    skipped.
    """
    takes_args = ['from_location', 'to_location?']
    takes_options = [Option('trees', help='Create working trees.'),
                     Option('standalone', help='Create standalone branches.'),
                     Option('all', 
                         help='Convert all revisions, even those not in '
                              'current branch history (forbids --standalone).'),
                     Option('layout', type=get_layout,
                         help='Repository layout (none, trunk, etc). '
                              'Default: auto.'),
                     Option('keep', 
                         help="Don't delete branches removed in Subversion."),
                     Option('incremental',
                         help="Import revisions incrementally."),
                     Option('prefix', type=str, 
                         help='Only consider branches of which path starts '
                              'with prefix.'),
                     Option('until', type=int,
                         help="Only import revisions up to specified Subversion revnum"),
                    ]

    def run(self, from_location, to_location=None, trees=False, 
            standalone=False, layout=None, all=False, prefix=None, keep=False,
            incremental=False, until=None):
        from bzrlib.bzrdir import BzrDir
        from bzrlib.errors import BzrCommandError, NoRepositoryPresent
        from bzrlib import osutils, urlutils
        from bzrlib.plugins.svn.convert import convert_repository
        from bzrlib.plugins.svn.remote import SvnRemoteAccess
        from bzrlib.plugins.svn.repository import SvnRepository
        from bzrlib.plugins.svn.workingtree import SvnCheckout
        from bzrlib.trace import info
        import os
        from subvertpy import NODE_NONE

        if to_location is None:
            to_location = os.path.basename(from_location.rstrip("/\\"))

        if all:
            # All implies shared repository 
            # (otherwise there is no repository to store revisions in)
            standalone = False

        if os.path.isfile(from_location):
            from bzrlib.plugins.svn.convert import load_dumpfile
            import tempfile
            tmp_repos = tempfile.mkdtemp(prefix='bzr-svn-dump-')
            load_dumpfile(from_location, tmp_repos)
            from_location = tmp_repos
        else:
            tmp_repos = None

        from_dir = BzrDir.open(from_location)

        if not (isinstance(from_dir, SvnRemoteAccess) or 
                isinstance(from_dir, SvnCheckout)):
            raise BzrCommandError("Source repository is not a Subversion repository.")

        try:
            from_repos = from_dir.open_repository()
        except NoRepositoryPresent, e:
            if prefix is not None:
                raise BzrCommandError("Path inside repository specified "
                                      "and --prefix specified")
            from_repos = from_dir.find_repository(_ignore_branch_path=True)
            assert from_dir.root_transport.base.startswith(from_repos.base)
            prefix = from_dir.root_transport.base[len(from_repos.base):].strip("/")
            prefix = prefix.encode("utf-8")

        if not isinstance(from_repos, SvnRepository):
            raise BzrCommandError(
                    "Not a Subversion repository: %s" % from_location)

        if until is None:
            to_revnum = from_repos.get_latest_revnum()
        else:
            to_revnum = min(until, from_repos.get_latest_revnum())

        from_repos.lock_read()
        try:

            if prefix is not None:
                if layout is None:
                    overall_layout = from_repos.get_guessed_layout()
                else:
                    overall_layout = layout
                prefix = prefix.strip("/") + "/"
                if overall_layout.is_branch(prefix):
                    raise BzrCommandError("%s appears to contain a branch. " 
                            "For individual branches, use 'bzr branch'." % 
                            from_location)
                # FIXME: Hint about is_tag()
                elif overall_layout.is_branch_parent(prefix):
                    self.outf.write("Importing branches with prefix /%s\n" % 
                        urlutils.unescape_for_display(prefix, self.outf.encoding))
                else:
                    raise BzrCommandError("The specified path is inside a branch. "
                        "Specify a different URL or a different repository layout (see also 'bzr help svn-layout').")

            if (prefix is not None and 
                from_repos.transport.check_path(prefix, to_revnum) == NODE_NONE):
                raise BzrCommandError("Prefix %s does not exist" % prefix)

            def filter_branch(branch):
                if (prefix is not None and 
                    not branch.get_branch_path().startswith(prefix)):
                    return False
                return True

            info("Using repository layout: %s" % (layout or from_repos.get_layout(),))
            convert_repository(from_repos, to_location, layout, 
                               not standalone, trees, all, 
                               filter_branch=filter_branch,
                               keep=keep, incremental=incremental,
                               to_revnum=to_revnum, prefix=prefix)

            if tmp_repos is not None:
                osutils.rmtree(tmp_repos)
        finally:
            from_repos.unlock()


class cmd_svn_set_revprops(Command):
    """Migrate Bazaar metadata to Subversion revision properties.

    This requires that you have permission to change the 
    revision properties on the repository.

    To change these permissions, edit the hooks/pre-revprop-change 
    file in the Subversion repository. 
    """
    takes_args = ['location?']
    from bzrlib.plugins.svn.mapping import mapping_registry
    takes_options = [RegistryOption('mapping', 
                                 help="New mapping to upgrade to.",
                                 registry=mapping_registry,
                                 title="Subversion mapping",
                                 value_switches=True),
                     Option('upgrade', help='Upgrade to new mapping version rather than setting revision properties for the current mapping')]

    def run(self, location=".", upgrade=False, mapping=None):
        from bzrlib.errors import BzrCommandError
        from bzrlib.repository import Repository
        from bzrlib.plugins.svn.upgrade import set_revprops, upgrade_revprops
        from bzrlib.plugins.svn.mapping import mapping_registry
        repos = Repository.open(location) 
        if not repos.transport.has_capability("commit-revprops"):
            raise BzrCommandError("Please upgrade the Subversion server to 1.5 or higher.")
        if mapping is None:
            mapping = mapping_registry.get_default()
        new_mapping = mapping.from_repository(repos)
        if not new_mapping.can_use_revprops:
            raise BzrCommandError("Please specify a different mapping, %s doesn't support revision properties." % new_mapping.name)

        if upgrade:
            num = upgrade_revprops(repos, new_mapping)
        else:
            num = set_revprops(repos)
        self.outf.write("Revision properties set for %d revisions.\n" % num)
        self.outf.write("Please restore the hooks/pre-revprop-change script "
                        "to refuse changes to most revision properties.\n")


class cmd_svn_layout(Command):
    """Print the repository layout in use for a repository.

    This will print the name of the repository layout. See 
    "bzr help svn-layout" for more information about repository 
    layouts.
    """
    takes_args = ["path?"]

    def run(self, path="."):
        from bzrlib import (
            errors,
            urlutils,
            )
        from bzrlib.branch import Branch
        from bzrlib.repository import Repository

        try:
            branch, _ = Branch.open_containing(path)
            repos = branch.repository
        except errors.NotBranchError:
            repos = Repository.open(path)
            branch = None
        layout = repos.get_layout()
        self.outf.write("Repository root: %s\n" % repos.base)
        self.outf.write("Layout: %s\n" % str(layout))
        if branch is not None:
            self.outf.write("Branch path: %s\n" % branch.get_branch_path())
            if branch.project:
                self.outf.write("Project: %s\n" % branch.project)
            self.outf.write("Tag container directory: %s\n" % 
                    urlutils.dirname(layout.get_tag_path("test", branch.project)))
            self.outf.write("Branch container directory: %s\n" % 
                    urlutils.dirname(layout.get_branch_path("test", branch.project)))


class cmd_svn_serve(Command):
    """Provide access to a Bazaar branch using the Subversion ra_svn protocol.

    This command is experimental and doesn't support incremental updates 
    properly yet.
    """
    takes_options = [
        Option('inet',
               help='serve on stdin/out for use from inetd or sshd'),
        Option('port',
               help='listen for connections on nominated port of the form '
                    '[hostname:]portnumber. Passing 0 as the port number will '
                    'result in a dynamically allocated port.',
               type=str),
        Option('directory',
               help='serve contents of directory',
               type=unicode)
    ]

    def run(self, inet=None, port=None, directory=None):
        from bzrlib.plugins.svn import lazy_check_versions
        lazy_check_versions()
        from subvertpy.ra_svn import SVNServer, TCPSVNServer, SVN_PORT
        from bzrlib.plugins.svn.server import BzrServerBackend
        from bzrlib.trace import warning
        import os
        import sys

        warning("server support in bzr-svn is experimental.")

        if directory is None:
            directory = os.getcwd()

        backend = BzrServerBackend(directory)
        if inet:
            def send_fn(data):
                sys.stdout.write(data)
                sys.stdout.flush()
            server = SVNServer(backend, sys.stdin.read, send_fn, 
                               self.outf)
        else:
            if port is None:
                port = SVN_PORT
            server = TCPSVNServer(backend, ('0.0.0.0', port), self.outf)
        server.serve()
