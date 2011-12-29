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


from bzrlib import (
    bzrdir,
    )
from bzrlib.commands import (
    Command,
    )
from bzrlib.option import (
    RegistryOption,
    Option,
    )

from bzrlib.plugins.svn import gettext


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
        raise BzrCommandError(gettext('No such repository layout %r') %
                layoutname)
    return ret


class cmd_svn_import(Command):
    """Convert a Subversion repository to a Bazaar repository.

    To save disk space, only branches will be created by default
    (no working trees). To create a tree for a branch, run "bzr co" in
    it.

    This command is resumable; any previously imported revisions will be
    skipped.
    """
    _see_also = ['formats']
    takes_args = ['from_location', 'to_location?']
    takes_options = [RegistryOption('format',
                            help='Specify a format for this repository. See'
                                 ' "bzr help formats" for details. Must support rich-root.',
                            lazy_registry=('bzrlib.bzrdir', 'format_registry'),
                            converter=lambda name: bzrdir.format_registry.make_bzrdir(name),
                            value_switches=False, title='Repository format'),
                     Option('trees', help='Create working trees.'),
                     Option('standalone', help='Create standalone branches.'),
                     Option('all',
                         help='Convert all revisions, even those not in '
                              'current branch history.'),
                     Option('layout', type=get_layout,
                         help='Repository layout (none, trunk, etc). '
                              'Default: auto.'),
                     Option('keep',
                         help="Don't delete branches removed in Subversion."),
                     Option('restore',
                         help="Restore branches that were removed but have "
                              "not been changed since the last import."),
                     Option('prefix', type=str,
                         hidden=True,
                         help='Only consider branches of which path starts '
                              'with prefix.'),
                     Option('until', type=int,
                         help="Only import revisions up to specified Subversion revnum"),
                    ]

    def run(self, from_location, to_location=None, format=None, trees=False,
            standalone=False, layout=None, all=False, prefix=None, keep=False,
            restore=False, until=None):
        from bzrlib import (
            osutils,
            trace,
            urlutils,
            )
        from bzrlib.bzrdir import BzrDir
        from bzrlib.errors import (
            BzrCommandError,
            NoRepositoryPresent,
            )
        from bzrlib.plugins.svn import gettext
        from bzrlib.plugins.svn.convert import convert_repository
        from bzrlib.plugins.svn.remote import SvnRemoteAccess
        from bzrlib.plugins.svn.repository import SvnRepository
        from bzrlib.plugins.svn.workingtree import SvnCheckout
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
            raise BzrCommandError(gettext(
                "Source repository is not a Subversion repository."))

        try:
            from_repos = from_dir.open_repository()
        except NoRepositoryPresent, e:
            if prefix is not None:
                raise BzrCommandError(gettext("Path inside repository specified "
                                      "and --prefix specified"))
            from_repos = from_dir.find_repository(_ignore_branch_path=True)
            assert from_dir.root_transport.base.startswith(from_repos.base)
            prefix = from_dir.root_transport.base[len(from_repos.base):].strip("/")
            prefix = prefix.encode("utf-8")

        if not isinstance(from_repos, SvnRepository):
            raise BzrCommandError(
                    gettext("Not a Subversion repository: %s") % from_location)

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
                    raise BzrCommandError(gettext("%s appears to contain a branch. "
                            "For individual branches, use 'bzr branch'.") %
                            from_location)
                # FIXME: Hint about is_tag()
                elif overall_layout.is_branch_parent(prefix):
                    self.outf.write(gettext(gettext("Importing branches with prefix %s\n")) %
                        ("/" + urlutils.unescape_for_display(prefix, self.outf.encoding)))
                else:
                    raise BzrCommandError(gettext("The specified path is inside a branch. "
                        "Specify a different URL or a different repository layout (see also 'bzr help svn-layout')."))

            if (prefix is not None and
                from_repos.transport.check_path(prefix, to_revnum) == NODE_NONE):
                raise BzrCommandError("Prefix %s does not exist" % prefix)

            def filter_branch(branch):
                if (prefix is not None and
                    not branch.get_branch_path().startswith(prefix)):
                    return False
                return True

            trace.note(gettext("Using repository layout: %s"),
                       layout or from_repos.get_layout())
            convert_repository(from_repos, to_location, layout,
                not standalone, trees, all, format=format,
                filter_branch=filter_branch, keep=keep,
                incremental=not restore, to_revnum=to_revnum, prefix=prefix)

            if tmp_repos is not None:
                osutils.rmtree(tmp_repos)
            if not trees:
                trace.note(
                    gettext("Use 'bzr checkout' to create a working tree in "
                            "the newly created branches."))
        finally:
            from_repos.unlock()


class cmd_svn_layout(Command):
    """Print the repository layout in use for a Subversion repository.

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
        from bzrlib.plugins.svn import gettext
        from bzrlib.repository import Repository
        from bzrlib.plugins.svn import errors as bzrsvn_errors

        try:
            branch, _ = Branch.open_containing(path)
            repos = branch.repository
        except (errors.NotBranchError, errors.NoColocatedBranchSupport):
            repos = Repository.open(path)
            branch = None
        if getattr(repos, "uuid", None) is None:
            raise errors.BzrCommandError(
                gettext("Not a Subversion branch or repository."))
        layout = repos.get_layout()
        self.outf.write(gettext("Repository root: %s\n") % repos.base)
        self.outf.write(gettext("Layout: %s\n") % str(layout))
        if branch is not None:
            self.outf.write(gettext("Branch path: %s\n") % branch.get_branch_path())
            if branch.project:
                self.outf.write(gettext("Project: %s\n") % branch.project)
            try:
                test_tag_path = layout.get_tag_path("test", branch.project)
            except bzrsvn_errors.NoLayoutTagSetSupport:
                self.outf.write(gettext("No tag support\n"))
            else:
                if test_tag_path:
                    self.outf.write(gettext("Tag container directory: %s\n") %
                            urlutils.dirname(test_tag_path))
            try:
                test_branch_path = layout.get_branch_path("test",
                    branch.project)
            except bzrsvn_errors.NoCustomBranchPaths:
                self.outf.write(gettext("No custom branch support\n"))
            else:
                if test_branch_path:
                    self.outf.write(
                        gettext("Branch container directory: %s\n" %
                            urlutils.dirname(test_branch_path)))
            self.outf.write(gettext("Push merged revisions: %s\n") %
                    branch.get_push_merged_revisions())


class cmd_svn_branches(Command):
    """Print the branches and tags in a repository.

    """

    takes_args = ["location"]

    takes_options = [
        Option('layout', type=get_layout,
               help='Repository layout (none, trunk, etc). '
                    'Default: auto.')]

    hidden = True

    def run(self, location, layout=None):
        from bzrlib import errors
        from bzrlib.bzrdir import BzrDir
        from bzrlib.plugins.svn import gettext
        from bzrlib.plugins.svn.remote import SvnRemoteAccess
        from bzrlib.plugins.svn.workingtree import SvnCheckout

        dir = BzrDir.open(location)

        if not (isinstance(dir, SvnRemoteAccess) or
                isinstance(dir, SvnCheckout)):
            raise errors.BzrCommandError(
                gettext("Source repository is not a Subversion repository."))

        try:
            repository = dir.open_repository()
        except errors.NoRepositoryPresent, e:
            repository = dir.find_repository(_ignore_branch_path=True)
            assert dir.root_transport.base.startswith(repository.base)
            prefix = dir.root_transport.base[len(repository.base):].strip("/")
            prefix = prefix.encode("utf-8")
        else:
            prefix = None

        revnum = repository.get_latest_revnum()
        if layout is None:
            layout = repository.get_guessed_layout()

        self.outf.write(gettext("Branches:\n"))
        for (project, path, name, has_props, revnum) in layout.get_branches(repository, revnum, prefix):
            self.outf.write("%s (%s)\n" % (path, name))
        self.outf.write(gettext("Tags:\n"))
        for (project, path, name, has_props, revnum) in layout.get_tags(repository, revnum, prefix):
            self.outf.write("%s (%s)\n" % (path, name))


class cmd_fix_svn_ancestry(Command):
    """Fix the SVN ancestry of a repository.

    This will fix revisions that were imported from Subversion with older
    versions of bzr-svn but have some incorrect data.
    """

    takes_args = ['svn_repository']
    takes_options = [
        'directory',
        Option('no-reconcile', help="Don't reconcile the new repository.")]

    def run(self, svn_repository, directory=".", no_reconcile=False):
        from bzrlib.controldir import ControlDir
        from bzrlib.repository import InterRepository, Repository
        from bzrlib import trace
        correct_repo = ControlDir.open_containing(svn_repository)[0].find_repository()
        repo_to_fix = Repository.open(directory)
        revids = repo_to_fix.all_revision_ids()
        present_revisions = correct_repo.has_revisions(revids)
        dir_to_fix = repo_to_fix.bzrdir
        old_repo_format = repo_to_fix._format
        del repo_to_fix
        trace.note("Renaming existing repository to repository.backup.")
        dir_to_fix.control_transport.rename('repository', 'repository.backup')
        old_repo = old_repo_format.open(dir_to_fix, _found=True,
            _override_transport=dir_to_fix.control_transport.clone('repository.backup'))
        new_repo = dir_to_fix.create_repository()
        interrepo = InterRepository.get(correct_repo, new_repo)
        revisionfinder = interrepo.get_revision_finder(True)
        trace.note("Finding revisions to fetch from SVN")
        for revid in present_revisions:
            foreign_revid, mapping = correct_repo.lookup_bzr_revision_id(
                revid)
            revisionfinder.find_until(foreign_revid, mapping,
                find_ghosts=False, exclude_non_mainline=False)
        trace.note("Fetching correct SVN revisions")
        interrepo.fetch(needed=revisionfinder.get_missing())
        trace.note("Fetching other revisions")
        new_repo.fetch(old_repo)
        if not no_reconcile:
            from bzrlib.reconcile import reconcile
            trace.note("Reconciling new repository.")
            reconcile(dir_to_fix)
        trace.note('Removing backup')
        dir_to_fix.control_transport.delete_tree('repository.backup')
