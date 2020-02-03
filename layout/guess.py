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

from __future__ import absolute_import

from subvertpy import SubversionException, ERR_FS_NOT_FOUND
import urllib

from .... import ui
from ....trace import mutter

from ..changes import (
    common_prefix,
    )

from .standard import (
    CustomLayout,
    RootLayout,
    TrunkLayout,
    )

# Number of revisions to evaluate when guessing the repository layout
GUESS_SAMPLE_SIZE = 300


def find_commit_paths(changed_paths):
    """Find the commit-paths used in a bunch of revisions.

    :param changed_paths:
        List of changed_paths (dictionary with path -> action)
    :return: List of potential commit paths.
    """
    for changes in changed_paths:
        yield common_prefix(list(changes.keys()))


def guess_layout_from_branch_path(relpath):
    """Try to guess the branching layout from a branch path.

    :param relpath: Relative URL to a branch.
    :return: New Branchinglayout instance.
    """
    parts = relpath.strip(u"/").split(u"/")
    for i in range(0, len(parts)):
        if parts[i] == u"trunk" and i == len(parts)-1:
            return TrunkLayout(level=i)
        elif parts[i] in (u"branches", u"tags") and i == len(parts)-2:
            return TrunkLayout(level=i)

    if len(parts) == 1 and parts[0] == u"wiki":
        # Google code keeps the wiki in Subversion
        return TrunkLayout(level=0)

    if parts == [u""]:
        return RootLayout()
    return CustomLayout([relpath])


def guess_layout_from_path(relpath):
    """Try to guess the branching layout from a path in the repository,
    not necessarily a branch path.

    :param relpath: Relative path in repository
    :return: New Branchinglayout instance.
    """
    parts = relpath.strip(u"/").split(u"/")
    for i in range(0, len(parts)):
        if parts[i] == u"trunk":
            return TrunkLayout(level=i)
        elif parts[i] in (u"branches", u"tags"):
            return TrunkLayout(level=i)

    if len(parts) > 0 and parts[0] == u"wiki":
        # Google code keeps the wiki in Subversion
        return TrunkLayout(level=0)

    return RootLayout()


def guess_layout_from_history(changed_paths, last_revnum, relpath=None):
    """Try to determine the best fitting layout.

    :param changed_paths: Iterator over (branch_path, changes, revnum,
        revprops) as returned from LogWalker.iter_changes().
    :param last_revnum: Number of entries in changed_paths.
    :param relpath: Branch path that should be accepted by the branching
                    scheme as a branch.
    :return: Tuple with layout that best matches history and
             layout instance that best matches but also considers
             relpath a valid branch path.
    """
    potentials = {}
    layout_cache = {}
    with ui.ui_factory.nested_progress_bar() as pb:
        for (revpaths, revnum, revprops) in changed_paths:
            assert isinstance(revpaths, dict)
            pb.update("analyzing repository layout", last_revnum-revnum,
                      last_revnum)
            if revnum == 0 or revpaths == {}:
                continue
            for path in find_commit_paths([revpaths]):
                layout = guess_layout_from_path(path)
                if str(layout) not in potentials:
                    potentials[str(layout)] = 0
                potentials[str(layout)] += 1
                layout_cache[str(layout)] = layout

    entries = sorted(potentials.items(), key=lambda e: e[1])

    mutter('potential branching layouts: %r' % entries)

    if len(entries) > 0:
        best_match = layout_cache[entries[0][0]]
    else:
        best_match = None

    if relpath is None and best_match is not None:
        return (best_match, best_match)

    for (layoutname, _) in entries:
        layout = layout_cache[layoutname]
        if layout.is_branch(relpath):
            return (best_match, layout)

    if relpath == u"" or relpath is None:
        guessed_layout = TrunkLayout()
    else:
        guessed_layout = guess_layout_from_branch_path(relpath)

    return (best_match, guessed_layout)


def repository_guess_layout(repository, revnum, branch_path=None):
    return logwalker_guess_layout(repository._log, revnum,
        branch_path=branch_path)


def logwalker_guess_layout(logwalker, revnum, branch_path=None):
    with ui.ui_factory.nested_progress_bar() as pb:
        (guessed_layout, layout) = guess_layout_from_history(
            logwalker.iter_changes(None, revnum,
                max(0, revnum-GUESS_SAMPLE_SIZE)), revnum, branch_path)
    mutter("Guessed repository layout: %r, guess layout to use: %r" %
            (guessed_layout, layout))
    return (guessed_layout, layout)


def is_likely_branch_url(url):
    """Check whether url refers to a likely branch URL.

    """
    from ..transport import SvnRaTransport
    from ..logwalker import LogWalker
    transport = SvnRaTransport(url)
    lw = LogWalker(transport=transport)
    svn_root_url = transport.get_repos_root()
    branch_path = urllib.unquote(url[len(svn_root_url):])
    try:
        (guessed_layout, _) = logwalker_guess_layout(
            lw, transport.get_latest_revnum())
    except SubversionException as e:
        if e.args[1] == ERR_FS_NOT_FOUND:
            return False  # path doesn't exist
        raise
    if guessed_layout is None:
        return None
    return guessed_layout.is_branch(branch_path)
