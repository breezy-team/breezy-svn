# Copyright (C) 2006 Jelmer Vernooij <jelmer@samba.org>

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
"""Branching scheme implementations."""

from bzrlib import ui, urlutils
from bzrlib.errors import BzrError
from bzrlib.trace import mutter

import base64
import bz2
from subvertpy import properties
import urllib

from bzrlib.plugins.svn.layout.guess import find_commit_paths
from bzrlib.plugins.svn.layout.standard import (
        CustomLayout,
        RootLayout,
        TrunkLayout,
        )
from bzrlib.plugins.svn.errors import LayoutUnusable


class InvalidSvnBranchPath(BzrError):
    """Error raised when a path was specified that is not a child of or itself
    a valid branch path in the current branching scheme."""
    _fmt = """%(path)s is not a valid Subversion branch path in the current 
repository layout. See 'bzr help svn-repository-layout' for details."""

    def __init__(self, path, layout):
        assert isinstance(path, str)
        BzrError.__init__(self)
        self.path = urllib.quote(path)
        self.layout = layout


class BranchingScheme(object):
    """ Divides SVN repository data up into branches. Since there
    is no proper way to do this, there are several subclasses of this class
    each of which handles a particular convention that may be in use.
    """
    def is_branch(self, path, project=None):
        """Check whether a location refers to a branch.
        
        :param path: Path to check.
        """
        raise NotImplementedError

    def unprefix(self, path):
        """Split up a Subversion path into a branch-path and inside-branch path.

        :param path: Path to split up.
        :return: Tuple with project name,branch-path and inside-branch path.
        """
        raise NotImplementedError

    def get_tag_path(self, name, project=""):
        """Find the path for a tag.

        :param name: Tag name.
        """
        raise NotImplementedError

    def get_branch_path(self, name, project=""):
        """Find the path for a named branch.

        :param name: Branch name.
        """
        raise NotImplementedError

    @staticmethod
    def find_scheme(name):
        """Find a branching scheme by name.

        :param name: Name of branching scheme.
        :return: Branching scheme instance.
        """
        assert isinstance(name, str)
        if name.startswith("trunk"):
            if name == "trunk":
                return TrunkBranchingScheme()
            try:
                return TrunkBranchingScheme(level=int(name[len("trunk"):]))
            except ValueError:
                raise UnknownBranchingScheme(name)

        if name == "none":
            return NoBranchingScheme()

        if name.startswith("single-"):
            return SingleBranchingSchemev0(name[len("single-"):])

        if name.startswith("single1-"):
            return SingleBranchingScheme(encoded=name[len("single1-"):])

        if name.startswith("list-"):
            return ListBranchingScheme(name[len("list-"):])

        raise UnknownBranchingScheme(name)

    def is_branch_parent(self, path, project=None):
        """Check whether the specified path is the parent directory of branches.
        The path may not be a branch itself.
        
        :param path: path to check
        :returns: boolean
        """
        raise NotImplementedError

    def is_tag_parent(self, path, project=None):
        """Check whether the specified path is the parent directory of tags.
        The path may not be a tag itself.
        
        :param path: path to check
        :returns: boolean
        """
        raise NotImplementedError

    def is_tag(self, path, project=None):
        """Check whether the specified path is a tag 
        according to this branching scheme.

        :param path: path to check
        :return: boolean
        """
        raise NotImplementedError

    def to_lines(self):
        """Generate a list of lines for this branching scheme.

        :return: List of lines representing the data in this branching 
            scheme.
        """
        raise NotImplementedError(self.to_lines)


def parse_list_scheme_text(text):
    """Parse a text containing the branches for a ListBranchingScheme.

    :param text: Text.
    :return: List of branch paths.
    """
    branches = []
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        branches.append(line.strip("/"))
    return branches


def prop_name_unquote(text):
    return base64.urlsafe_b64decode(text.replace(".", "="))


def prop_name_quote(text):
    return base64.urlsafe_b64encode(text).replace("=", ".")


class ListBranchingScheme(BranchingScheme):
    """Branching scheme that keeps a list of branch paths, including 
    wildcards."""
    def __init__(self, branch_list, tag_list=[]):
        """Create new ListBranchingScheme instance.

        :param branch_list: List of know branch locations.
        """
        assert isinstance(branch_list, list) or isinstance(branch_list, str)
        if isinstance(branch_list, str):
            branch_list = bz2.decompress(prop_name_unquote(branch_list.encode("ascii"))).splitlines()
        self.branch_list = [p.strip("/") for p in branch_list]
        self.tag_list = tag_list
        self.split_branch_list = [p.split("/") for p in self.branch_list]

    def __str__(self):
        return "list-%s" % prop_name_quote(bz2.compress("".join(map(lambda x:x+"\n", self.branch_list))))
            
    def is_tag(self, path, project=None):
        """See BranchingScheme.is_tag()."""
        return False

    @staticmethod
    def _pattern_cmp(parts, pattern):
        if len(parts) != len(pattern):
            return False
        for (p, q) in zip(pattern, parts):
            if p != q and p != "*":
                return False
        return True

    def is_branch(self, path, project=None):
        """See BranchingScheme.is_branch()."""
        parts = path.strip("/").split("/")
        for pattern in self.split_branch_list:
            if self._pattern_cmp(parts, pattern):
                return True
        return False

    def unprefix(self, path):
        """See BranchingScheme.unprefix()."""
        assert isinstance(path, str)
        parts = path.strip("/").split("/")
        for pattern in self.split_branch_list:
            if self._pattern_cmp(parts[:len(pattern)], pattern):
                return ("/".join(parts[:len(pattern)]), 
                        "/".join(parts[:len(pattern)]), 
                        "/".join(parts[len(pattern):]))
        raise InvalidSvnBranchPath(path, self)

    def __eq__(self, other):
        return (self.branch_list == other.branch_list and \
                self.tag_list == other.tag_list)

    def __ne__(self, other):
        return not self.__eq__(other)

    def to_lines(self):
        return self.branch_list

    def is_tag_parent(self, path, project=None):
        # ListBranchingScheme doesn't have tags
        return False

    def is_branch_parent(self, path, project=None):
        parts = path.strip("/").split("/")
        for pattern in self.split_branch_list:
            if len(parts) == len(pattern):
                continue
            if self._pattern_cmp(parts, pattern[0:len(parts)]):
                return True
        return False


class NoBranchingScheme(ListBranchingScheme):
    """Describes a scheme where there is just one branch, the 
    root of the repository."""
    def __init__(self):
        ListBranchingScheme.__init__(self, [""])

    def is_branch(self, path, project=None):
        """See BranchingScheme.is_branch()."""
        if project is None or project == "":
            return path.strip("/") == ""
        return False

    def is_tag(self, path, project=None):
        return False

    def unprefix(self, path):
        """See BranchingScheme.unprefix()."""
        assert isinstance(path, str)
        return ("", "", path.strip("/"))

    def __str__(self):
        return "none"

    def __repr__(self):
        return "%s()" % self.__class__.__name__

    def is_branch_parent(self, path, project=None):
        return False

    def is_tag_parent(self, path, project=None):
        return False


class TrunkBranchingScheme(ListBranchingScheme):
    """Standard Subversion repository layout. 
    
    Each project contains three directories `trunk`, `tags` and `branches`. 
    """
    def __init__(self, level=0):
        self.level = level
        ListBranchingScheme.__init__(self,
            ["*/" * level + "trunk",
             "*/" * level + "branches/*"])
        self.tag_list = ["*/" * level + "tags/*"]

    def get_tag_path(self, name, project=""):
        assert isinstance(name, unicode)
        if project == "":
            return urlutils.join("tags", name.encode("utf-8"))
        return urlutils.join(project, "tags", name.encode("utf-8"))

    def get_branch_path(self, name, project=""):
        # Only implemented for level 0
        if name == "trunk":
            if project == "":
                return "trunk"
            else:
                return urlutils.join(project, "trunk")
        else:
            if project == "":
                return urlutils.join("branches", name)
            else:
                return urlutils.join(project, "branches", name)

    def is_branch(self, path, project=None):
        """See BranchingScheme.is_branch()."""
        parts = path.strip("/").split("/")

        if project is not None and project != "/".join(parts[0:self.level]):
            return False

        if len(parts) == self.level+1 and parts[self.level] == "trunk":
            return True

        if len(parts) == self.level+2 and parts[self.level] == "branches":
            return True

        return False

    def is_tag(self, path, project=None):
        """See BranchingScheme.is_tag()."""
        parts = path.strip("/").split("/")

        if project is not None and project != "/".join(parts[0:self.level]):
            return False

        if len(parts) == self.level+2 and \
           (parts[self.level] == "tags"):
            return True

        return False

    def unprefix(self, path):
        """See BranchingScheme.unprefix()."""
        assert isinstance(path, str)
        parts = path.strip("/").split("/")
        if len(parts) == 0 or self.level >= len(parts):
            raise InvalidSvnBranchPath(path, self)

        if parts[self.level] == "trunk" or parts[self.level] == "hooks":
            return ("/".join(parts[0:self.level]).strip("/"),
                    "/".join(parts[0:self.level+1]).strip("/"), 
                    "/".join(parts[self.level+1:]).strip("/"))
        elif ((parts[self.level] == "tags" or parts[self.level] == "branches") and 
              len(parts) >= self.level+2):
            return ("/".join(parts[0:self.level]).strip("/"),
                    "/".join(parts[0:self.level+2]).strip("/"), 
                    "/".join(parts[self.level+2:]).strip("/"))
        else:
            raise InvalidSvnBranchPath(path, self)

    def __str__(self):
        return "trunk%d" % self.level

    def __repr__(self):
        return "%s(%d)" % (self.__class__.__name__, self.level)

    def is_branch_parent(self, path, project=None):
        parts = path.strip("/").split("/")
        if len(parts) <= self.level:
            return True
        return self.is_branch(path+"/trunk", project)

    def is_tag_parent(self, path, project=None):
        parts = path.strip("/").split("/")
        return self.is_tag(path+"/aname", project)



class UnknownBranchingScheme(BzrError):
    _fmt = "Branching scheme could not be found: %(name)s"

    def __init__(self, name):
        self.name = name


class SingleBranchingScheme(ListBranchingScheme):
    """Recognizes just one directory in the repository as branch.
    """
    def __init__(self, path=None, encoded=None):
        if encoded is not None:
            path = prop_name_unquote(encoded)
        self.path = path.strip("/")
        if self.path == "":
            raise BzrError("NoBranchingScheme should be used")
        ListBranchingScheme.__init__(self, [self.path])

    def is_branch(self, path, project=None):
        """See BranchingScheme.is_branch()."""
        return self.path == path.strip("/")

    def is_tag(self, path, project=None):
        """See BranchingScheme.is_tag()."""
        return False

    def unprefix(self, path):
        """See BranchingScheme.unprefix()."""
        assert isinstance(path, str)
        path = path.strip("/")
        if not path.startswith(self.path):
            raise InvalidSvnBranchPath(path, self)

        return (self.path, 
                path[0:len(self.path)].strip("/"), 
                path[len(self.path):].strip("/"))

    def __str__(self):
        if properties.is_valid_property_name(self.path):
            return "single-%s" % self.path
        else:
            return "single1-%s" % prop_name_quote(self.path)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.path)

    def is_branch_parent(self, path, project=None):
        if not "/" in self.path:
            return False
        return self.is_branch(path+"/"+self.path.split("/")[-1], project=None)

    def is_tag_parent(self, path, project=None):
        return False


class SingleBranchingSchemev0(SingleBranchingScheme):
    """Version of SingleBranchingSchemev0 that *never* quotes.
    """
    def __init__(self, path=None, allow_quotes=True):
        SingleBranchingScheme.__init__(self, path)

    def __str__(self):
        return "single-%s" % self.path


def guess_scheme_from_branch_path(relpath):
    """Try to guess the branching scheme from a branch path.

    :param relpath: Relative URL to a branch.
    :return: New BranchingScheme instance.
    """
    parts = relpath.strip("/").split("/")
    for i in range(0, len(parts)):
        if parts[i] == "trunk" and i == len(parts)-1:
            return TrunkBranchingScheme(level=i)
        elif parts[i] in ("branches", "tags") and i == len(parts)-2:
            return TrunkBranchingScheme(level=i)

    if parts == [""]:
        return NoBranchingScheme()
    return SingleBranchingScheme(relpath)


def guess_scheme_from_path(relpath):
    """Try to guess the branching scheme from a path in the repository, 
    not necessarily a branch path.

    :param relpath: Relative path in repository
    :return: New BranchingScheme instance.
    """
    parts = relpath.strip("/").split("/")
    for i in range(0, len(parts)):
        if parts[i] == "trunk":
            return TrunkBranchingScheme(level=i)
        elif parts[i] in ("branches", "tags"):
            return TrunkBranchingScheme(level=i)

    return NoBranchingScheme()


def guess_scheme_from_history(changed_paths, last_revnum, 
                              relpath=None):
    """Try to determine the best fitting branching scheme.

    :param changed_paths: Iterator over (branch_path, changes, revnum, revprops)
        as returned from LogWalker.iter_changes().
    :param last_revnum: Number of entries in changed_paths.
    :param relpath: Branch path that should be accepted by the branching 
                    scheme as a branch.
    :return: Tuple with branching scheme that best matches history and 
             branching scheme instance that best matches but also considers
             relpath a valid branch path.
    """
    potentials = {}
    pb = ui.ui_factory.nested_progress_bar()
    scheme_cache = {}
    try:
        for (revpaths, revnum, revprops) in changed_paths:
            assert isinstance(revpaths, dict)
            pb.update("analyzing repository layout", last_revnum-revnum, 
                      last_revnum)
            if revpaths == {}:
                continue
            for path in find_commit_paths([revpaths]):
                scheme = guess_scheme_from_path(path)
                if not potentials.has_key(str(scheme)):
                    potentials[str(scheme)] = 0
                potentials[str(scheme)] += 1
                scheme_cache[str(scheme)] = scheme
    finally:
        pb.finished()
    
    entries = potentials.items()
    entries.sort(lambda (a, b), (c, d): d - b)

    mutter('potential branching schemes: %r' % entries)

    if len(entries) > 0:
        best_match = scheme_cache[entries[0][0]]
    else:
        best_match = None

    if relpath is None:
        if best_match is None:
            return (None, NoBranchingScheme())
        return (best_match, best_match)

    for (schemename, _) in entries:
        scheme = scheme_cache[schemename]
        if scheme.is_branch(relpath):
            return (best_match, scheme)

    return (best_match, guess_scheme_from_branch_path(relpath))


def scheme_from_branch_list(branch_list):
    """Determine a branching scheme for a branch list.

    :param branch_list: List of branch paths, may contain wildcards.
    :return: New branching scheme.
    """
    if branch_list == ["."] or branch_list == []:
        return NoBranchingScheme()
    if branch_list == TrunkBranchingScheme(0).branch_list:
        return TrunkBranchingScheme(0)
    return ListBranchingScheme(branch_list) 


def scheme_from_layout(layout):
    if getattr(layout, "scheme", None) is not None:
        return layout.scheme
    if isinstance(layout, TrunkLayout):
        return TrunkBranchingScheme(layout.level or 0)
    if isinstance(layout, RootLayout):
        return NoBranchingScheme()
    if isinstance(layout, CustomLayout) and len(layout.branches) == 1:
        return SingleBranchingScheme(layout.branches[0])
    raise LayoutUnusable(layout, self)


help_schemes = """Subversion Branching Schemes

Subversion is basically a versioned file system. It does not have 
any notion of branches and what is a branch in Subversion is therefor
up to the user. 

In order for Bazaar to access a Subversion repository it has to know 
what paths to consider branches. It does this by using so-called branching 
schemes. When you connect to a repository for the first time, Bazaar
will try to determine the branching scheme to use using some simple 
heuristics. It is always possible to change the branching scheme it should 
use later.

There are some conventions in use in Subversion for repository layouts. 
The most common one is probably the trunk/branches/tags 
layout, where the repository contains a "trunk" directory with the main 
development branch, other branches in a "branches" directory and tags as 
subdirectories of a "tags" directory. This branching scheme is named 
"trunk" in Bazaar.

Another option is simply having just one branch at the root of the repository. 
This scheme is called "none" by Bazaar.

The branching scheme bzr-svn should use for a repository can be set in the 
configuration file ~/.bazaar/subversion.conf.

Branching schemes are only used for version 3 of the Bzr<->Svn mappings.
"""
