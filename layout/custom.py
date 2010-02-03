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

from bzrlib import urlutils
from bzrlib.plugins.svn import errors as svn_errors
from bzrlib.plugins.svn.layout import (
    RepositoryLayout,
    RootPathFinder,
    )
from bzrlib.plugins.svn.layout.standard import (
    TrunkLayout,
    )

class KDELayout(RepositoryLayout):
    """Layout for the KDE repository."""

    def get_project_prefixes(self, project):
        sproject = project.strip("/").split("/")
        return [urlutils.join("trunk", sproject[0]),
                urlutils.join("branches", sproject[0]),
                urlutils.join("tags", sproject[0])]

    def __repr__(self):
        return "KDELayout()"

    def get_tag_path(self, name, project=None):
        if project is None:
            raise AssertionError
        p = project.strip("/").split("/")
        return urlutils.join("tags", p[0], name.encode("utf-8"), *p[1:])

    def _is_parent(self, path, project, kind):
        path = path.strip("/")
        if path == "":
            return True
        spath = path.strip("/").split("/")
        if len(spath) == 0:
            return True
        if spath[0] == "trunk":
            if kind != "branches":
                return False
            candidate = spath[1:]
        elif spath[0] == kind:
            candidate = [spath[1]] + spath[3:]
        else:
            return False
        if len(candidate) == 0:
            return True
        if candidate[0] == "KDE" and len(candidate) == 1:
            return True
        return False

    def is_branch_parent(self, path, project=""):
        return self._is_parent(path, project, "branches")

    def is_tag_parent(self, path, project=""):
        return self._is_parent(path, project, "tags")

    def get_tag_name(self, path, project=""):
        pts = path.strip("/").split("/")
        assert pts[0] == "tags"
        return pts[1]

    def push_merged_revisions(self, project=""):
        return False

    def get_branch_path(self, name, project=None):
        if project is None:
            raise AssertionError
        pts = project.strip("/").split("/")
        if name == "trunk":
            return urlutils.join("trunk", project)
        return urlutils.join("branches", pts[0], name, *pts[1:])

    def parse(self, path):
        pts = path.strip("/").split("/")
        if pts[0] == "trunk":
            first = pts[1]
            base = "trunk/%s" % (pts[1],)
            kind = "branch"
            rest = pts[2:]
        elif pts[0] == "branches":
            first = pts[1]
            base = "/".join(pts[:2])
            kind = "branch"
            rest = pts[3:]
        elif pts[0] == "tags":
            first = pts[1]
            base = "/".join(pts[:2])
            kind = "tag"
            rest = pts[3:]
        else:
            raise svn_errors.NotSvnBranchPath(path, self)
        if first == "KDE":
            cutoff = 2
        else:
            cutoff = 1
        projectpart = rest[:cutoff]
        ipath = "/".join(rest[cutoff:])
        return (kind, "/".join(projectpart), base+"/".join(projectpart[1:]), 
                ipath)

    def _children_helper(self, rpf, name, trunk=False):
        if trunk:
            return [("trunk", None)]
        else:
            return [(urlutils.join(name, subname), has_props) for (subname, has_props) in rpf.find_children(name)]

    def _get_project_items(self, name, repository, revnum, project, pb, trunk=False):
        ret = []
        rpf = RootPathFinder(repository, revnum) 
        children = self._children_helper(rpf, name, trunk)
        for subpath, has_props in children:
            cp = urlutils.join(subpath, project)
            if rpf.check_path(cp):
                ret.append((project, cp, urlutils.split(subpath)[-1], has_props))
        return ret

    def _get_all_items(self, name, repository, revnum, pb, trunk=False):
        ret = []
        rpf = RootPathFinder(repository, revnum) 
        children = self._children_helper(rpf, name, trunk)
        for subpath, _ in children:
            for p, has_props in rpf.find_children(subpath):
                pp = urlutils.join(subpath, p)
                if p == "KDE":
                    for kdep, has_props in rpf.find_children(pp):
                        ret.append(("KDE/" + kdep, urlutils.join(pp, kdep), urlutils.split(subpath)[-1], has_props))
                else:
                    ret.append((p, pp, urlutils.split(subpath)[-1], has_props))
        return ret

    def _get_items(self, name, repository, revnum, project, pb, trunk=False):
        if project is None:
            return self._get_all_items(name, repository, revnum, pb, trunk)
        else:
            return self._get_project_items(name, repository, revnum, project, pb, trunk)

    def get_branches(self, repository, revnum, project=None, pb=None):
        return self._get_items("branches", repository, revnum,
            project, pb, trunk=False) + self._get_items("trunk", repository,
                revnum, project, pb, trunk=True)

    def get_tags(self, repository, revnum, project=None, pb=None):
        return self._get_items("tags", repository, revnum, project, pb,
            trunk=False)

    def __str__(self):
        return "kde"


class ApacheLayout(TrunkLayout):
    """Layout for the Apache repository."""


