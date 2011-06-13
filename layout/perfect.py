from subvertpy import NODE_DIR

from bzrlib import urlutils
from bzrlib.plugins.svn import errors as svn_errors
from bzrlib.plugins.svn.layout import (
    RepositoryLayout,
    get_root_paths,
    wildcard_matches,
    )

class PerfectLayout(RepositoryLayout):
    def __init__(self, _log = None):
        if (_log is None):
            return
        print('PerfectLayout with log', _log )
        self.log = _log

    def supports_tags(self):
        return True

    def get_tag_path(self, name, project=""):
        """Return the path at which the tag with specified name should be found.

        :param name: Name of the tag.
        :param project: Optional name of the project the tag is for. Can include slashes.
        :return: Path of the tag.
        """
        return None

    def get_tag_name(self, path, project=""):
        """Determine the tag name from a tag path.

        :param path: Path inside the repository.
        """
        return None

    def parse(self, path):
        """Parse a path.

        :return: Tuple with type ('tag', 'branch'), project name, branch path and path
            inside the branch
        """

        raise svn_errors.NotSvnBranchPath(path)

    def get_branches(self, repository, revnum, project=None, pb=None):
        """Retrieve a list of paths that refer to branches in a specific revision.

        :return: Iterator over tuples with (project, branch path)
        """

    def get_tags(self, repository, revnum, project=None, pb=None):
        """Retrieve a list of paths that refer to tags in a specific revision.

        :return: Iterator over tuples with (project, branch path)
        """

    def __repr__(self):
        return "%s" % (self.__class__.__name__)

    def __str__(self):
        return "perfect"

    def _is_prefix(self, prefixes, path, project=None):
        for branch in prefixes:
            if branch.startswith("%s/" % path):
                return True
        return False
    
    def _get_branches(self):
        return []

    def _get_tags(self):
        return []

    def is_branch_parent(self, path, project=None):
        return self._is_prefix(self._get_branches(), path, project)

    def is_tag_parent(self, path, project=None):
        return self._is_prefix(self._get_tags(), path, project)
