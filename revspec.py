# Copyright (C) 2007-2009 Jelmer Vernooij <jelmer@samba.org>

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
"""Custom revision specifier for Subversion."""

from bzrlib.errors import (
    BzrError,
    InvalidRevisionId,
    InvalidRevisionSpec,
    NoSuchRevision,
    )
from bzrlib.revisionspec import (
    RevisionSpec,
    RevisionInfo,
    )

from bzrlib.plugins.svn import lazy_check_versions
from bzrlib.plugins.svn.errors import AmbiguousRevisionSpec
from bzrlib.plugins.svn.mapping import mapping_registry

class RevisionSpec_svn(RevisionSpec):
    """Selects a revision using a Subversion revision number."""

    help_txt = """Selects a revision using a Subversion revision number (revno).

    Subversion revision numbers are per-repository whereas Bazaar revision 
    numbers are per-branch. This revision specifier allows specifying 
    a Subversion revision number.

    This only works directly against Subversion branches.
    """
    
    prefix = 'svn:'

    def _get_revnum(self):
        loc = self.spec.find(':')
        return int(self.spec[loc+1:])

    def _match_on_foreign(self, branch):
        ret = set()
        revnum = self._get_revnum()
        branch.lock_read()
        try:
            graph = branch.repository.get_graph()
            for revid, _ in graph.iter_ancestry([branch.last_revision()]):
                try:
                    (found_uuid, found_branch_path, found_revnum), found_mapping = \
                            mapping_registry.parse_revision_id(revid)
                    if found_revnum == revnum:
                        ret.add(revid)
                except InvalidRevisionId:
                    continue
            if len(ret) == 1:
                revid = ret.pop()
                history = list(branch.repository.iter_reverse_revision_history(revid))
                history.reverse()
                return RevisionInfo.from_revision_id(branch, revid, history)
            elif len(ret) == 0:
                raise InvalidRevisionSpec(self.user_spec, branch)
            else:
                raise AmbiguousRevisionSpec(self.user_spec, branch)
        finally:
            branch.unlock()

    def _match_on_native(self, branch):
        try:
            return RevisionInfo.from_revision_id(branch, branch.generate_revision_id(self._get_revnum()), branch.revision_history())
        except ValueError:
            raise InvalidRevisionSpec(self.user_spec, branch)
        except NoSuchRevision:
            raise InvalidRevisionSpec(self.user_spec, branch)

    def _match_on(self, branch, revs):
        lazy_check_versions()
        uuid = getattr(branch.repository, 'uuid', None)
        if uuid is not None:
            return self._match_on_native(branch)
        return self._match_on_foreign(branch)

    def needs_branch(self):
        return True

    def get_branch(self):
        return None
