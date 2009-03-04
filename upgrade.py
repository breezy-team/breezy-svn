# Copyright (C) 2006-2009 by Jelmer Vernooij
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
"""Upgrading revisions made with older versions of the mapping."""

from itertools import ifilter
from subvertpy import (
    SubversionException,
    ERR_FS_NOT_DIRECTORY,
    properties,
    )

from bzrlib import ui
from bzrlib.trace import mutter

from bzrlib.plugins.svn import (
    changes,
    logwalker,
    mapping,
    )
from bzrlib.plugins.svn.commit import set_svn_revprops


def export_as_mapping(revmeta, graph, old_mapping, new_mapping):
    """Determine the new revision properties for an older revision.

    :param revmeta: Revision metadata object
    :param graph: Graph walker object for the repository
    :param old_mapping: Previous mapping used
    :param new_mapping: New mapping to use
    :return: Dictionary with revision properties
    """
    assert new_mapping.can_use_revprops
    new_revprops = dict(revmeta.get_revprops().iteritems())
    rev = revmeta.get_revision(old_mapping)
    revno = graph.find_distance_to_null(rev.revision_id, [])
    new_mapping.export_revision_revprops(revmeta.uuid, revmeta.branch_path, rev.timestamp, rev.timezone, rev.committer, rev.properties, rev.revision_id, revno, rev.parent_ids, new_revprops)
    new_mapping.export_fileid_map_revprops(revmeta.get_fileid_overrides(new_mapping), new_revprops)
    new_mapping.export_text_parents_revprops(revmeta.get_text_parents(new_mapping), new_revprops)
    new_mapping.export_text_revisions_revprops(revmeta.get_text_revisions(new_mapping), new_revprops)
    if rev.message != mapping.parse_svn_log(revmeta.get_revprops().get(properties.PROP_REVISION_LOG)):
        new_mapping.export_message_revprops(rev.message, new_revprops)
    return new_revprops


def upgrade_revprops(repository, new_mapping, from_revnum=0, to_revnum=None):
    # FIXME: Check that repository has log-revprops capability
    # (otherwise the new revprops might not be seen by the clients)
    raise NotImplementedError()


def set_revprops(repository, from_revnum=0, to_revnum=None):
    """Set bzr-svn revision properties for existing bzr-svn revisions.

    :param repository: Subversion Repository object.
    :param new_mapping: Mapping to upgrade to
    """
    num_changed = 0
    def set_skip_revprop(revnum, revprops):
        if not mapping.SVN_REVPROP_BZR_SKIP in revprops:
            set_svn_revprops(repository, revnum, {mapping.SVN_REVPROP_BZR_SKIP: ""})
            return 1
        return 0
    if to_revnum is None:
        to_revnum = repository.get_latest_revnum()
    graph = repository.get_graph()
    assert from_revnum <= to_revnum
    pb = ui.ui_factory.nested_progress_bar()
    try:
        for (paths, revnum, revprops) in repository._log.iter_changes(None, to_revnum, from_revnum, pb=pb):
            if revnum == 0:
                # Never a bzr-svn revision
                continue
            bp = mapping.find_roundtripped_root(revprops, paths)
            if bp is None:
                # Not a bzr-svn revision, since there is not a single root
                # (fileproperties) nor a bzr:root revision property
                num_changed += set_skip_revprop(revnum, revprops)
                continue
            revmeta = repository._revmeta_provider.get_revision(bp, revnum, paths, revprops)
            try:
                old_mapping = mapping.find_mapping_fileprops(revmeta.get_changed_fileprops())
            except SubversionException, (_, ERR_FS_NOT_DIRECTORY):
                num_changed += set_skip_revprop(revnum, revprops)
                continue
            if old_mapping is None:
                num_changed += set_skip_revprop(revnum, revprops)
                continue
            assert old_mapping.can_use_revprops or bp is not None
            assert bp is not None
            new_revprops = export_as_mapping(revmeta, graph, old_mapping, old_mapping)
            changed_revprops = dict(((k,v) for k,v in new_revprops.iteritems() if k not in revprops or revprops[k] != v))
            set_svn_revprops(repository, revnum, changed_revprops)
            if changed_revprops != {}:
                num_changed += 1
            # Might as well update the cache while we're at it
    finally:
        pb.finished()
    return num_changed
