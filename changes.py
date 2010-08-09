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


"""Utility functions for dealing with changes dictionaries as return by Subversions' log functions."""

from subvertpy import NODE_DIR

try:
    any
except NameError:
    from bzrlib.plugins.svn.pycompat import any

REV0_CHANGES = {"": ('A', None, -1, NODE_DIR)}


def path_is_child(branch_path, path):
    """Check whether path is or is under branch_path."""
    return (branch_path == "" or
            branch_path == path or
            path.startswith(branch_path+"/"))


def find_prev_location(paths, branch_path, revnum):
    """Find the previous location at which branch_path can be found.

    :note: If branch_path wasn't copied, this will return revnum-1 as the
        previous revision.
    """
    assert type(branch_path) is str
    assert type(revnum) is int
    if revnum == 0:
        assert branch_path == ""
        return None
    # If there are no special cases, just go try the
    # next revnum in history
    revnum -= 1

    if branch_path == "":
        return (branch_path, revnum)

    # Make sure we get the right location for next time, if
    # the branch itself was copied
    if (paths.has_key(branch_path) and
        paths[branch_path][0] in ('R', 'A')):
        if paths[branch_path][1] is None:
            return None # Was added here
        revnum = paths[branch_path][2]
        assert type(paths[branch_path][1]) is str
        branch_path = paths[branch_path][1]
        return (branch_path, revnum)

    # Make sure we get the right location for the next time if
    # one of the parents changed

    # Path names need to be sorted so the longer paths
    # override the shorter ones
    for p in sorted(paths.keys(), reverse=True):
        if paths[p][0] == 'M':
            continue
        if branch_path.startswith(p+"/"):
            assert paths[p][0] in ('A', 'R'), "Parent %r wasn't added" % p
            assert paths[p][1] is not None, \
                "Empty parent %r added, but child %r wasn't added !?" % (p, branch_path)

            revnum = paths[p][2]
            branch_path = paths[p][1].encode("utf-8") + branch_path[len(p):]
            return (branch_path, revnum)

    return (branch_path, revnum)


def changes_path(changes, path, parents=False):
    """Check if one of the specified changes applies
    to path or one of its children.

    :param parents: Whether to consider a parent moving a change.
    """
    for p in changes:
        assert type(p) is str
        if path_is_child(path, p):
            return True
        if parents and path.startswith(p+"/") and changes[p][0] in ('R', 'A'):
            return True
    return False


def changes_children(changes, path):
    """Check if one of the specified changes applies to
    one of paths children.

    :note: Does not consider changes to path itself.
    """
    for p in changes:
        assert type(p) is str
        if path_is_child(path, p) and path != p:
            return True
    return False


def changes_root(paths):
    """Find the root path that was changed.

    If there is more than one root, returns None
    """
    if paths == []:
        return None
    paths = sorted(paths)
    root = paths[0]
    for p in paths[1:]:
        if p.startswith("%s/" % root): # new path is child of root
            continue
        elif root.startswith("%s/" % p): # new path is parent of root
            root = p
        else:
            if "" in paths:
                return ""
            return None # Mismatch
    return root

def apply_reverse_changes(branches, changes):
    """Apply the specified changes on a set of branch names in reverse.
    (E.g. as if we were applying the reverse of a delta)

    :return: [(new_name, old_name, new_rev, kind)]
    :note: new_name is the name before these changes,
           old_name is the name after the changes.
           new_rev is the revision that the changes were copied from
           (new_name), or -1 if the previous revnum
    """
    branches = set(branches)
    for p in sorted(changes):
        (action, cf, cr, kind) = changes[p]
        if action == 'D':
            for b in list(branches):
                if path_is_child(p, b):
                    branches.remove(b)
                    yield b, None, -1, kind
        elif cf is not None:
            for b in list(branches):
                if path_is_child(p, b):
                    old_b = rebase_path(b, p, cf)
                    yield b, old_b, cr, kind
                    branches.remove(b)
                    branches.add(old_b)


def rebase_path(path, orig_parent, new_parent):
    """Rebase a path on a different parent."""
    return (new_parent+"/"+path[len(orig_parent):].strip("/")).strip("/")


def changes_outside_branch_path(branch_path, paths):
    """Check whether there are any paths that are not under branch_path."""
    for p in paths:
        if not path_is_child(branch_path, p):
            return True
    return False


def under_prefixes(path, prefixes):
    """Check if path is under one of prefixes."""
    if prefixes is None or "" in prefixes:
        return True
    return any([x for x in prefixes if path.startswith(x+"/") or x == path or x == ""])
