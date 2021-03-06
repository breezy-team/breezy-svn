Bazaar Subversion Mapping Specification
=======================================

This document specifies mapping between Subversion and Bazaar semantics.

Revision: 4

Written by Jelmer Vernooij <jelmer@samba.org>

.. contents::

Branch paths
------------

It is hard to know, given a SVN URL, to figure out what branch a particular
file is in. Other then the convention that branches are named 'trunk' and
'branches/\*', there is no way to automatically find out what a branch is.
Finding out what a branch is and what is not is done by a BranchingScheme
implementation. At the moment, the following branching schemes are available:

- NoBranchingScheme: The top-level directory in the repository is a branch.
  (consequence of this is that there is only one branch total)

- TrunkBranchingScheme: There is a directory structure with 'trunk',
  'branches', and 'tags' directories as common in Subversion-based projects.
  These directories can be nested (e.g. subproject1/trunk) inside the
  repository. The level of nesting is stored.

- ListBranchingScheme: There is a list of branches. This branching
  scheme is present in the code, but is never used automatically (yet).

- SingleBranchingScheme: There is a single branch (path of that branch is also part of the scheme name).

The branching scheme can be explicitly specified or automatically guessed.

Revision ids
------------

An easy way to generate globally unique and reproducible revision ids is to
simply combine the repositories UUID and commit revision number.

However, this can lead to overlap in revision ids when a commit touches more
then one branch (something that is possible in SVN). This can be fixed by
including the branch path (``trunk``, ``branches/SAMBA_4_0``, etc) in the
revision-id. Example revision id:

``svn-v3-trunk0:0c0555d6-39d7-0310-84fc-f1cc0bd64818:trunk:14323``

The version number is used to distinguish between versions of the mapping
between Bazaar and Subversion. The mapping will change when previously
unsupported features are added to Bazaar (see below), or when a bug in bzr-svn
is fixed that may affect the mappings.

Once branching schemes can be manually specified, also needs to contain
branching scheme as it might influence the parents of the current revision (if
a parent path is a branch according but not according to another).

Since '/' and whitespace are forbidden in revision ids, the branch paths
are all urlencoded. Example revision id for branches/foobranch:

``svn-v3-trunk-1:0c0555d6-39d7-0310-84fc-f1cc0bd64818:branches%2Ffoobranch:14323``

It is also possible that the revision id for a particular revision is
stored in a revision property. To guarantee that the meaning of a revision id
does not change, this revision id is only valid within a specific version
of the mappings.

To override the revision id this way, set the branch path file property:

``bzr:revision-id-v%d:%s`` (where %d is the current mapping version and %s is
                            the name of the branching scheme in use)

to the bzr revision number following by a space and the revision id. This
property should only be honored for the revision in which it was set, as
subversion will not erase the property for subsequent commits.

A (path,revnum) tuple is valid if:

- path is valid according to the branching scheme
- either path, revnum or one of its children was touched in the particular
  revision

If possible, the Subversion revision property ``bzr:revision-id`` should be
set to the revision id. The revision property ``bzr:root`` should be set to
the root path of the revision and revision property ``bzr:scheme`` should be
set to the name of the branching scheme.

File ids
--------

Subversion does not use file ids. It is not possible to know whether a file in
revision X and a file in revision Y are the same without traversing over all
the revisions between X and Y.

File ids use the following syntax:

``<REVNO>@<UUID>:<BRANCHPATH>:<PATH>``

Since / is forbidden in file ids, all characters are urlencoded.

The same rules apply to the roots of branches. This means there is no
predefined file id for tree roots.

Alternatively, these file ids can be mapped to more specific file ids. Such
a map should be stored in the 'bzr:file-ids' file property that is set on the
branch path and, if possible, as revision property ``bzr:file-ids``.

The bzr:file-ids property should contain a list of mappings. Entries are
separated by newlines. The path in the branch and new file-id are separated
by a tab.

Given, the path, the revision the mapping was added, the repository uuid
and the path the property is set on the (the branch path), the original
file id can be determined.

Tabs, newlines and percent signs in path will be urlencoded.

Neither the original nor the target file id may occur more than once.

The entries are sorted by revnum (highest revnum last). Within a specific
revnum, the order is not specified.

File id mappings can only change if something about the metadata of a file
changed: it is in no way related to the contents of that file.

If a file is being replaced by a copy of itself in an older revision it will
receive a new file id.

If the file id generated is longer than 150 bytes, the following format will
be used:

<REVNO>@<UUID>:<BRANCH>;<SHA1>

where <SHA1> is the sha1 of the file's path.

NEXT VERSION: Special rules are applied to make sure that renames are tracked.

Properties
----------

SVN allows setting properties on versioned files and also interprets several
of these properties.

"svn:executable" is mapped to bzr's executable bit.

"svn:ignore" is currently ignored.

"svn:mime-type" is currently ignored.

"svn:special" for symlinks is interpreted and mapped to symlinks in bzr.

"svk:merge" is understood and set where possible.

Ancestry Information
--------------------

Ancestry in Subversion is linear. Most revisions have just one parent. Files
can be copied, moved or merged from other branches, which can result in partial
merges that bzr doesn't support at the moment.

Whenever a Bazaar commit to Subversion has more than one parent (merges two
revisions), it will add a line to the 'bzr:ancestry:vX-SCHEME' property set on
the branch path. The format of these lines is:

([\tPARENT-REV-ID]+)\n

If possible, the list of merges will also be stored in the Subversion revision
property ``bzr:merges``.

This property should be considered add-only. This way, it is
possible to know the parents of a revision when running checkout or
diff, because the Subversion API will mark the property as modified. The
parents can be obtained by simply looking at the last line.

Other operations (outside of checkouts) can obtain the revision
parents by simply running diff on the property between the current and the
previous revision of the branch path.

Bazaar will also set 'svk:merge' if one of the merges is originally from a
Subversion branch and not on the mainline. If 'svk:merge' is changed and
'bzr:ancestry:...' didn't, the diff in 'svk:merge' is also used to obtain the
parents of a commit.

This means svk and bzr *should be* interoperable. However, there are no tests
for this yet.

Revision properties
-------------------

If possible, the Bazaar revision metadata should be stored in Subversion
revision properties. The names of the revision properties are:

- committer: ``bzr:committer``
- timestamp: ``svn:original-date``

All custom revision properties are prefixed by ``bzr:revprop:``

Bazaar revision metadata is also stored in a Subversion revision property
``bzr:revision-info``. The format of this property is the same as used
by version 0.9 of the bundle format.


Signatures
----------

NEXT VERSION: GPG Signatures for commits will be stored in the SVN revision
property 'bzr:gpg-signature'.

Revisions
---------

Revision 1 was the original version of this document.

Revision 2 enforces UTF-8-valid characters for everything except file
contents.

Revision 3 changed the file id format, changed the urlencoding to be
uppercased, changed the separator between uuids and branch paths in the
revision id from "-" to ":", added the branching scheme to the revision id
and added the bzr:revision-id-vX:YY property.

Revision 3 uses real file ids for the tree root rather than the hardcoded
"TREE_ROOT" and adds the file id map.

Revision 4 adds revision properties in addition to file properties.

..
	vim: ft=rest
