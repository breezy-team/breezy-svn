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


"""Subversion-specific errors and conversion of Subversion-specific errors."""

from __future__ import absolute_import

import subvertpy
import urllib

from breezy import (
    trace,
    )
import breezy.errors
from breezy.errors import (
    BzrError,
    ConnectionError,
    ConnectionReset,
    DependencyNotPresent,
    DivergedBranches,
    InvalidRevisionSpec,
    LockActive,
    PermissionDenied,
    NoRepositoryPresent,
    NoSuchRevision,
    TagsNotSupported,
    TipChangeRejected,
    TransportError,
    UnexpectedEndOfContainerError,
    VersionedFileInvalidChecksum,
    )


class InvalidBzrSvnRevision(NoSuchRevision):
    _fmt = """Revision id %(revid)s was added incorrectly"""

    def __init__(self, revid):
        self.revid = revid


class NotSvnBranchPath(BzrError):
    """Error raised when a path was specified that did not exist."""
    _fmt = """%(branch_path)s is not a valid Subversion branch path.
See 'bzr help svn-layout' for details."""

    def __init__(self, branch_path, mapping=None):
        BzrError.__init__(self)
        self.branch_path = urllib.quote(branch_path)
        self.mapping = mapping


class NoSvnRepositoryPresent(NoRepositoryPresent):

    def __init__(self, url):
        BzrError.__init__(self)
        self.path = url


class ChangesRootLHSHistory(BzrError):
    _fmt = """Changing lhs branch history not possible on repository root"""


class MissingPrefix(BzrError):
    _fmt = """Prefix missing for %(path)s; please create it before pushing. """

    def __init__(self, path, existing_path):
        BzrError.__init__(self)
        self.path = path
        self.existing_path = existing_path


class RevpropChangeFailed(BzrError):
    _fmt = """Unable to set revision property %(name)s.
Does the repository pre-revprop-change hook allow property changes?"""

    def __init__(self, name):
        BzrError.__init__(self)
        self.name = name


class DavRequestFailed(BzrError):
    _fmt = """A Subversion remote access command failed: %(msg)s"""

    def __init__(self, msg):
        BzrError.__init__(self)
        self.msg = msg


def convert_error(err):
    """Convert a Subversion exception to the matching BzrError.

    :param err: SubversionException.
    :return: BzrError instance if it could be converted, err otherwise
    """
    (msg, num) = err.args

    if num == subvertpy.ERR_RA_SVN_CONNECTION_CLOSED:
        return ConnectionReset(msg=msg)
    elif num == subvertpy.ERR_WC_LOCKED:
        return LockActive(msg)
    elif num == subvertpy.ERR_RA_NOT_AUTHORIZED:
        return PermissionDenied('.', msg)
    elif num == subvertpy.ERR_INCOMPLETE_DATA:
        return UnexpectedEndOfContainerError()
    elif num == subvertpy.ERR_RA_SVN_MALFORMED_DATA:
        return TransportError("Malformed data", msg)
    elif num == subvertpy.ERR_RA_NOT_IMPLEMENTED:
        return NotImplementedError("Function not implemented in remote server")
    elif num == subvertpy.ERR_RA_DAV_REQUEST_FAILED:
        return DavRequestFailed(msg)
    elif num == subvertpy.ERR_REPOS_HOOK_FAILURE:
        return TipChangeRejected(msg)
    if num == subvertpy.ERR_RA_DAV_PROPPATCH_FAILED:
        return PropertyChangeFailed(msg)
    if (num > subvertpy.ERR_APR_OS_START_EAIERR and
        num < subvertpy.ERR_APR_OS_START_EAIERR + subvertpy.ERR_CATEGORY_SIZE):
        # Newer versions of subvertpy (>= 0.7.6) do this for us.
        return ConnectionError(msg=msg)
    else:
        return err


def convert_svn_error(unbound):
    """Decorator that catches particular Subversion exceptions and
    converts them to Bazaar exceptions.
    """
    def convert(*args, **kwargs):
        try:
            return unbound(*args, **kwargs)
        except subvertpy.SubversionException, svn_err:
            mapped_err = convert_error(svn_err)
            if svn_err is mapped_err:
                # Bare 'raise' preserves the original traceback, whereas
                # 'raise e' would not.
                raise
            else:
                raise mapped_err

    convert.__doc__ = unbound.__doc__
    convert.__name__ = unbound.__name__
    return convert


class InvalidPropertyValue(BzrError):

    _fmt = 'Invalid property value for Subversion property %(property)s: %(msg)s'

    def __init__(self, property, msg):
        BzrError.__init__(self)
        self.property = property
        self.msg = msg


class InvalidFileName(BzrError):

    _fmt = "Unable to convert Subversion path %(path)s because it contains characters invalid in Bazaar."

    def __init__(self, path):
        BzrError.__init__(self)
        self.path = path


class SymlinkTargetContainsNewline(BzrError):

    _fmt = "Unable to convert target of symlink %(path)s because it contains newlines."

    def __init__(self, path):
        BzrError.__init__(self)
        self.path = path


class CorruptMappingData(BzrError):

    _fmt = "An invalid change was made to the bzr-specific properties in %(path)s."

    def __init__(self, path):
        BzrError.__init__(self)
        self.path = path


class LayoutUnusable(BzrError):
    _fmt = "Unable to use layout %(layout)r with mapping %(mapping)r."

    def __init__(self, layout, mapping):
        BzrError.__init__(self)
        self.layout = layout
        self.mapping = mapping


class AppendRevisionsOnlyViolation(breezy.errors.AppendRevisionsOnlyViolation):

    _fmt = ('Operation denied because it would change the mainline history.'
            ' Set the append_revisions_only setting to False on'
            ' branch "%(location)s" to allow the mainline to change.')


class FileIdMapIncomplete(BzrError):

    _fmt = "Unable to find file id for child '%(child)s' in '%(parent)s' in %(revmeta)r."

    def __init__(self, child, parent, revmeta):
        BzrError.__init__(self)
        self.child = child
        self.parent = parent
        self.revmeta = revmeta


class InvalidFileId(BzrError):

    _fmt = "Unable to parse file id %(fileid)s."

    def __init__(self, fileid):
        BzrError.__init__(self)
        self.fileid = fileid


class DifferentSubversionRepository(BzrError):

    _fmt = "UUID %(got)s does not match expected UUID %(expected)s."

    def __init__(self, got, expected):
        BzrError.__init__(self)
        self.got = got
        self.expected = expected


class UnknownMapping(BzrError):

    _fmt = """Attempt to use unknown mapping. %(extra)s """

    def __init__(self, mapping, extra=None):
        BzrError.__init__(self, extra=(extra or ""))
        self.mapping = mapping


class AbsentPath(BzrError):

    _fmt = """Unable to access %(path)s: no permission?. """

    def __init__(self, path):
        BzrError.__init__(self, path=path)


class NoCustomBranchPaths(BzrError):

    _fmt = """Layout %(layout)r does not support custom branch paths."""

    def __init__(self, layout=None):
        BzrError.__init__(self, layout=layout)


class PushToEmptyBranch(BzrError):

    _fmt = ("Empty branch already exists at %(target)s. "
            "Specify --overwrite or remove it before pushing.")

    def __init__(self, target, source):
        BzrError.__init__(self, source=source.user_url, target=target.user_url)


class PropertyChangeFailed(BzrError):

    _fmt = """Unable to set DAV properties: %(msg)s. Perhaps LimitXMLRequestBody is set too low in the server."""

    def __init__(self, msg):
        BzrError.__init__(self, msg=msg)


class RequiresMetadataInFileProps(BzrError):

    _fmt = """This operation requires storing bzr-svn metadata in Subversion file properties. These file properties may cause spurious conflicts for other Subversion users during merges. To allow this, set `allow_metadata_in_file_properties = True` in your configuration and try again."""


class TextChecksumMismatch(VersionedFileInvalidChecksum):

    _fmt = """checksum mismatch: %(expected_checksum)r != %(actual_checksum)r in %(path)s:%(revnum)d"""

    def __init__(self, expected_checksum, actual_checksum, path, revnum):
        self.expected_checksum = expected_checksum
        self.actual_checksum = actual_checksum
        self.path = path
        self.revnum = revnum


class SubversionBranchDiverged(DivergedBranches):

    _fmt = "Subversion branch at %(branch_path)s has diverged from %(source_repo)r."

    def __init__(self, source_repo, source_revid, target_repo, branch_path, target_revid):
        self.branch_path = branch_path
        self.target_repo = target_repo
        self.source_repo = source_repo
        self.source_revid = source_revid
        self.target_revid = target_revid


class NoLayoutTagSetSupport(TagsNotSupported):

    _fmt = "Creating tags is not possible with the current layout %(layout)r%(extra)s"

    def __init__(self, layout, extra=None):
        self.layout = layout
        if extra is None:
            self.extra = ""
        else:
            self.extra = ": %s" % extra


class IncompleteRepositoryHistory(BzrError):

    _fmt = "Unable to fetch revision info; %(msg)s"

    def __init__(self, msg):
        self.msg = msg


_reuse_uuids_warned = set()
def warn_uuid_reuse(uuid, location):
    """Warn that a UUID is being reused for different repositories."""
    global _reuse_uuids_warned
    if uuid in _reuse_uuids_warned:
        return
    trace.warning("Repository with UUID %s at %s contains fewer revisions "
         "than cache. This either means that this repository contains an out "
         "of date mirror of another repository (harmless), or that the UUID "
         "is being used for two different Subversion repositories ("
         "potential repository corruption).",
         uuid, location)
    _reuse_uuids_warned.add(uuid)
