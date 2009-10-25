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


"""Maps between Subversion and Bazaar semantics."""


import calendar
from subvertpy import properties
import time
import urllib

from bzrlib import (
    foreign,
    osutils,
    )
from bzrlib.errors import (
    InvalidRevisionId,
    )
from bzrlib.revision import (
    NULL_REVISION,
    Revision,
    )
from bzrlib.trace import mutter

from bzrlib.plugins.svn import (
    changes,
    errors,
    get_client_string,
    version_info,
    )


SVN_PROP_BZR_PREFIX = 'bzr:'
SVN_PROP_BZR_ANCESTRY = 'bzr:ancestry:'
SVN_PROP_BZR_FILEIDS = 'bzr:file-ids'
SVN_PROP_BZR_REVISION_INFO = 'bzr:revision-info'
SVN_PROP_BZR_REVISION_ID = 'bzr:revision-id:'
SVN_PROP_BZR_TEXT_REVISIONS = 'bzr:text-revisions'
SVN_PROP_BZR_TEXT_PARENTS = 'bzr:text-parents'
SVN_PROP_BZR_LOG = 'bzr:log'
SVN_PROP_BZR_REQUIRED_FEATURES = 'bzr:required-features'
SVN_PROP_BZR_HIDDEN = 'bzr:hidden'
SVN_PROP_BZR_REVPROP_REDIRECT = 'bzr:see-revprops'

SVN_REVPROP_BZR_COMMITTER = 'bzr:committer'
SVN_REVPROP_BZR_FILEIDS = 'bzr:file-ids'
SVN_REVPROP_BZR_MAPPING_VERSION = 'bzr:mapping-version'
SVN_REVPROP_BZR_USER_AGENT = 'bzr:user-agent'
SVN_REVPROP_BZR_MERGE = 'bzr:merge'
SVN_REVPROP_BZR_REVISION_ID = 'bzr:revision-id'
SVN_REVPROP_BZR_REVNO = 'bzr:revno'
SVN_REVPROP_BZR_REVPROP_PREFIX = 'bzr:revprop:'
SVN_REVPROP_BZR_ROOT = 'bzr:root'
SVN_REVPROP_BZR_SIGNATURE = 'bzr:gpg-signature'
SVN_REVPROP_BZR_TIMESTAMP = 'bzr:timestamp'
SVN_REVPROP_BZR_LOG = 'bzr:log'
SVN_REVPROP_BZR_TEXT_PARENTS = 'bzr:text-parents'
SVN_REVPROP_BZR_TEXT_REVISIONS = 'bzr:text-revisions'
SVN_REVPROP_BZR_REQUIRED_FEATURES = 'bzr:required-features'
SVN_REVPROP_BZR_BASE_REVISION = 'bzr:base-revision'
SVN_REVPROP_BZR_SKIP = 'bzr:skip'
SVN_REVPROP_BZR_HIDDEN = 'bzr:hidden'
SVN_REVPROP_BZR_REPOS_UUID = 'bzr:repository-uuid'
SVN_REVPROP_BZR_POINTLESS = 'bzr:pointless'


def find_new_lines((oldvalue, newvalue)):
    """Find any new lines that have been added to a string.

    :param oldvalue: Previous contents
    :param newvalue: Newcontents
    :raises ValueError: If the existing contents were changed
    """
    if oldvalue is None:
        oldvalue = ""
    if not newvalue.startswith(oldvalue):
        raise ValueError("Existing contents were changed")
    appended = newvalue[len(oldvalue):]
    return appended.splitlines()


def escape_svn_path(x):
    """Escape a Subversion path for use in a revision identifier.

    :param x: Path
    :return: Escaped path
    """
    assert isinstance(x, str)
    return urllib.quote(x, "")
unescape_svn_path = urllib.unquote


# The following two functions don't use day names (which can vary by 
# locale) unlike the alternatives in bzrlib.timestamp

def format_highres_date(t, offset=0):
    """Format a date, such that it includes higher precision in the
    seconds field.

    :param t:   The local time in fractional seconds since the epoch
    :type t: float
    :param offset:  The timezone offset in integer seconds
    :type offset: int
    """
    assert isinstance(t, float)

    # This has to be formatted for "original" date, so that the
    # revision XML entry will be reproduced faithfully.
    if offset is None:
        offset = 0
    tt = time.gmtime(t + offset)

    return (time.strftime("%Y-%m-%d %H:%M:%S", tt)
            # Get the high-res seconds, but ignore the 0
            + ('%.9f' % (t - int(t)))[1:]
            + ' %+03d%02d' % (offset / 3600, (offset / 60) % 60))


def unpack_highres_date(date):
    """This takes the high-resolution date stamp, and
    converts it back into the tuple (timestamp, timezone)
    Where timestamp is in real UTC since epoch seconds, and timezone is an
    integer number of seconds offset.

    :param date: A date formated by format_highres_date
    :type date: string
    """
    # skip day if applicable
    if not date[0].isdigit():
        space_loc = date.find(' ')
        if space_loc == -1:
            raise ValueError("No valid date: %r" % date)
        date = date[space_loc+1:]
    # Up until the first period is a datestamp that is generated
    # as normal from time.strftime, so use time.strptime to
    # parse it
    dot_loc = date.find('.')
    if dot_loc == -1:
        raise ValueError(
            'Date string does not contain high-precision seconds: %r' % date)
    base_time = time.strptime(date[:dot_loc], "%Y-%m-%d %H:%M:%S")
    fract_seconds, offset = date[dot_loc:].split()
    fract_seconds = float(fract_seconds)

    offset = int(offset)

    hours = int(offset / 100)
    minutes = (offset % 100)
    seconds_offset = (hours * 3600) + (minutes * 60)

    # time.mktime returns localtime, but calendar.timegm returns UTC time
    timestamp = calendar.timegm(base_time)
    timestamp -= seconds_offset
    # Add back in the fractional seconds
    timestamp += fract_seconds
    return (timestamp, seconds_offset)


def parse_merge_property(line):
    """Parse a bzr:merge property value.

    :param line: Line to parse
    :return: List of revisions merged
    """
    if ' ' in line:
        mutter('invalid revision id %r in merged property, skipping', line)
        return ()

    return tuple(filter(lambda x: x != "", line.split("\t")))


def parse_svn_dateprop(date):
    """Parse a Subversion date property and return a unix timestamp.
    
    :param date: A string containing a Subversion date string
    :return: Unix timestamp
    """
    assert isinstance(date, str)
    return (properties.time_from_cstring(date) / 1000000.0, 0)


def parse_svn_log(log):
    if log is None:
        return ""
    try:
        return log.decode("utf-8")
    except UnicodeDecodeError:
        return log


def parse_svn_revprops(svn_revprops, rev):
    rev.committer = svn_revprops.get(properties.PROP_REVISION_AUTHOR, "").decode("utf-8")
    if rev.committer is None:
        rev.committer = u""
    rev.message = parse_svn_log(svn_revprops.get(properties.PROP_REVISION_LOG))

    assert svn_revprops.has_key(properties.PROP_REVISION_DATE)
    (rev.timestamp, rev.timezone) = parse_svn_dateprop(svn_revprops[properties.PROP_REVISION_DATE])
    rev.properties = {}


def parse_revision_metadata(text, rev):
    """Parse a revision info text (as set in bzr:revision-info).

    :param text: text to parse
    :param rev: Revision object to apply read parameters to
    """
    in_properties = False
    for l in text.splitlines():
        if not ": " in l:
            raise errors.InvalidPropertyValue(SVN_PROP_BZR_REVISION_INFO, 
                    "Missing : in revision metadata in %r" % l)
        key, value = l.split(": ", 1)
        if key == "committer":
            rev.committer = value.decode("utf-8")
        elif key == "timestamp":
            (rev.timestamp, rev.timezone) = unpack_highres_date(value)
        elif key == "properties":
            in_properties = True
        elif key[0] == "\t" and in_properties:
            name = str(key[1:])
            if not name in rev.properties:
                rev.properties[name] = value.decode("utf-8")
            else:
                rev.properties[name] += "\n" + value.decode("utf-8")
        else:
            raise errors.InvalidPropertyValue(SVN_PROP_BZR_REVISION_INFO, 
                    "Invalid key %r" % key)


def parse_revid_property(line):
    """Parse a (revnum, revid) tuple as set in revision id properties.
    :param line: line to parse
    :return: tuple with (bzr_revno, revid)
    """
    if '\n' in line:
        raise errors.InvalidPropertyValue(SVN_PROP_BZR_REVISION_ID, 
                "newline in revision id property line")
    try:
        (revno, revid) = line.split(' ', 1)
    except ValueError:
        raise errors.InvalidPropertyValue(SVN_PROP_BZR_REVISION_ID, 
                "missing space")
    if revid == "":
        raise errors.InvalidPropertyValue(SVN_PROP_BZR_REVISION_ID,
                "empty revision id")
    return (int(revno), revid)


def generate_revision_metadata(timestamp, timezone, committer, revprops):
    """Generate revision metadata text for the specified revision 
    properties.

    :param timestamp: timestamp of the revision, in seconds since epoch
    :param timezone: timezone, specified by offset from GMT in seconds
    :param committer: name/email of the committer
    :param revprops: dictionary with custom revision properties
    :return: text with data to set bzr:revision-info to.
    """
    assert timestamp is None or isinstance(timestamp, float)
    text = ""
    if timestamp is not None:
        text += "timestamp: %s\n" % format_highres_date(timestamp, timezone) 
    if committer is not None:
        if "\n" in committer:
            raise AssertionError("Invalid committer: contains newline")
        text += "committer: %s\n" % committer.encode("utf-8")
    if revprops is not None and revprops != {}:
        text += "properties: \n"
        for k, v in sorted(revprops.iteritems()):
            if "\n" in k:
                raise AssertionError("Invalid property name: %s" % k)
            for vline in v.encode("utf-8").split("\n"):
                text += "\t%s: %s\n" % (k.encode("utf-8"), vline)
    assert isinstance(text, str)
    return text


def parse_bzr_svn_revprops(props, rev):
    """Update a Revision object from a set of Subversion revision properties.
    
    :param props: Dictionary with Subversion revision properties.
    :param rev: Revision object
    """
    changed = False
    if props.has_key(SVN_REVPROP_BZR_TIMESTAMP):
        (rev.timestamp, rev.timezone) = unpack_highres_date(props[SVN_REVPROP_BZR_TIMESTAMP])
        changed = True

    if props.has_key(SVN_REVPROP_BZR_COMMITTER):
        rev.committer = props[SVN_REVPROP_BZR_COMMITTER].decode("utf-8")
        changed = True

    if props.has_key(SVN_REVPROP_BZR_LOG):
        rev.message = props[SVN_REVPROP_BZR_LOG]
        changed = True

    for name, value in props.iteritems():
        if name.startswith(SVN_REVPROP_BZR_REVPROP_PREFIX):
            rev.properties[name[len(SVN_REVPROP_BZR_REVPROP_PREFIX):]] = value
        changed = True

    return changed


def parse_required_features_property(text):
    return set(text.split(","))


class BzrSvnMapping(foreign.VcsMapping):
    """Class that maps between Subversion and Bazaar semantics."""

    experimental = False
    _warned_experimental = False
    roundtripping = False
    can_use_revprops = False
    can_use_fileprops = False
    must_use_fileprops = False
    supports_hidden = False
    restricts_branch_paths = False
    parseable_file_ids = False

    def __init__(self):
        super(BzrSvnMapping, self).__init__(foreign_vcs_svn)
        if (version_info[3] == 'exp' or self.experimental) and not BzrSvnMapping._warned_experimental:
            from bzrlib.trace import warning
            warning("using experimental bzr-svn mappings; may break existing branches in the most horrible ways")
            BzrSvnMapping._warned_experimental = True

    @classmethod
    def from_repository(cls, repository, _hinted_branch_path=None):
        return cls()

    @classmethod
    def from_revprops(cls, revprops):
        raise NotImplementedError

    def check_layout(self, repository, layout):
        """Check whether a layout can be used with this mapping."""
        pass

    def get_mandated_layout(self, repository):
        """Return the repository layout if any is mandated by this mapping, 
        None otherwise."""
        return None

    def get_guessed_layout(self, repository):
        """Return the repository layout guessed by this mapping or None.
        """
        return None

    def revision_id_bzr_to_foreign(self, revid):
        """Parse an existing Subversion-based revision id.

        :param revid: The revision id.
        :raises: InvalidRevisionId
        :return: Tuple with (uuid, branch path, revision number) and mapping.
        """
        raise NotImplementedError(self.revision_id_bzr_to_foreign)

    def revision_id_foreign_to_bzr(self, (uuid, path, revnum)):
        """Generate a unambiguous revision id. 
        
        :param uuid: UUID of the repository.
        :param path: Branch path.
        :param revnum: Subversion revision number.

        :return: New revision id.
        """
        raise NotImplementedError(self.revision_id_foreign_to_bzr)

    def is_branch(self, branch_path):
        raise NotImplementedError(self.is_branch)

    def get_branch_root(self, revprops):
        """Determine the branch root set in the revision properties.
        
        :note: Return None if no branch root is set.
        """
        return None

    def get_repository_uuid(self, revprops):
        """Determine the uuid of the repository in which this 
        round-tripped revision was originally committed.

        :note: Return None if no uuid is set.
        """
        return None

    def is_tag(self, tag_path):
        raise NotImplementedError(self.is_tag)

    def is_branch_or_tag(self, path):
        return self.is_branch(path) or self.is_tag(path)

    @staticmethod
    def generate_file_id((uuid, branch, revnum), inv_path):
        """Create a file id identifying a Subversion file.

        :param uuid: UUID of the repository
        :param revnum: Revision number at which the file was introduced.
        :param branch: Branch path of the branch in which the file was introduced.
        :param inv_path: Original path of the file within the inventory
        """
        raise NotImplementedError

    @staticmethod
    def parse_file_id(fileid):
        """Parse a file id created by bzr-svn.

        :param fileid: The file id to parse
        :return: Tuple with (uuid, revnum, path)
        """
        raise NotImplementedError

    def import_revision_revprops(self, revprops, rev):
        """Update a Revision object from Subversion revision and branch 
        properties.

        :param revprops: Dictionary with Subversion revision properties.
        :param rev: Revision object to import data into.
        :return: Whether or not any metadata was found
        """
        raise NotImplementedError(self.import_revision_revprops)

    def import_revision_fileprops(self, fileprops, rev):
        """Update a Revision object from Subversion revision and branch 
        properties.

        :param fileprops: Dictionary with Subversion file properties on the 
                          branch root.
        :param rev: Revision object to import data into.
        :return: Whether or not any metadata was found
        """
        raise NotImplementedError(self.import_revision_fileprops)

    def get_lhs_parent_revprops(self, revprops):
        """Determine the left hand side parent, if it was explicitly recorded.

        If not explicitly recorded, returns None. Returns NULL_REVISION if 
        there is no lhs parent.

        """
        return None

    def get_lhs_parent_fileprops(self, fileprops):
        """Determine the left hand side parent, if it was explicitly recorded.

        If not explicitly recorded, returns None. Returns NULL_REVISION if 
        there is no lhs parent.

        """
        return None

    def get_rhs_parents_fileprops(self, fileprops):
        """Obtain the right-hand side parents for a revision.

        """
        raise NotImplementedError(self.get_rhs_parents_fileprops)

    def get_rhs_parents_revprops(self, revprops):
        """Obtain the right-hand side parents for a revision.

        """
        raise NotImplementedError(self.get_rhs_parents_revprops)

    def get_rhs_ancestors(self, fileprops):
        """Obtain the right-hand side ancestors for a revision.

        """
        raise NotImplementedError(self.get_rhs_ancestors)

    def import_fileid_map_fileprops(self, fileprops):
        """Obtain the file id map for a revision from the properties.

        """
        raise NotImplementedError(self.import_fileid_map_fileprops)

    def import_fileid_map_revprops(self, revprops):
        """Obtain the file id map for a revision from the properties.

        """
        raise NotImplementedError(self.import_fileid_map_revprops)

    def export_fileid_map_revprops(self, fileids, revprops):
        """Adjust the properties for a file id map.

        :param fileids: Dictionary
        :param revprops: Subversion revision properties
        """
        raise NotImplementedError(self.export_fileid_map_revprops)

    def export_fileid_map_fileprops(self, fileids, fileprops):
        """Adjust the properties for a file id map.

        :param fileids: Dictionary
        :param fileprops: File properties
        """
        raise NotImplementedError(self.export_fileid_map_fileprops)

    def import_text_parents_revprops(self, revprops):
        """Obtain a text parent map from properties.

        :param revprops: Subversion revision properties.
        """
        raise NotImplementedError(self.import_text_parents_revprops)

    def import_text_parents_fileprops(self, fileprops):
        """Obtain a text parent map from properties.

        :param fileprops: File properties.
        """
        raise NotImplementedError(self.import_text_parents_fileprops)

    def export_text_parents_revprops(self, text_parents, revprops):
        """Store a text parent map.

        :param text_parents: Text parent map
        :param revprops: Revision properties
        """
        raise NotImplementedError(self.export_text_parents_revprops)

    def export_text_parents_fileprops(self, text_parents, fileprops):
        """Store a text parent map.

        :param text_parents: Text parent map
        :param fileprops: File properties
        """
        raise NotImplementedError(self.export_text_parents_fileprops)

    def export_text_revisions_revprops(self, text_revisions, revprops):
        """Store a text revisions map.

        :param text_parents: Text revision map
        :param revprops: Revision properties
        """
        raise NotImplementedError(self.export_text_revisions_revprops)

    def export_text_revisions_fileprops(self, text_revisions, fileprops):
        """Store a text revisions map.

        :param text_parents: Text revision map
        :param fileprops: File properties
        """
        raise NotImplementedError(self.export_text_revisions_fileprops)

    def import_text_revisions_revprops(self, revprops):
        raise NotImplementedError(self.import_text_revisions_revprops)

    def import_text_revisions_fileprops(self, fileprops):
        raise NotImplementedError(self.import_text_revisions_fileprops)

    def export_revision_revprops(self, uuid, branch_root, timestamp, timezone, committer, revprops, revision_id, revno, parent_ids, svn_revprops):
        """Determines the revision properties.
        """
        raise NotImplementedError(self.export_revision_revprops)

    def export_revision_fileprops(self, timestamp, timezone, committer, revprops, revision_id, revno, parent_ids, svn_fileprops):
        """Determines the branch root file properties.
        """
        raise NotImplementedError(self.export_revision_fileprops)

    def export_message_revprops(self, log, revprops):
        raise NotImplementedError(self.export_message_revprops)

    def export_message_fileprops(self, log, fileprops):
        raise NotImplementedError(self.export_message_fileprops)

    def get_revision_id_revprops(self, revprops):
        raise NotImplementedError(self.get_revision_id_revprops)

    def get_revision_id_fileprops(self, fileprops):
        raise NotImplementedError(self.get_revision_id_fileprops)

    def supports_tags(self):
        raise NotImplementedError(self.supports_tags)

    @classmethod
    def get_test_instance(cls):
        return cls()

    def is_bzr_revision_hidden_revprops(self, revprops):
        return False
    
    def is_bzr_revision_hidden_fileprops(self, changed_fileprops):
        return False

    def export_hidden_revprops(self, branch_root, revprops):
        raise NotImplementedError(self.export_hidden_revprops)

    def export_hidden_fileprops(self, fileprops):
        raise NotImplementedError(self.export_hidden_fileprops)

    def export_revprop_redirect(self, revnum, fileprops):
        raise NotImplementedError(self.export_revprop_redirect)

    def check_revprops(self, revprops, checkresult):
        pass

    def check_fileprops(self, changed_fileprops, checkresult):
        pass

    def newer_than(self, other_mapping):
        """Check whether other_mapping precedes this mapping, and that 
        revisions mapped with other_mapping can exist in the ancestry of revisions
        mapped with this mapping.
        """
        return False


def parse_fileid_property(text):
    """Pares a fileid file or revision property.

    :param text: Property value
    :return: Map of path -> fileid
    """
    ret = {}
    for line in text.splitlines():
        (path, key) = line.split("\t", 1)
        ret[urllib.unquote(path).decode("utf-8")] = osutils.safe_file_id(key)
    return ret


def generate_fileid_property(fileids):
    """Marshall a dictionary with file ids.
    
    :param fileids: Map of path -> fileid
    :return: Property value
    """
    return "".join(["%s\t%s\n" % (urllib.quote(path.encode("utf-8")), fileids[path]) for path in sorted(fileids.keys())])


def parse_text_parents_property(text):
    ret = {}
    for line in text.splitlines():
        parts = line.split("\t")
        entry = parts[0]
        ret[urllib.unquote(entry).decode("utf-8")] = filter(lambda x: x != "", [osutils.safe_revision_id(parent_revid) for parent_revid in parts[1:]])
    return ret


def parse_text_revisions_property(text):
    ret = {}
    for line in text.splitlines():
        (entry, revid) = line.split("\t", 1)
        ret[urllib.unquote(entry).decode("utf-8")] = osutils.safe_revision_id(revid)
    return ret


def generate_text_parents_property(text_parents):
    return "".join(["%s\t%s\n" % (urllib.quote(path.encode("utf-8")), "\t".join(text_parents[path])) for path in sorted(text_parents.keys())])


def generate_text_revisions_property(text_revisions):
    return "".join(["%s\t%s\n" % (urllib.quote(path.encode("utf-8")), text_revisions[path]) for path in sorted(text_revisions.keys())])


class BzrSvnMappingFileProps(object):
    def __init__(self, name):
        self.name = name

    def import_revision_fileprops(self, fileprops, rev):
        if SVN_PROP_BZR_LOG in fileprops:
            rev.message = fileprops[SVN_PROP_BZR_LOG][1]
        metadata = fileprops.get(SVN_PROP_BZR_REVISION_INFO)
        if metadata is not None:
            assert isinstance(metadata, tuple)
            parse_revision_metadata(metadata[1], rev)
            return True
        return False

    def import_text_parents_fileprops(self, fileprops):
        metadata = fileprops.get(SVN_PROP_BZR_TEXT_PARENTS)
        if metadata is None:
            return {}
        assert isinstance(metadata, tuple)
        return parse_text_parents_property(metadata[1])

    def import_text_revisions_fileprops(self, fileprops):
        metadata = fileprops.get(SVN_PROP_BZR_TEXT_REVISIONS)
        if metadata is None:
            return {}
        assert isinstance(metadata, tuple)
        return parse_text_revisions_property(metadata[1])

    def export_text_parents_fileprops(self, text_parents, fileprops):
        if text_parents != {}:
            fileprops[SVN_PROP_BZR_TEXT_PARENTS] = generate_text_parents_property(text_parents)
        elif SVN_PROP_BZR_TEXT_PARENTS in fileprops:
            fileprops[SVN_PROP_BZR_TEXT_PARENTS] = ""

    def export_text_revisions_fileprops(self, text_revisions, fileprops):
        if text_revisions != {}:
            fileprops[SVN_PROP_BZR_TEXT_REVISIONS] = generate_text_revisions_property(text_revisions)
        elif SVN_PROP_BZR_TEXT_REVISIONS in fileprops:
            fileprops[SVN_PROP_BZR_TEXT_REVISIONS] = ""

    def get_rhs_parents_fileprops(self, fileprops):
        bzr_merges = fileprops.get(SVN_PROP_BZR_ANCESTRY+self.name, None)
        if bzr_merges is not None:
            try:
                new_lines = find_new_lines(bzr_merges)
            except ValueError, e:
                mutter(str(e))
                return ()
            if len(new_lines) != 1:
                mutter("unexpected number of lines in bzr merge property: %r" % new_lines)
                return ()
            return parse_merge_property(new_lines[0])

        return ()

    def get_rhs_ancestors(self, fileprops):
        change = fileprops.get(SVN_PROP_BZR_ANCESTRY+self.name)
        if change is None:
            return []
        assert isinstance(change, tuple)
        ancestry = []
        for l in change[1].splitlines():
            ancestry.extend(l.split("\n"))
        return ancestry

    def import_fileid_map_fileprops(self, fileprops):
        fileids = fileprops.get(SVN_PROP_BZR_FILEIDS, None)
        if fileids is None:
            return {}
        assert isinstance(fileids, tuple)
        return parse_fileid_property(fileids[1])

    def _record_merges(self, merges, fileprops):
        """Store the extra merges (non-LHS parents) in a file property.

        :param merges: List of parents.
        """
        # Bazaar Parents
        old = fileprops.get(SVN_PROP_BZR_ANCESTRY+self.name, "")
        svnprops = { SVN_PROP_BZR_ANCESTRY+self.name: old + "\t".join(merges) + "\n" }

        return svnprops
 
    def export_revision_fileprops(self, timestamp, timezone, committer, revprops, revision_id, revno, parent_ids, svn_fileprops):

        # Keep track of what Subversion properties to set later on
        svn_fileprops[SVN_PROP_BZR_REVISION_INFO] = generate_revision_metadata(
            timestamp, timezone, committer, revprops)

        if len(parent_ids) > 1:
            svn_fileprops.update(self._record_merges(parent_ids[1:], svn_fileprops))

        # Set appropriate property if revision id was specified by 
        # caller
        if revision_id is not None:
            old = svn_fileprops.get(SVN_PROP_BZR_REVISION_ID+self.name, "")
            svn_fileprops[SVN_PROP_BZR_REVISION_ID+self.name] = old + "%d %s\n" % (revno, revision_id)

    def export_message_fileprops(self, message, fileprops):
        fileprops[SVN_PROP_BZR_LOG] = message.encode("utf-8")

    def get_revision_id_fileprops(self, fileprops):
        # Lookup the revision from the bzr:revision-id-vX property
        text = fileprops.get(SVN_PROP_BZR_REVISION_ID+self.name, None)
        if text is None:
            return (None, None, 
                    self.is_bzr_revision_hidden_fileprops(fileprops))

        try:
            new_lines = find_new_lines(text)
        except ValueError, e:
            mutter(str(e))
            return (None, None, 
                    self.is_bzr_revision_hidden_fileprops(fileprops))

        if len(new_lines) != 1:
            mutter("unexpected number of lines: %r" % new_lines)
            return (None, None, self.is_bzr_revision_hidden_fileprops(fileprops))

        try:
            (revno, revid) = parse_revid_property(new_lines[0])
            return (revno, revid, self.is_bzr_revision_hidden_fileprops(fileprops))
        except errors.InvalidPropertyValue, e:
            mutter(str(e))
            return (None, None, self.is_bzr_revision_hidden_fileprops(fileprops))

    def export_fileid_map_fileprops(self, fileids, fileprops):
        if fileids != {}:
            file_id_text = generate_fileid_property(fileids)
            fileprops[SVN_PROP_BZR_FILEIDS] = file_id_text
        elif SVN_PROP_BZR_FILEIDS in fileprops:
            fileprops[SVN_PROP_BZR_FILEIDS] = ""

    def check_fileprops(self, changed_fileprops, checkresult):
        # bzr:revision-id:*
        text = changed_fileprops.get(SVN_PROP_BZR_REVISION_ID+self.name, ("", ""))
        try:
            new_lines = find_new_lines(text)
        except ValueError, e:
            checkresult.invalid_fileprop_cnt += 1
        else:
            if len(new_lines) > 1:
                checkresult.invalid_fileprop_cnt += 1

        if new_lines == []:
            return
        try:
            revid = parse_revid_property(new_lines[0])
        except errors.InvalidPropertyValue, e:
            self.invalid_fileprop_cnt += 1
            return

        text = changed_fileprops.get(SVN_PROP_BZR_REVISION_INFO, ("", ""))
        try:
            parse_revision_metadata(text[1], Revision(revid))
        except errors.InvalidPropertyValue:
            checkresult.invalid_fileprop_cnt += 1



class BzrSvnMappingRevProps(object):

    def import_revision_revprops(self, svn_revprops, rev):
        return parse_bzr_svn_revprops(svn_revprops, rev)

    def import_fileid_map_revprops(self, svn_revprops):
        if not svn_revprops.has_key(SVN_REVPROP_BZR_FILEIDS):
            return {}
        return parse_fileid_property(svn_revprops[SVN_REVPROP_BZR_FILEIDS])

    def import_text_parents_revprops(self, svn_revprops):
        if not svn_revprops.has_key(SVN_REVPROP_BZR_TEXT_PARENTS):
            return {}
        return parse_text_parents_property(svn_revprops[SVN_REVPROP_BZR_TEXT_PARENTS])

    def export_text_parents_revprops(self, text_parents, svn_revprops):
        if text_parents != {}:
            svn_revprops[SVN_REVPROP_BZR_TEXT_PARENTS] = generate_text_parents_property(text_parents)

    def import_text_revisions_revprops(self, svn_revprops):
        if not svn_revprops.has_key(SVN_REVPROP_BZR_TEXT_REVISIONS):
            return {}
        return parse_text_revisions_property(svn_revprops[SVN_REVPROP_BZR_TEXT_REVISIONS])

    def export_text_revisions_revprops(self, text_revisions, svn_revprops):
        if text_revisions != {}:
            svn_revprops[SVN_REVPROP_BZR_TEXT_REVISIONS] = generate_text_revisions_property(text_revisions)

    def get_lhs_parent_revprops(self, svn_revprops):
        return svn_revprops.get(SVN_REVPROP_BZR_BASE_REVISION)

    def get_rhs_parents_revprops(self, svn_revprops):
        return tuple(svn_revprops.get(SVN_REVPROP_BZR_MERGE, "").splitlines())

    def get_branch_root(self, revprops):
        return revprops.get(SVN_REVPROP_BZR_ROOT)

    def get_repository_uuid(self, revprops):
        return revprops.get(SVN_REVPROP_BZR_REPOS_UUID)

    def get_revision_id_revprops(self, revprops):
        if (not is_bzr_revision_revprops(revprops) or 
            not SVN_REVPROP_BZR_REVISION_ID in revprops):
            return (None, None, self.is_bzr_revision_hidden_revprops(revprops))
        revid = revprops[SVN_REVPROP_BZR_REVISION_ID]
        revno = int(revprops[SVN_REVPROP_BZR_REVNO])
        return (revno, revid, self.is_bzr_revision_hidden_revprops(revprops))

    def export_message_revprops(self, message, revprops):
        revprops[SVN_REVPROP_BZR_LOG] = message.encode("utf-8")

    def export_revision_revprops(self, uuid, branch_root, timestamp, timezone, committer, revprops, revision_id, revno, parent_ids, svn_revprops):
        svn_revprops[SVN_REVPROP_BZR_MAPPING_VERSION] = self.name
        if timestamp is not None:
            svn_revprops[SVN_REVPROP_BZR_TIMESTAMP] = format_highres_date(timestamp, timezone)

        if committer is not None:
            svn_revprops[SVN_REVPROP_BZR_COMMITTER] = committer.encode("utf-8")

        if revprops is not None:
            for name, value in revprops.iteritems():
                svn_revprops[SVN_REVPROP_BZR_REVPROP_PREFIX+name] = value.encode("utf-8")

        svn_revprops[SVN_REVPROP_BZR_ROOT] = branch_root
        svn_revprops[SVN_REVPROP_BZR_REPOS_UUID] = uuid

        if revision_id is not None:
            svn_revprops[SVN_REVPROP_BZR_REVISION_ID] = revision_id

        if len(parent_ids) > 1:
            svn_revprops[SVN_REVPROP_BZR_MERGE] = "".join([x+"\n" for x in parent_ids[1:]])
        if len(parent_ids) == 0:
            svn_revprops[SVN_REVPROP_BZR_BASE_REVISION] = NULL_REVISION
        else:
            svn_revprops[SVN_REVPROP_BZR_BASE_REVISION] = parent_ids[0]
        
        svn_revprops[SVN_REVPROP_BZR_REVNO] = str(revno)
        svn_revprops[SVN_REVPROP_BZR_USER_AGENT] = get_client_string()

    def export_fileid_map_revprops(self, fileids, revprops):
        if fileids != {}:
            revprops[SVN_REVPROP_BZR_FILEIDS] = generate_fileid_property(fileids)

    def check_revprops(self, revprops, check_result):
        pass


class SubversionMappingRegistry(foreign.VcsMappingRegistry):

    def parse_mapping_name(self, name):
        assert isinstance(name, str)
        assert name.startswith("svn-")
        name = name[len("svn-"):]
        if "-" in name:
            name, rest = name.split("-", 1)
            assert isinstance(rest, str)
            try:
                return self.get(name)(rest)
            except errors.UnknownMapping:
                raise KeyError(name)
        return self.get(name)()

    def parse_revision_id(self, revid):
        """Try to parse a Subversion revision id.
        
        :param revid: Revision id to parse
        :return: tuple with (uuid, branch_path, revno), mapping
        """
        if not revid.startswith("svn-"):
            raise InvalidRevisionId(revid, None)
        mapping_version = revid[len("svn-"):len("svn-vx")]
        mapping = self.get(mapping_version)
        return mapping.revision_id_bzr_to_foreign(revid)

    revision_id_bzr_to_foreign = parse_revision_id


mapping_registry = SubversionMappingRegistry()
mapping_registry.register_lazy('v1', 'bzrlib.plugins.svn.mapping2', 
                               'BzrSvnMappingv1', 
                               'Original bzr-svn mapping format (bzr-svn 0.2.x)')
mapping_registry.register_lazy('v2', 'bzrlib.plugins.svn.mapping2',
                               'BzrSvnMappingv2', 
                               'Second format (bzr-svn 0.3.x)')
mapping_registry.register_lazy('v3', 'bzrlib.plugins.svn.mapping3.base', 
                               'BzrSvnMappingv3', 
                               'Third format (bzr-svn 0.4.x)')
mapping_registry.register_lazy('v4', 'bzrlib.plugins.svn.mapping4', 
                               'BzrSvnMappingv4',
                               'Fourth format (bzr-svn 0.5.x)')
mapping_registry.set_default('v4')


def find_mapping_revprops(revprops):
    if SVN_REVPROP_BZR_MAPPING_VERSION in revprops:
        try:
            return mapping_registry.parse_mapping_name("svn-" + revprops[SVN_REVPROP_BZR_MAPPING_VERSION])
        except KeyError:
            return None
    return None


def find_mappings_fileprops(changed_fileprops):
    ret = []
    for k, v in changed_fileprops.iteritems():
        if k.startswith(SVN_PROP_BZR_REVISION_ID):
            try:
                # perhaps check if content change was valid?
                find_new_lines(v)
            except ValueError:
                pass
            else:
                try:
                    mapping = mapping_registry.parse_mapping_name("svn-" + k[len(SVN_PROP_BZR_REVISION_ID):])
                except KeyError:
                    pass
                else:
                    ret.append(mapping)
    return ret


def find_mapping_fileprops(changed_fileprops):
    mappings = find_mappings_fileprops(changed_fileprops)
    if len(mappings) == 0:
        return None
    return mappings[0]


def is_bzr_revision_revprops(revprops):
    """Check if the specified revision properties 
    contain bzr-svn metadata."""
    if revprops.has_key(SVN_REVPROP_BZR_MAPPING_VERSION):
        return True
    if revprops.has_key(SVN_REVPROP_BZR_SKIP):
        return False
    return None


def is_bzr_revision_fileprops(fileprops):
    """Check if the specified fileprops dictionary contains 
    bzr-svn metadata.
    """
    for k in fileprops:
        if k.startswith(SVN_PROP_BZR_PREFIX):
            return True
    return None


def estimate_bzr_ancestors(fileprops):
    """Estimate the number of bzr ancestors that a revision has based on file properties.

    """
    found = []
    for k, v in fileprops.iteritems():
        if k.startswith(SVN_PROP_BZR_REVISION_ID):
            found.append(len(v.splitlines()))
    if found != []:
        return sorted(found, reverse=True)[0]
    for k in fileprops:
        if k.startswith(SVN_PROP_BZR_PREFIX):
            return 1
    return 0


def get_roundtrip_ancestor_revids(fileprops):
    for propname, propvalue in fileprops.iteritems():
        if not propname.startswith(SVN_PROP_BZR_REVISION_ID):
            continue
        mapping_name = propname[len(SVN_PROP_BZR_REVISION_ID):]
        for line in propvalue.splitlines():
            try:
                (revno, revid) = parse_revid_property(line)
                yield (revid, revno, mapping_name)
            except errors.InvalidPropertyValue, ie:
                mutter(str(ie))


def find_roundtripped_root(revprops, path_changes):
    # Find the root path of the change
    bp = revprops.get(SVN_REVPROP_BZR_ROOT)
    if bp is not None:
        return bp
    return changes.changes_root(path_changes.keys())


def revprops_complete(revprops):
    return (SVN_REVPROP_BZR_MAPPING_VERSION in revprops or 
            SVN_REVPROP_BZR_HIDDEN in revprops)


class ForeignSubversion(foreign.ForeignVcs):

    @property
    def branch_format(self):
        from bzrlib.plugins.svn.branch import SvnBranchFormat
        return SvnBranchFormat()

    @property
    def repository_format(self):
        from bzrlib.plugins.svn.repository import SvnRepositoryFormat
        return SvnRepositoryFormat()

    def __init__(self):
        super(ForeignSubversion, self).__init__(mapping_registry)
        self.abbreviation = "svn"

    def show_foreign_revid(self, (uuid, bp, revnum)):
        return { "svn revno": "%d (on /%s)" % (revnum, bp)}

    def serialize_foreign_revid(self, (uuid, bp, revnum)):
        return "%s:%d:%s" % (uuid, revnum, urllib.quote(bp))


foreign_vcs_svn = ForeignSubversion()
