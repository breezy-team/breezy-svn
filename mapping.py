# Copyright (C) 2005-2008 Jelmer Vernooij <jelmer@samba.org>
 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Maps between Subversion and Bazaar semantics."""

from bzrlib import foreign, osutils
from bzrlib.errors import InvalidRevisionId
from bzrlib.revision import NULL_REVISION
from bzrlib.trace import mutter

from bzrlib.plugins.svn import errors, version_info, get_client_string

import calendar
from subvertpy import properties
import time
import urllib

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
        return None
    try:
        return log.decode("utf-8")
    except UnicodeDecodeError:
        return log


def parse_svn_revprops(svn_revprops, rev):
    rev.committer = svn_revprops.get(properties.PROP_REVISION_AUTHOR, "")
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
        try:
            key, value = l.split(": ", 2)
        except ValueError:
            raise errors.InvalidPropertyValue(SVN_PROP_BZR_REVISION_INFO, 
                    "Missing : in revision metadata")
        if key == "committer":
            rev.committer = value.decode("utf-8")
        elif key == "timestamp":
            (rev.timestamp, rev.timezone) = unpack_highres_date(value)
        elif key == "properties":
            in_properties = True
        elif key[0] == "\t" and in_properties:
            rev.properties[str(key[1:])] = value.decode("utf-8")
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
        text += "committer: %s\n" % committer.encode("utf-8")
    if revprops is not None and revprops != {}:
        text += "properties: \n"
        for k, v in sorted(revprops.iteritems()):
            text += "\t%s: %s\n" % (k.encode("utf-8"), v.encode("utf-8"))
    assert isinstance(text, str)
    return text


def parse_bzr_svn_revprops(props, rev):
    """Update a Revision object from a set of Subversion revision properties.
    
    :param props: Dictionary with Subversion revision properties.
    :param rev: Revision object
    """
    if props.has_key(SVN_REVPROP_BZR_TIMESTAMP):
        (rev.timestamp, rev.timezone) = unpack_highres_date(props[SVN_REVPROP_BZR_TIMESTAMP])

    if props.has_key(SVN_REVPROP_BZR_COMMITTER):
        rev.committer = props[SVN_REVPROP_BZR_COMMITTER].decode("utf-8")

    if props.has_key(SVN_REVPROP_BZR_LOG):
        rev.message = props[SVN_REVPROP_BZR_LOG]

    for name, value in props.iteritems():
        if name.startswith(SVN_REVPROP_BZR_REVPROP_PREFIX):
            rev.properties[name[len(SVN_REVPROP_BZR_REVPROP_PREFIX):]] = value


def parse_required_features_property(text):
    return set(text.split(","))


class BzrSvnMapping(foreign.VcsMapping):
    """Class that maps between Subversion and Bazaar semantics."""
    experimental = False
    _warned_experimental = False
    roundtripping = False
    can_use_revprops = False
    can_use_fileprops = False
    supports_hidden = False
    restricts_branch_paths = False

    def __init__(self):
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

    def is_tag(self, tag_path):
        raise NotImplementedError(self.is_tag)

    def is_branch_or_tag(self, path):
        return self.is_branch(path) or self.is_tag(path)

    @staticmethod
    def generate_file_id(uuid, revnum, branch, inv_path):
        """Create a file id identifying a Subversion file.

        :param uuid: UUID of the repository
        :param revnum: Revision number at which the file was introduced.
        :param branch: Branch path of the branch in which the file was introduced.
        :param inv_path: Original path of the file within the inventory
        """
        raise NotImplementedError

    def import_revision(self, revprops, fileprops, uuid, branch, revnum, rev):
        """Update a Revision object from Subversion revision and branch 
        properties.

        :param revprops: Dictionary with Subversion revision properties.
        :param fileprops: Dictionary with Subversion file properties on the 
                          branch root.
        :param revnum: Revision number in Subversion.
        :param rev: Revision object to import data into.
        """
        raise NotImplementedError(self.import_revision)

    def get_lhs_parent(self, branch_path, revprops, fileprops):
        """Determine the left hand side parent, if it was explicitly recorded.

        If not explicitly recorded, returns None. Returns NULL_REVISION if 
        there is no lhs parent.

        """
        return None

    def get_rhs_parents(self, branch_path, revprops, fileprops):
        """Obtain the right-hand side parents for a revision.

        """
        raise NotImplementedError(self.get_rhs_parents)

    def get_rhs_ancestors(self, branch_path, revprops, fileprops):
        """Obtain the right-hand side ancestors for a revision.

        """
        raise NotImplementedError(self.get_rhs_ancestors)

    def import_fileid_map(self, revprops, fileprops):
        """Obtain the file id map for a revision from the properties.

        """
        raise NotImplementedError(self.import_fileid_map)

    def export_fileid_map(self, fileids, revprops, fileprops):
        """Adjust the properties for a file id map.

        :param fileids: Dictionary
        :param revprops: Subversion revision properties
        :param fileprops: File properties
        """
        raise NotImplementedError(self.export_fileid_map)

    def import_text_parents(self, revprops, fileprops):
        """Obtain a text parent map from properties.

        :param revprops: Subversion revision properties.
        :param fileprops: File properties.
        """
        raise NotImplementedError(self.import_text_parents)

    def export_text_parents(self, text_parents, revprops, fileprops):
        """Store a text parent map.

        :param text_parents: Text parent map
        :param revprops: Revision properties
        :param fileprops: File properties
        """
        raise NotImplementedError(self.export_text_parents)

    def export_text_revisions(self, text_revisions, revprops, fileprops):
        """Store a text revisions map.

        :param text_parents: Text revision map
        :param revprops: Revision properties
        :param fileprops: File properties
        """
        raise NotImplementedError(self.export_text_revisions)

    def import_text_revisions(self, revprops, fileprops):
        raise NotImplementedError(self.import_text_revisions)

    def export_revision(self, branch_root, timestamp, timezone, committer, revprops, revision_id, revno, parent_ids, svn_revprops, svn_fileprops):
        """Determines the revision properties and branch root file 
        properties.
        """
        raise NotImplementedError(self.export_revision)

    def export_message(self, log, revprops, fileprops):
        raise NotImplementedError(self.export_message)

    def get_revision_id(self, branch_path, revprops, fileprops):
        raise NotImplementedError(self.get_revision_id)

    def supports_tags(self):
        raise NotImplementedError(self.supports_tags)

    @classmethod
    def get_test_instance(cls):
        return cls()

    def is_bzr_revision_hidden(self, revprops, changed_fileprops):
        return False

    def export_hidden(self, revprops, fileprops):
        raise NotImplementedError(self.export_hidden)

    def show_foreign_revid(self, (uuid, bp, revnum)):
        return { "svn revno": "%d (on /%s)" % (revnum, bp)}


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
        ret[urllib.unquote(entry)] = filter(lambda x: x != "", [osutils.safe_revision_id(parent_revid) for parent_revid in parts[1:]])
    return ret


def parse_text_revisions_property(text):
    ret = {}
    for line in text.splitlines():
        (entry, revid) = line.split("\t", 1)
        ret[urllib.unquote(entry)] = osutils.safe_revision_id(revid)
    return ret


def generate_text_parents_property(text_parents):
    return "".join(["%s\t%s\n" % (urllib.quote(path.encode("utf-8")), "\t".join(text_parents[path])) for path in sorted(text_parents.keys())])


def generate_text_revisions_property(text_revisions):
    return "".join(["%s\t%s\n" % (urllib.quote(path.encode("utf-8")), text_revisions[path]) for path in sorted(text_revisions.keys())])


class BzrSvnMappingFileProps(object):
    def __init__(self, name):
        self.name = name

    def import_revision(self, svn_revprops, fileprops, uuid, branch, revnum, rev):
        parse_svn_revprops(svn_revprops, rev)
        if SVN_PROP_BZR_LOG in fileprops:
            rev.message = fileprops[SVN_PROP_BZR_LOG][1]
        metadata = fileprops.get(SVN_PROP_BZR_REVISION_INFO)
        if metadata is not None:
            parse_revision_metadata(metadata[1], rev)

    def import_text_parents(self, svn_revprops, fileprops):
        metadata = fileprops.get(SVN_PROP_BZR_TEXT_PARENTS)
        if metadata is None:
            return {}
        return parse_text_parents_property(metadata[1])

    def import_text_revisions(self, svn_revprops, fileprops):
        metadata = fileprops.get(SVN_PROP_BZR_TEXT_REVISIONS)
        if metadata is None:
            return {}
        return parse_text_revisions_property(metadata[1])

    def export_text_parents(self, text_parents, svn_revprops, fileprops):
        if text_parents != {}:
            fileprops[SVN_PROP_BZR_TEXT_PARENTS] = generate_text_parents_property(text_parents)
        elif SVN_PROP_BZR_TEXT_PARENTS in fileprops:
            fileprops[SVN_PROP_BZR_TEXT_PARENTS] = ""

    def export_text_revisions(self, text_revisions, svn_revprops, fileprops):
        if text_revisions != {}:
            fileprops[SVN_PROP_BZR_TEXT_REVISIONS] = generate_text_revisions_property(text_revisions)
        elif SVN_PROP_BZR_TEXT_REVISIONS in fileprops:
            fileprops[SVN_PROP_BZR_TEXT_REVISIONS] = ""

    def get_rhs_parents(self, branch_path, revprops, fileprops):
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

    def get_rhs_ancestors(self, branch_path, revprops, fileprops):
        ancestry = []
        for l in fileprops.get(SVN_PROP_BZR_ANCESTRY+self.name, (None, ""))[1].splitlines():
            ancestry.extend(l.split("\n"))
        return ancestry

    def import_fileid_map(self, svn_revprops, fileprops):
        fileids = fileprops.get(SVN_PROP_BZR_FILEIDS, None)
        if fileids is None:
            return {}
        return parse_fileid_property(fileids[1])

    def _record_merges(self, merges, fileprops):
        """Store the extra merges (non-LHS parents) in a file property.

        :param merges: List of parents.
        """
        # Bazaar Parents
        old = fileprops.get(SVN_PROP_BZR_ANCESTRY+self.name, "")
        svnprops = { SVN_PROP_BZR_ANCESTRY+self.name: old + "\t".join(merges) + "\n" }

        return svnprops
 
    def export_revision(self, branch_root, timestamp, timezone, committer, revprops, revision_id, revno, parent_ids, svn_revprops, svn_fileprops):

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

    def export_message(self, message, revprops, fileprops):
        fileprops[SVN_PROP_BZR_LOG] = message.encode("utf-8")

    def get_revision_id(self, branch_path, revprops, fileprops):
        # Lookup the revision from the bzr:revision-id-vX property
        text = fileprops.get(SVN_PROP_BZR_REVISION_ID+self.name, None)
        if text is None:
            return (None, None)

        try:
            new_lines = find_new_lines(text)
        except ValueError, e:
            mutter(str(e))
            return (None, None)

        if len(new_lines) != 1:
            mutter("unexpected number of lines: %r" % new_lines)
            return (None, None)

        try:
            return parse_revid_property(new_lines[0])
        except errors.InvalidPropertyValue, e:
            mutter(str(e))
            return (None, None)

    def export_fileid_map(self, fileids, revprops, fileprops):
        if fileids != {}:
            file_id_text = generate_fileid_property(fileids)
            fileprops[SVN_PROP_BZR_FILEIDS] = file_id_text
        elif SVN_PROP_BZR_FILEIDS in fileprops:
            fileprops[SVN_PROP_BZR_FILEIDS] = ""


class BzrSvnMappingRevProps(object):
    def import_revision(self, svn_revprops, fileprops, uuid, branch, revnum, rev):
        parse_svn_revprops(svn_revprops, rev)
        parse_bzr_svn_revprops(svn_revprops, rev)

    def import_fileid_map(self, svn_revprops, fileprops):
        if not svn_revprops.has_key(SVN_REVPROP_BZR_FILEIDS):
            return {}
        return parse_fileid_property(svn_revprops[SVN_REVPROP_BZR_FILEIDS])

    def import_text_parents(self, svn_revprops, fileprops):
        if not svn_revprops.has_key(SVN_REVPROP_BZR_TEXT_PARENTS):
            return {}
        return parse_text_parents_property(svn_revprops[SVN_REVPROP_BZR_TEXT_PARENTS])

    def export_text_parents(self, text_parents, svn_revprops, fileprops):
        if text_parents != {}:
            svn_revprops[SVN_REVPROP_BZR_TEXT_PARENTS] = generate_text_parents_property(text_parents)

    def import_text_revisions(self, svn_revprops, fileprops):
        if not svn_revprops.has_key(SVN_REVPROP_BZR_TEXT_REVISIONS):
            return {}
        return parse_text_revisions_property(svn_revprops[SVN_REVPROP_BZR_TEXT_REVISIONS])

    def export_text_revisions(self, text_revisions, svn_revprops, fileprops):
        if text_revisions != {}:
            svn_revprops[SVN_REVPROP_BZR_TEXT_REVISIONS] = generate_text_revisions_property(text_revisions)

    def get_lhs_parent(self, branch_parent, svn_revprops, fileprops):
        return svn_revprops.get(SVN_REVPROP_BZR_BASE_REVISION)

    def get_rhs_parents(self, branch_path, svn_revprops, fileprops):
        return tuple(svn_revprops.get(SVN_REVPROP_BZR_MERGE, "").splitlines())

    def get_branch_root(self, revprops):
        return revprops.get(SVN_REVPROP_BZR_ROOT)

    def get_revision_id(self, branch_path, revprops, fileprops):
        if not is_bzr_revision_revprops(revprops) or not SVN_REVPROP_BZR_REVISION_ID in revprops:
            return (None, None)
        revid = revprops[SVN_REVPROP_BZR_REVISION_ID]
        revno = int(revprops[SVN_REVPROP_BZR_REVNO])
        return (revno, revid)

    def export_message(self, message, revprops, fileprops):
        revprops[SVN_REVPROP_BZR_LOG] = message.encode("utf-8")

    def export_revision(self, branch_root, timestamp, timezone, committer, revprops, revision_id, revno, parent_ids, svn_revprops, svn_fileprops):

        if timestamp is not None:
            svn_revprops[SVN_REVPROP_BZR_TIMESTAMP] = format_highres_date(timestamp, timezone)

        if committer is not None:
            svn_revprops[SVN_REVPROP_BZR_COMMITTER] = committer.encode("utf-8")

        if revprops is not None:
            for name, value in revprops.iteritems():
                svn_revprops[SVN_REVPROP_BZR_REVPROP_PREFIX+name] = value.encode("utf-8")

        svn_revprops[SVN_REVPROP_BZR_ROOT] = branch_root

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

    def export_fileid_map(self, fileids, revprops, fileprops):
        if fileids != {}:
            revprops[SVN_REVPROP_BZR_FILEIDS] = generate_fileid_property(fileids)

    def get_rhs_ancestors(self, branch_path, revprops, fileprops):
        raise NotImplementedError(self.get_rhs_ancestors)


class SubversionMappingRegistry(foreign.VcsMappingRegistry):

    def parse_mapping_name(self, name):
        assert isinstance(name, str)
        assert name.startswith("svn-")
        name = name[len("svn-"):]
        if "-" in name:
            name, rest = name.split("-", 1)
            assert isinstance(rest, str)
            return self.get(name)(rest)
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
mapping_registry.register_lazy('v3', 'bzrlib.plugins.svn.mapping3', 
                               'BzrSvnMappingv3FileProps', 
                               'Third format (bzr-svn 0.4.x)')
mapping_registry.register_lazy('v4', 'bzrlib.plugins.svn.mapping4', 
                               'BzrSvnMappingv4',
                               'Fourth format (bzr-svn 0.5.x)')
mapping_registry.set_default('v4')


def find_mapping_revprops(revprops):
    if SVN_REVPROP_BZR_MAPPING_VERSION in revprops:
        try:
            cls = mapping_registry.get(revprops[SVN_REVPROP_BZR_MAPPING_VERSION])
            ret = cls.from_revprops(revprops)
        except KeyError:
            pass
        except NotImplementedError:
            pass
        else:
            if ret is not None:
                return ret
    return None


def find_mapping_fileprops(changed_fileprops):
    for k, v in changed_fileprops.iteritems():
        if k.startswith(SVN_PROP_BZR_REVISION_ID):
            return mapping_registry.parse_mapping_name("svn-" + k[len(SVN_PROP_BZR_REVISION_ID):])
    return None


def find_mapping(revprops, changed_fileprops):
    """Find a mapping instance based on the revprops and fileprops set on a revision.

    :param revprops: Revision properties.
    :param fileprops: File properties set on branch root.
    :return: BzrSvnMapping instance or None if no mapping found.
    """
    return find_mapping_revprops(revprops) or find_mapping_fileprops(changed_fileprops)


def is_bzr_revision_revprops(revprops):
    if revprops.has_key(SVN_REVPROP_BZR_MAPPING_VERSION):
        return True
    if revprops.has_key(SVN_REVPROP_BZR_SKIP):
        return False
    return None


def is_bzr_revision_fileprops(fileprops):
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

foreign_vcs_svn = foreign.ForeignVcs(mapping_registry)
