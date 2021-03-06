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

"""Utility functions for dealing with SVK properties."""

from __future__ import absolute_import

from six import text_type

from . import errors

SVN_PROP_SVK_MERGE = 'svk:merge'

# Some previous versions of svk occasionally recorded a merge ticket which
# contained a line containing only two colons. This is used to filter them out
filter_svk_merge_ticket = lambda line: line != '::'

parse_svk_features = lambda text: set(filter(filter_svk_merge_ticket, text.splitlines()))

serialize_svk_features = lambda features: "".join([x+"\n" for x in sorted(features)])

def svk_features_merged_since(new_text, old_text=""):
    return parse_svk_features(new_text).difference(parse_svk_features(old_text))


def parse_svk_feature(feature):
    """Parse a svk feature identifier.

    :param feature: The feature identifier as string.
    :return: tuple with uuid, branch path and revnum
    """
    try:
        (uuid, branch, revnum) = feature.split(":", 3)
    except ValueError:
        raise errors.InvalidPropertyValue(SVN_PROP_SVK_MERGE,
                "not enough colons")
    return (uuid, branch.strip("/").decode('utf-8'), int(revnum))


def generate_svk_feature(foreign_revid):
    """Create a SVK feature identifier.

    :param uuid: Subversion repository UUID
    :param branch: Branch path
    :param revnum: Revision number
    :return: Matching SVK feature identifier.
    """
    (uuid, branch, revnum) = foreign_revid
    assert isinstance(revnum, int)
    assert isinstance(uuid, str)
    assert isinstance(branch, text_type) and (branch == u"" or not branch.startswith(u"/"))
    return b"%s:/%s:%d" % (uuid, branch.encode('utf-8'), revnum)


def estimate_svk_ancestors(fileprops):
    return len(fileprops.get(SVN_PROP_SVK_MERGE,"").splitlines())
