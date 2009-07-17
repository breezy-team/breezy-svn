# Copyright (C) 2009 Jelmer Vernooij <jelmer@samba.org>

# Portions copied from the bzr-keywords plugin, written by Ian Clatworthy and:
# Copyright (C) 2008 Canonical Ltd

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


"""Support for keyword expansion similar to that done by svn:keywords."""


import re

from subvertpy import (
    properties,
    )

from bzrlib import (
    debug,
    trace,
    urlutils,
    )
from bzrlib.errors import (
    BzrError,
    InvalidRevisionId,
    )
from bzrlib.filters import (
    ContentFilter,
    )

from bzrlib.plugins.svn.mapping import (
    mapping_registry,
    )


def keyword_date(revid, rev, relpath, revmeta):
    """last changed date"""
    if revmeta is not None:
        return revmeta.get_revprops().get(properties.PROP_REVISION_DATE, "")
    return properties.time_to_cstring(1000000*rev.timestamp)


def keyword_rev(revid, rev, relpath, revmeta):
    """last revno that changed this file"""
    #  See if c.revision() can be traced back to a subversion revision

    # Revision comes directly from a foreign repository
    if revmeta is not None:
        return str(revmeta.revnum)

    # Revision was once imported from a foreign repository
    try:
        foreign_revid, mapping = mapping_registry.parse_revision_id(revid)
    except InvalidRevisionId:
        pass
    else:
        return str(foreign_revid[2])

    # If we can't find the actual svn revision, just return the bzr revid
    return revid


def keyword_author(revid, rev, relpath, revmeta):
    """author of the last commit."""
    if revmeta is not None:
        return revmeta.get_revprops().get(properties.PROP_REVISION_AUTHOR, "")
    return rev.committer


def keyword_id(*args):
    """basename <space> revno <space> date <space> author"""
    return "%s %s %s %s" % (urlutils.basename(keyword_url(*args)), 
            keyword_rev(*args), keyword_date(*args), 
            keyword_author(*args)) 


def keyword_url(revid, rev, relpath, revmeta):
    # URL in the svn repository
    # See if c.revision() can be traced back to a subversion revision
    if revmeta is not None:
        return urlutils.join(revmeta.repository.base, revmeta.branch_path,
                             relpath)
    return relpath


# callback(revision_id, revision, relpath, revmeta) -> str
keywords = {
    "LastChangedDate": keyword_date,
    "Date": keyword_date,
    "LastChangedRevision": keyword_rev,
    "Rev": keyword_rev,
    "Revision": keyword_rev,
    "LastChangedBy": keyword_author, 
    "Author": keyword_author,
    "HeadURL": keyword_url,
    "URL": keyword_url,
    "Id": keyword_id,
}


# Regular expressions for matching the raw and cooked patterns
_KW_RAW_RE = re.compile(r'\$([\w\-]+)(:[^$]*)?\$')
_KW_COOKED_RE = re.compile(r'\$([\w\-]+):([^$]+)\$')


def expand_keywords(s, allowed_keywords, context=None, encoder=None):
    """Replace raw style keywords in a string.
    
    Note: If the keyword is already in the expanded style, the value is
    not replaced.

    :param s: the string
    :param keyword_dicts: an iterable of keyword dictionaries. If values
      are callables, they are executed to find the real value.
    :param context: the parameter to pass to callable values
    :param style: the style of expansion to use of None for the default
    :return: the string with keywords expanded
    """
    result = ''
    rest = s
    while True:
        match = _KW_RAW_RE.search(rest)
        if not match:
            break
        result += rest[:match.start()]
        keyword = match.group(1)
        if not keyword in allowed_keywords:
            # Unknown expansion - leave as is
            result += match.group(0)
            rest = rest[match.end():]
            continue
        try:
            expansion = keywords[keyword](context.revision_id(),
                context.revision(), context.relpath(), 
                getattr(context.revision(), "svn_meta", None))
        except AttributeError, err:
            if 'error' in debug.debug_flags:
                trace.note("error evaluating %s for keyword %s: %s",
                    expansion, keyword, err)
            expansion = "(evaluation error)"
        if '$' in expansion:
            # Expansion is not safe to be collapsed later
            expansion = "(value unsafe to expand)"
        if encoder is not None:
            expansion = encoder(expansion)
        result += "$%s: %s $" % (keyword, expansion)
        rest = rest[match.end():]
    return result + rest


def compress_keywords(s, allowed_keywords):
    """Replace cooked style keywords with raw style in a string.
    
    :param s: the string
    :return: the string with keywords compressed
    """
    result = ''
    rest = s
    while True:
        match = _KW_COOKED_RE.search(rest)
        if not match:
            break
        result += rest[:match.start()]
        keyword = match.group(1)
        if keyword in allowed_keywords:
            result += "$%s$" % keyword
        else:
            result += match.group(0)
        rest = rest[match.end():]
    return result + rest


class SubversionKeywordContentFilter(ContentFilter):

    def __init__(self, allowed_keywords):
        self.allowed_keywords = allowed_keywords

    def reader(self, chunks, context=None):
        """Filter that replaces keywords with their compressed form."""
        text = ''.join(chunks)
        return [compress_keywords(text, self.allowed_keywords)]

    def writer(self, chunks, context, encoder=None):
        """Keyword expander."""
        text = ''.join(chunks)
        return [expand_keywords(text, self.allowed_keywords, context=context,
            encoder=encoder)]


def create_svn_keywords_filter(value):
    if not value:
        return
    keywords = value.split(" ")
    for k in keywords:
        if not k in keywords:
            raise BzrError("Unknown svn keyword %s" % k)
    if keywords == []:
        return []
    return [SubversionKeywordContentFilter(keywords)]
