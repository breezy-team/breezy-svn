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
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

"""Support for Subversion branches

Bazaar can be used with Subversion branches through the bzr-svn plugin.

Most Bazaar commands should work fine with Subversion branches.

bzr-svn also adds new commands to Bazaar:

 - bzr svn-import
 - bzr svn-layout

For more information about bzr-svn, see the bzr-svn FAQ.

"""

from __future__ import absolute_import

import breezy

from .info import (
    bzr_plugin_version as version_info,
    bzr_compatible_versions,
    subvertpy_minimum_version,
    )

if version_info[3] == 'final':
    version_string = '%d.%d.%d' % version_info[:3]
else:
    version_string = '%d.%d.%d%s%d' % version_info
__version__ = version_string

from breezy.i18n import load_plugin_translations
translation = load_plugin_translations("bzr-svn")
gettext = translation.gettext


from breezy import (
    config as _mod_bzr_config,  # Or we mask plugins.svn.config
    urlutils,
    )
from breezy.branch import (
    format_registry as branch_format_registry,
    network_format_registry as branch_network_format_registry,
    )
from breezy.commands import (
    plugin_cmds,
    )
from breezy.controldir import (
    ControlDirFormat,
    Prober,
    format_registry,
    network_format_registry,
    )
from breezy.errors import (
    DependencyNotPresent,
    InvalidRevisionId,
    InProcessTransport,
    InvalidHttpResponse,
    NotBranchError,
    NoSuchFile,
    UnsupportedFormatError,
    )
from breezy.repository import (
    format_registry as repository_format_registry,
    network_format_registry as repository_network_format_registry,
    )
from breezy.transport import (
    ConnectedTransport,
    register_lazy_transport,
    register_transport_proto,
    )



def check_subversion_version():
    """Check that Subversion is compatible.

    """
    # Installed ?
    from subvertpy import ra, __version__ as subvertpy_version
    ra_version = ra.version()
    if (ra_version[1] >= 5 and getattr(ra, 'SVN_REVISION', None) and
        27729 <= ra.SVN_REVISION < 31470):
        raise DependencyNotPresent("subvertpy",
                'bzr-svn: Installed Subversion has buggy svn.ra.get_log() '
                'implementation, please install newer.')

    from breezy.trace import mutter
    versions = ["Subversion %d.%d.%d (%s)" % ra_version]
    if getattr(ra, "api_version", None) is not None and ra.api_version() != ra_version:
        versions.append("Subversion API %d.%d.%d (%s)" % ra.api_version())
    versions.append("subvertpy %d.%d.%d" % subvertpy_version)
    mutter("bzr-svn: using " + ", ".join(versions))

    if subvertpy_version < subvertpy_minimum_version:
        raise DependencyNotPresent("subvertpy", "bzr-svn: at least subvertpy %d.%d.%d is required, %d.%d.%d is installed." % (subvertpy_minimum_version + subvertpy_version))


def get_client_string():
    """Return a string that can be send as part of the User Agent string."""
    return "bzr%s+bzr-svn%s" % (breezy.__version__, __version__)


def init_subvertpy():
    try:
        import subvertpy
    except ImportError as e:
        raise DependencyNotPresent("subvertpy", "bzr-svn: %s" % str(e))

    check_subversion_version()

    def get_ssh_vendor():
        import breezy.transport.ssh
        return breezy.transport.ssh._get_ssh_vendor()
    import subvertpy.ra_svn
    subvertpy.ra_svn.get_ssh_vendor = get_ssh_vendor

_versions_checked = False
def lazy_check_versions():
    """Check whether all dependencies have the right versions.

    :note: Only checks once, caches the result."""
    global _versions_checked
    if _versions_checked:
        return
    _versions_checked = True
    init_subvertpy()


class SvnProber(Prober):

    @classmethod
    def _check_versions(cls):
        lazy_check_versions()


class SvnWorkingTreeProber(SvnProber):

    @classmethod
    def _check_versions(cls):
        lazy_check_versions()

    def probe_transport(self, transport):
        from breezy.transport.local import LocalTransport

        if (not isinstance(transport, LocalTransport)
            or not transport.has(".svn")):
            raise NotBranchError(path=transport.base)

        self._check_versions()
        import subvertpy
        from subvertpy.wc import check_wc
        try:
            version = check_wc(transport.local_abspath('.').encode("utf-8"))
        except subvertpy.SubversionException as e:
            if e.args[1] == subvertpy.ERR_WC_UPGRADE_REQUIRED:
                raise UnsupportedFormatError(msg)
            raise
        from .workingtree import SvnWorkingTreeDirFormat
        return SvnWorkingTreeDirFormat(version)

    @classmethod
    def known_formats(cls):
        try:
            cls._check_versions()
        except DependencyNotPresent:
            return set()
        else:
            from .workingtree import SvnWorkingTreeDirFormat
            return set([SvnWorkingTreeDirFormat()])


class SvnRemoteProber(SvnProber):

    _supported_schemes = ["http", "https", "file", "svn"]

    @classmethod
    def priority(klas, transport):
        if 'svn' in transport.base:
            return -15
        return -11

    def probe_transport(self, transport):

        try:
            url = transport.external_url()
        except InProcessTransport:
            # bzr-svn not supported on MemoryTransport
            raise NotBranchError(path=transport.base)

        if url.startswith("readonly+"):
            url = url[len("readonly+"):]

        if url.startswith("file://"):
            # Cheaper way to figure out if there is a svn repo
            maybe = False
            subtransport = transport
            while subtransport:
                try:
                    if subtransport.has("format"):
                        maybe = True
                        break
                except UnicodeEncodeError:
                    pass
                prevsubtransport = subtransport
                subtransport = prevsubtransport.clone("..")
                if subtransport.base == prevsubtransport.base:
                    break
            if not maybe:
                raise NotBranchError(path=transport.base)

        scheme = url.split(":")[0]
        if (not scheme.startswith("svn+") and
                not scheme in self._supported_schemes):
            raise NotBranchError(path=transport.base)

        # If this is a HTTP transport, use the existing connection to check
        # that the remote end supports version control.
        if scheme in ("http", "https"):
            priv_transport = getattr(transport, "_decorated", transport)
            try:
                headers = priv_transport._options('.')
            except (InProcessTransport, NoSuchFile, InvalidHttpResponse):
                raise NotBranchError(path=transport.base)
            else:
                dav_entries = set()
                for key, value in headers:
                    if key.upper() == 'DAV':
                        dav_entries.update(
                            [x.strip() for x in value.split(',')])
                if "version-control" not in dav_entries:
                    raise NotBranchError(path=transport.base)

        self._check_versions()
        from .transport import get_svn_ra_transport
        from .errors import DavRequestFailed
        import subvertpy
        try:
            transport = get_svn_ra_transport(transport)
        except subvertpy.SubversionException as e:
            num = e.args[1]
            if num == subvertpy.ERR_RA_DAV_NOT_VCC:
                raise NotBranchError(path=transport.base)
            if num in (subvertpy.ERR_RA_ILLEGAL_URL,
                       subvertpy.ERR_RA_LOCAL_REPOS_OPEN_FAILED,
                       subvertpy.ERR_BAD_URL):
                from breezy.trace import mutter
                mutter("Unable to open %r with Subversion: %s", transport, msg)
                raise NotBranchError(path=transport.base)
            raise
        except (InProcessTransport, NoSuchFile, urlutils.InvalidURL, InvalidHttpResponse):
            raise NotBranchError(path=transport.base)
        except DavRequestFailed as e:
            if "501 Unsupported method" in e[1]:
                raise NotBranchError(path=transport.base)
            else:
                raise

        from .remote import SvnRemoteFormat
        return SvnRemoteFormat()

    @classmethod
    def known_formats(cls):
        from .remote import SvnRemoteFormat
        return set([SvnRemoteFormat()])


register_transport_proto('svn+ssh://',
    help="Access using the Subversion smart server tunneled over SSH.")
register_transport_proto('svn+http://')
register_transport_proto('svn+https://')
register_transport_proto('svn://',
    help="Access using the Subversion smart server.")
register_lazy_transport('svn://', __name__ + '.transport',
                        'SvnRaTransport')
register_lazy_transport('svn+', __name__ + '.transport',
                        'SvnRaTransport')

#BzrDirFormat.register_control_server_format(format.SvnRemoteFormat)
# Register as the first control server format, since the default smart
# server implementation tries to do a POST request against .bzr/smart and
# this causes some Subversion servers to reply with 401 Authentication required
# even though they are accessible without authentication.
ControlDirFormat.register_prober(SvnWorkingTreeProber)
ControlDirFormat.register_prober(SvnRemoteProber)

network_format_registry.register_lazy("svn-wc",
    __name__ + '.workingtree', 'SvnWorkingTreeDirFormat')
network_format_registry.register_lazy("subversion",
    __name__ + '.remote', 'SvnRemoteFormat')
branch_format_registry.register_extra_lazy(
    __name__ + '.branch', 'SvnBranchFormat')
from breezy.workingtree import (
    format_registry as workingtree_format_registry,
    )
workingtree_format_registry.register_extra_lazy(
    __name__ + '.workingtree', 'SvnWorkingTreeFormat')
branch_network_format_registry.register_lazy("subversion",
    __name__ + '.branch', 'SvnBranchFormat')
repository_network_format_registry.register_lazy("subversion",
    __name__ + '.repository', 'SvnRepositoryFormat')
register_extra_lazy_repository_format = getattr(repository_format_registry,
    'register_extra_lazy')
register_extra_lazy_repository_format(__name__ + '.repository',
    'SvnRepositoryFormat')

format_registry.register_lazy("subversion", __name__ + ".remote",
                         "SvnRemoteFormat",
                         "Subversion repository. ",
                         native=False)
format_registry.register_lazy("subversion-wc", __name__ + ".workingtree",
                         "SvnWorkingTreeDirFormat",
                         "Subversion working copy. ",
                         native=False, hidden=True)

_mod_bzr_config.credential_store_registry.register_lazy(
    "subversion", __name__ + ".auth", "SubversionCredentialStore",
    help=__doc__, fallback=True)


plugin_cmds.register_lazy('cmd_svn_import', [], __name__ + '.commands')
plugin_cmds.register_lazy('cmd_svn_branching_scheme', [],
                          __name__ + '.mapping3.commands')
plugin_cmds.register_lazy('cmd_svn_layout', [],
                          __name__ + '.commands')
plugin_cmds.register_lazy('cmd_svn_branches', [],
                          __name__ + '.commands')
plugin_cmds.register_lazy('cmd_fix_svn_ancestry', [],
                          __name__ + '.commands')


from breezy.filters import filter_stacks_registry
filter_stacks_registry.register_lazy(
    "svn-keywords", __name__ + ".keywords", "create_svn_keywords_filter")


def info_svn_repository(repository, stats, outf):
    if "svn-uuid" in stats:
        outf.write("Subversion UUID: %s\n" % stats["svn-uuid"])
    if "svn-last-revnum" in stats:
        outf.write("Subversion Last Revision: %d\n" % stats["svn-last-revnum"])


def extract_svn_foreign_revid(rev):
    try:
        foreign_revid = rev.foreign_revid
    except AttributeError:
        from .mapping import mapping_registry
        foreign_revid, mapping = \
            mapping_registry.parse_revision_id(rev.revision_id)
        return foreign_revid
    else:
        from .mapping import foreign_vcs_svn
        if rev.mapping.vcs == foreign_vcs_svn:
            return foreign_revid
        else:
            raise InvalidRevisionId(rev.revision_id, None)


def update_stanza(rev, stanza):
    try:
        (uuid, branch_path, revno) = extract_svn_foreign_revid(rev)
    except InvalidRevisionId:
        pass
    else:
        stanza.add("svn-revno", str(revno))
        stanza.add("svn-uuid", uuid)

from breezy.hooks import install_lazy_named_hook
install_lazy_named_hook("breezy.version_info_formats.format_rio",
    "RioVersionInfoBuilder.hooks", "revision", update_stanza, "svn metadata")
install_lazy_named_hook("breezy.info", "hooks",
        'repository', info_svn_repository, "svn repository info")

try:
    from breezy.registry import register_lazy
except ImportError:
    from breezy.diff import format_registry as diff_format_registry
    diff_format_registry.register_lazy(
        'svn', __name__ + '.send', 'SvnDiffTree', 'Subversion diff format')

    from breezy.revisionspec import (
        revspec_registry,
        )
    revspec_registry.register_lazy(
        "svn:", __name__ + ".revspec", "RevisionSpec_svn")

    from breezy.send import format_registry as send_format_registry
    send_format_registry.register_lazy('svn', __name__ + '.send',
                                       'send_svn', 'Subversion diff format')
    from breezy.foreign import (
        foreign_vcs_registry,
        )
    foreign_vcs_registry.register_lazy(
        "svn", __name__ + ".mapping", "foreign_vcs_svn")
    from breezy.help_topics import topic_registry
    topic_registry.register_lazy(
        'svn-layout', __name__ + '.layout', 'help_layout',
        'Subversion repository layouts')
else:
    register_lazy("breezy.diff", "format_registry", 'svn', __name__ + '.send',
            'SvnDiffTree', help='Subversion diff format')
    register_lazy("breezy.revisionspec", "revspec_registry", "svn:",
            __name__ + ".revspec", "RevisionSpec_svn")
    register_lazy("breezy.send", "format_registry", 'svn',
            __name__ + '.send', 'send_svn', 'Subversion diff format')
    register_lazy("breezy.foreign", "foreign_vcs_registry", "svn",
            __name__ + ".mapping", "foreign_vcs_svn")
    register_lazy("breezy.help_topics", "topic_registry", 'svn-layout',
            __name__ + '.layout', 'help_layout',
            'Subversion repository layouts')


_mod_bzr_config.option_registry.register_lazy('layout',
    __name__ + '.config', 'svn_layout_option')
_mod_bzr_config.option_registry.register_lazy('guessed-layout',
    __name__ + '.config', 'svn_guessed_layout_option')
_mod_bzr_config.option_registry.register_lazy('branches',
    __name__ + '.config', 'svn_branches_option')
_mod_bzr_config.option_registry.register_lazy('tags',
    __name__ + '.config', 'svn_tags_option')
_mod_bzr_config.option_registry.register_lazy('override-svn-revprops',
    __name__ + '.config', 'svn_override_revprops')
_mod_bzr_config.option_registry.register_lazy('log-strip-trailing-newline',
    __name__ + '.config', 'svn_log_strip_trailing_new_line')
_mod_bzr_config.option_registry.register_lazy('push_merged_revisions',
    __name__ + '.config', 'svn_push_merged_revisions')
_mod_bzr_config.option_registry.register_lazy('allow_metadata_in_file_properties',
    __name__ + '.config', 'svn_allow_metadata_in_fileprops')

def test_suite():
    """Returns the testsuite for bzr-svn."""
    from unittest import TestSuite
    from . import tests
    suite = TestSuite()
    suite.addTest(tests.test_suite())
    return suite


if __name__ == '__main__':
    print ("This is a Bazaar plugin. Copy this directory to ~/.bazaar/plugins "
          "to use it.\n")
elif __name__ != 'breezy.plugins.svn':
    raise ImportError('The Subversion plugin must be installed as'
                      ' breezy.plugins.svn not %s' % __name__)
