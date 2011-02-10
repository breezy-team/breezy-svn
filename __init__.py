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
import bzrlib
import bzrlib.api

from info import (
    bzr_plugin_version as version_info,
    bzr_compatible_versions,
    subvertpy_minimum_version,
    )

if version_info[3] == 'final':
    version_string = '%d.%d.%d' % version_info[:3]
else:
    version_string = '%d.%d.%d%s%d' % version_info
__version__ = version_string

bzrlib.api.require_any_api(bzrlib, bzr_compatible_versions)


from bzrlib import config
from bzrlib.branch import (
    network_format_registry as branch_network_format_registry,
    )
from bzrlib.commands import (
    plugin_cmds,
    )
from bzrlib.errors import (
    DependencyNotPresent,
    InvalidRevisionId,
    )
from bzrlib.filters import (
    lazy_register_filter_stack_map,
    )
from bzrlib.foreign import (
    foreign_vcs_registry,
    )
from bzrlib.help_topics import (
    topic_registry,
    )
from bzrlib.repository import (
    format_registry as repository_format_registry,
    network_format_registry as repository_network_format_registry,
    )
from bzrlib.revisionspec import (
    revspec_registry,
    )
from bzrlib.transport import (
    register_lazy_transport,
    register_transport_proto,
    )
from bzrlib.version_info_formats.format_rio import (
    RioVersionInfoBuilder,
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

    from bzrlib.trace import mutter
    versions = ["Subversion %d.%d.%d (%s)" % ra_version]
    if getattr(ra, "api_version", None) is not None and ra.api_version() != ra_version:
        versions.append("Subversion API %d.%d.%d (%s)" % ra.api_version())
    versions.append("subvertpy %d.%d.%d" % subvertpy_version)
    mutter("bzr-svn: using " + ", ".join(versions))

    if subvertpy_version < subvertpy_minimum_version:
        raise DependencyNotPresent("subvertpy", "bzr-svn: at least subvertpy %d.%d.%d is required, %d.%d.%d is installed." % (subvertpy_minimum_version + subvertpy_version))


def get_client_string():
    """Return a string that can be send as part of the User Agent string."""
    return "bzr%s+bzr-svn%s" % (bzrlib.__version__, __version__)


def init_subvertpy():
    try:
        import subvertpy
    except ImportError, e:
        raise DependencyNotPresent("subvertpy", "bzr-svn: %s" % str(e))

    check_subversion_version()

    import subvertpy.ra_svn
    import bzrlib.transport.ssh
    subvertpy.ra_svn.get_ssh_vendor = bzrlib.transport.ssh._get_ssh_vendor

_versions_checked = False
def lazy_check_versions():
    """Check whether all dependencies have the right versions.

    :note: Only checks once, caches the result."""
    global _versions_checked
    if _versions_checked:
        return
    _versions_checked = True
    init_subvertpy()

from bzrlib.plugins.svn import format

register_transport_proto('svn+ssh://',
    help="Access using the Subversion smart server tunneled over SSH.")
register_transport_proto('svn+http://')
register_transport_proto('svn+https://')
register_transport_proto('svn://',
    help="Access using the Subversion smart server.")
register_lazy_transport('svn://', 'bzrlib.plugins.svn.transport',
                        'SvnRaTransport')
register_lazy_transport('svn+', 'bzrlib.plugins.svn.transport',
                        'SvnRaTransport')
topic_registry.register_lazy('svn-layout',
                             'bzrlib.plugins.svn.layout',
                             'help_layout', 'Subversion repository layouts')
try:
    from bzrlib.controldir import (
        ControlDirFormat,
        format_registry,
        )
except ImportError:
    # Bzr < 2.3
    from bzrlib.bzrdir import (
        BzrDirFormat,
        format_registry,
        )
    #BzrDirFormat.register_control_server_format(format.SvnRemoteFormat)
    # Register as the first control server format, since the default smart
    # server implementation tries to do a POST request against .bzr/smart and
    # this causes some Subversion servers to reply with 401 Authentication required
    # even though they are accessible without authentication.
    BzrDirFormat._control_server_formats.insert(0, format.SvnRemoteFormat)
    BzrDirFormat.register_control_format(format.SvnWorkingTreeDirFormat)
else:
    #BzrDirFormat.register_control_server_format(format.SvnRemoteFormat)
    # Register as the first control server format, since the default smart
    # server implementation tries to do a POST request against .bzr/smart and
    # this causes some Subversion servers to reply with 401 Authentication required
    # even though they are accessible without authentication.
    ControlDirFormat.register_prober(format.SvnWorkingTreeProber)
    ControlDirFormat._server_probers.insert(0, format.SvnRemoteProber)
    ControlDirFormat.register_format(format.SvnWorkingTreeDirFormat())
    ControlDirFormat.register_format(format.SvnRemoteFormat())
try:
    from bzrlib.controldir import network_format_registry
except ImportError:
    from bzrlib.bzrdir import network_format_registry

network_format_registry.register_lazy("svn-wc",
    'bzrlib.plugins.svn.format', 'SvnWorkingTreeDirFormat')
network_format_registry.register_lazy("subversion",
    'bzrlib.plugins.svn.format', 'SvnRemoteFormat')
branch_network_format_registry.register_lazy("subversion",
    'bzrlib.plugins.svn.branch', 'SvnBranchFormat')
repository_network_format_registry.register_lazy("subversion",
    'bzrlib.plugins.svn.repository', 'SvnRepositoryFormat')
try:
    register_extra_lazy_repository_format = getattr(repository_format_registry,
        'register_extra_lazy')
except AttributeError:
    pass
else:
    register_extra_lazy_repository_format('bzrlib.plugins.svn.repository',
        'SvnRepositoryFormat')

format_registry.register_lazy("subversion", "bzrlib.plugins.svn.format",
                         "SvnRemoteFormat",
                         "Subversion repository. ",
                         native=False)
format_registry.register_lazy("subversion-wc", "bzrlib.plugins.svn.format",
                         "SvnWorkingTreeDirFormat",
                         "Subversion working copy. ",
                         native=False, hidden=True)
revspec_registry.register_lazy("svn:", "bzrlib.plugins.svn.revspec",
    "RevisionSpec_svn")

config.credential_store_registry.register_lazy(
    "subversion", "bzrlib.plugins.svn.auth", "SubversionCredentialStore",
    help=__doc__, fallback=True)

foreign_vcs_registry.register_lazy("svn", "bzrlib.plugins.svn.mapping",
                                   "foreign_vcs_svn")

from bzrlib.transport import transport_server_registry
transport_server_registry.register_lazy('svn',
    'bzrlib.plugins.svn.server', 'serve_svn',
    "Subversion svn_ra protocol. (default port: 3690)")


_optimizers_registered = False
def lazy_register_optimizers():
    """Register optimizers for fetching between Subversion and Bazaar
    repositories.

    :note: Only registers on the first call."""
    global _optimizers_registered
    if _optimizers_registered:
        return
    from bzrlib.repository import InterRepository
    from bzrlib.plugins.svn import push, fetch
    _optimizers_registered = True
    InterRepository.register_optimiser(fetch.InterFromSvnRepository)
    InterRepository.register_optimiser(push.InterToSvnRepository)


plugin_cmds.register_lazy('cmd_svn_import', [], 'bzrlib.plugins.svn.commands')
plugin_cmds.register_lazy('cmd_svn_branching_scheme', [],
                          'bzrlib.plugins.svn.mapping3.commands')
plugin_cmds.register_lazy('cmd_svn_layout', [],
                          'bzrlib.plugins.svn.commands')

lazy_register_filter_stack_map("svn-keywords",
        "bzrlib.plugins.svn.keywords", "create_svn_keywords_filter")


def info_svn_repository(repository, stats, outf):
    if "svn-uuid" in stats:
        outf.write("Subversion UUID: %s\n" % stats["svn-uuid"])
    if "svn-last-revnum" in stats:
        outf.write("Subversion Last Revision: %d\n" % stats["svn-last-revnum"])


from bzrlib.info import hooks as info_hooks
info_hooks.install_named_hook('repository', info_svn_repository, None)


def extract_svn_foreign_revid(rev):
    try:
        foreign_revid = rev.foreign_revid
    except AttributeError:
        from bzrlib.plugins.svn.mapping import mapping_registry
        foreign_revid, mapping = \
            mapping_registry.parse_revision_id(rev.revision_id)
        return foreign_revid
    else:
        from bzrlib.plugins.svn.mapping import foreign_vcs_svn 
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


RioVersionInfoBuilder.hooks.install_named_hook('revision',
    update_stanza, None)


from bzrlib.send import format_registry as send_format_registry

send_format_registry.register_lazy('svn', 'bzrlib.plugins.svn.send',
                                   'send_svn', 'Subversion diff format')

try:
    from bzrlib.diff import format_registry as diff_format_registry
except ImportError:
    pass # Not available prior to bzr 2.2b2
else:
    diff_format_registry.register_lazy('svn', 'bzrlib.plugins.svn.send',
            'SvnDiffTree', 'Subversion diff format')


def test_suite():
    """Returns the testsuite for bzr-svn."""
    from unittest import TestSuite
    from bzrlib.plugins.svn import tests
    suite = TestSuite()
    suite.addTest(tests.test_suite())
    return suite


if __name__ == '__main__':
    print ("This is a Bazaar plugin. Copy this directory to ~/.bazaar/plugins "
          "to use it.\n")
elif __name__ != 'bzrlib.plugins.svn':
    raise ImportError('The Subversion plugin must be installed as'
                      ' bzrlib.plugins.svn not %s' % __name__)
