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

Most Bazaar commands should work fine with Subversion branches. To 
create new branches in Subversion using push, it is currently necessary
to use the svn-push command rather than the standard push command.

bzr-svn also adds four new commands to Bazaar:

 - bzr svn-import
 - bzr svn-serve
 - bzr svn-layout

For more information about bzr-svn, see the bzr-svn FAQ.

"""
import bzrlib
from bzrlib import config
import bzrlib.api
from bzrlib.bzrdir import (
    BzrDirFormat,
    format_registry,
    )
from bzrlib.commands import (
    plugin_cmds,
    )
from bzrlib.errors import (
    DependencyNotPresent,
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
from bzrlib.revisionspec import (
    revspec_registry,
    )
from bzrlib.trace import (
    mutter,
    )
from bzrlib.transport import (
    register_lazy_transport,
    register_transport_proto,
    )
from bzrlib.version_info_formats.format_rio import (
    RioVersionInfoBuilder,
    )

# versions ending in 'exp' mean experimental mappings
# versions ending in 'dev' mean development version
# versions ending in 'final' mean release (well tested, etc)
version_info = (0, 5, 4, 'dev', 0)

if version_info[3] == 'final':
    version_string = '%d.%d.%d' % version_info[:3]
else:
    version_string = '%d.%d.%d%s%d' % version_info
__version__ = version_string

COMPATIBLE_BZR_VERSIONS = [(1, 14, 0)]
MINIMUM_SUBVERTPY_VERSION = (0, 6, 1)


def check_subversion_version():
    """Check that Subversion is compatible.

    """
    # Installed ?
    from subvertpy import ra, __version__ as subvertpy_version
    ra_version = ra.version()
    if (ra_version[0] >= 5 and getattr(ra, 'SVN_REVISION', None) and 
        27729 <= ra.SVN_REVISION < 31470):
        raise DependencyNotPresent("subvertpy",
                'bzr-svn: Installed Subversion has buggy svn.ra.get_log() '
                'implementation, please install newer.')

    mutter("bzr-svn: using Subversion %d.%d.%d (%s)" % ra_version)

    if subvertpy_version < MINIMUM_SUBVERTPY_VERSION:
        raise DependencyNotPresent("subvertpy", "bzr-svn: at least subvertpy %d.%d.%d is required, %d.%d.%d is installed." % (MINIMUM_SUBVERTPY_VERSION + subvertpy_version))


def get_client_string():
    """Return a string that can be send as part of the User Agent string."""
    return "bzr%s+bzr-svn%s" % (bzrlib.__version__, __version__)


def init_subvertpy():
    try:
        import subvertpy 
    except ImportError, e:
        raise DependencyNotPresent("subvertpy", "bzr-svn: %s" % e.message)

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
    bzrlib.api.require_any_api(bzrlib, COMPATIBLE_BZR_VERSIONS)

from bzrlib.plugins.svn import format

register_transport_proto('svn+ssh://', 
    help="Access using the Subversion smart server tunneled over SSH.")
register_transport_proto('svn+file://', 
    help="Access of local Subversion repositories.")
register_transport_proto('svn+http://',
    help="Access of Subversion smart servers over HTTP.")
register_transport_proto('svn+https://',
    help="Access of Subversion smart servers over secure HTTP.")
register_transport_proto('svn://', 
    help="Access using the Subversion smart server.")
register_lazy_transport('svn://', 'bzrlib.plugins.svn.transport', 
                        'SvnRaTransport')
register_lazy_transport('svn+', 'bzrlib.plugins.svn.transport', 
                        'SvnRaTransport')
topic_registry.register_lazy('svn-branching-schemes', 
                             'bzrlib.plugins.svn.mapping3',
                             'help_schemes', 'Subversion branching schemes')
topic_registry.register_lazy('svn-layout', 
                             'bzrlib.plugins.svn.layout',
                             'help_layout', 'Subversion repository layouts')
BzrDirFormat.register_control_format(format.SvnRemoteFormat)
BzrDirFormat.register_control_format(format.SvnWorkingTreeDirFormat)
format_registry.register_lazy("subversion", "bzrlib.plugins.svn.format", "SvnRemoteFormat", 
                         "Subversion repository. ", 
                         native=False)
format_registry.register_lazy("subversion-wc", "bzrlib.plugins.svn.format", "SvnWorkingTreeDirFormat", 
                         "Subversion working copy. ", 
                         native=False, hidden=True)
revspec_registry.register_lazy("svn:", "bzrlib.plugins.svn.revspec", 
    "RevisionSpec_svn")

config.credential_store_registry.register_lazy(
    "subversion", "bzrlib.plugins.svn.auth", "SubversionCredentialStore", 
    help=__doc__)

foreign_vcs_registry.register_lazy("svn", "bzrlib.plugins.svn.mapping",
                                   "foreign_vcs_svn")

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
plugin_cmds.register_lazy('cmd_foreign_mapping_upgrade', ['svn-upgrade'], 
                          'bzrlib.plugins.svn.foreign')
plugin_cmds.register_lazy('cmd_svn_branching_scheme', [], 
                          'bzrlib.plugins.svn.mapping3.commands')
plugin_cmds.register_lazy('cmd_svn_set_revprops', [], 
                          'bzrlib.plugins.svn.commands')
plugin_cmds.register_lazy('cmd_svn_layout', [], 
                          'bzrlib.plugins.svn.commands')
plugin_cmds.register_lazy('cmd_svn_serve', [], 
                          'bzrlib.plugins.svn.commands')

lazy_register_filter_stack_map("svn-keywords", 
        "bzrlib.plugins.svn.keywords", "create_svn_keywords_filter")

def update_stanza(rev, stanza):
    revmeta = getattr(rev, "svn_meta", None)
    if revmeta is None:
        return
    stanza.add("svn-revno", str(revmeta.revnum))
    stanza.add("svn-uuid", revmeta.uuid)


try:
    RioVersionInfoBuilder.hooks.install_named_hook('revision',
        update_stanza, None)
except AttributeError:
    pass

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
