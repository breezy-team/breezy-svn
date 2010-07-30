#!/usr/bin/env python
# Setup file for bzr-svn

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

from info import *

if __name__ == '__main__':
    from distutils.core import setup
    version = bzr_plugin_version[:3]
    version_string = ".".join([str(x) for x in version])
    setup(name='bzr-svn',
          description='Support for Subversion branches in Bazaar',
          keywords='plugin bzr svn',
          version=version_string,
          url='http://bazaar-vcs.org/BzrForeignBranches/Subversion',
          download_url='http://samba.org/~jelmer/bzr/bzr-svn-%s.tar.gz' % version_string,
          license='GPL',
          author='Jelmer Vernooij',
          author_email='jelmer@samba.org',
          long_description="""
          This plugin adds support for branching off and
          committing to Subversion repositories from
          Bazaar.
          """,
          package_dir={'bzrlib.plugins.svn':'.' },
          packages=['bzrlib.plugins.svn',
                    'bzrlib.plugins.svn.cache',
                    'bzrlib.plugins.svn.layout',
                    'bzrlib.plugins.svn.mapping3',
                    'bzrlib.plugins.svn.tests',
                    'bzrlib.plugins.svn.tests.mapping3',
                    'bzrlib.plugins.svn.tests.mapping_implementations'],
          )
