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
    readme = open("README.rst", "r").read()
    version = bzr_plugin_version[:3]
    version_string = ".".join([str(x) for x in version])

    command_classes = {}
    try:
        from breezy.bzr_distutils import build_mo
    except ImportError:
        pass
    else:
        command_classes['build_mo'] = build_mo

    setup(name='bzr-svn',
          description='Support for Subversion branches in Bazaar',
          keywords='plugin bzr svn',
          version=version_string,
          url='http://samba.org/~jelmer/bzr-svn',
          download_url='http://samba.org/~jelmer/bzr/bzr-svn-%s.tar.gz' % version_string,
          license='GPL',
          author='Jelmer Vernooij',
          author_email='jelmer@samba.org',
          long_description=readme,
          package_dir={'breezy.plugins.svn':'.' },
          packages=['breezy.plugins.svn',
                    'breezy.plugins.svn.cache',
                    'breezy.plugins.svn.layout',
                    'breezy.plugins.svn.mapping3',
                    'breezy.plugins.svn.tests',
                    'breezy.plugins.svn.tests.layout',
                    'breezy.plugins.svn.tests.mapping3',
                    'breezy.plugins.svn.tests.mapping_implementations'],
          classifiers=[
              'Topic :: Software Development :: Version Control',
              'Environment :: Plugins',
              'Development Status :: 5 - Production/Stable',
              'License :: OSI Approved :: GNU General Public License (GPL)',
              'Natural Language :: English',
              'Operating System :: OS Independent',
              'Programming Language :: Python',
              'Programming Language :: Python :: 2',
          ],
          cmdclass=command_classes
          )
