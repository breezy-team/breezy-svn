#!/usr/bin/env python
# Setup file for bzr-svn
# Copyright (C) 2005-2009 Jelmer Vernooij <jelmer@samba.org>

bzr_plugin_name = "svn"

subvertpy_minimum_version = (0, 6, 1)

# versions ending in 'exp' mean experimental mappings
# versions ending in 'dev' mean development version
# versions ending in 'final' mean release (well tested, etc)
bzr_plugin_version = (0, 6, 4, 'dev', 0)

bzr_commands = ["svn-import", "svn-layout"]

bzr_compatible_versions = [(1, x, 0) for x in [15, 16, 17, 18]]

bzr_minimum_version = bzr_compatible_versions[0]

bzr_maximum_version = bzr_compatible_versions[-1]

bzr_control_formats = {"Subversion":{'.svn/': None}}

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
