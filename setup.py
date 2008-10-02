#!/usr/bin/env python
# Setup file for bzr-svn
# Copyright (C) 2005-2008 Jelmer Vernooij <jelmer@samba.org>

from distutils.core import setup
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "subvertpy"))
from setup import subvertpy_modules, install_lib_with_dlls

setup(name='bzr-svn',
      description='Support for Subversion branches in Bazaar',
      keywords='plugin bzr svn',
      version='0.5.0',
      url='http://bazaar-vcs.org/BzrForeignBranches/Subversion',
      download_url='http://bazaar-vcs.org/BzrSvn',
      license='GPL',
      author='Jelmer Vernooij',
      author_email='jelmer@samba.org',
      long_description="""
      This plugin adds support for branching off and 
      committing to Subversion repositories from 
      Bazaar.
      """,
      package_dir={'bzrlib.plugins.svn':'.',
                   'bzrlib.plugins.svn.subvertpy':'subvertpy/subvertpy'},
      packages=['bzrlib.plugins.svn', 
                'bzrlib.plugins.svn.mapping3', 
                'bzrlib.plugins.svn.subvertpy', 
                'bzrlib.plugins.svn.subvertpy.tests', 
                'bzrlib.plugins.svn.tests'],
      ext_modules=subvertpy_modules("bzrlib.plugins.svn.subvertpy"),
      cmdclass = { 'install_lib': install_lib_with_dlls },
      )
