#!/usr/bin/env python
# Setup file for bzr-svn
# Copyright (C) 2005-2009 Jelmer Vernooij <jelmer@samba.org>

from distutils.core import setup
import os, sys

setup(name='bzr-svn',
      description='Support for Subversion branches in Bazaar',
      keywords='plugin bzr svn',
      version='0.5.4',
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
      package_dir={'bzrlib.plugins.svn':'.' },
      packages=['bzrlib.plugins.svn', 
                'bzrlib.plugins.svn.cache', 
                'bzrlib.plugins.svn.foreign', 
                'bzrlib.plugins.svn.layout', 
                'bzrlib.plugins.svn.mapping3', 
                'bzrlib.plugins.svn.tests',
                'bzrlib.plugins.svn.tests.mapping3',
                'bzrlib.plugins.svn.tests.mapping_implementations'],
      )
