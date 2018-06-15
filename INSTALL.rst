Installation
------------

Requirements
~~~~~~~~~~~~

Bazaar
^^^^^^

You will need a recent version of Bazaar, usually the release of Bazaar 
released in the same month as the release of bzr-svn you are using. bzr-svn 
will warn if the Bazaar release used is too old or too new.

subvertpy
^^^^^^^^^

You need the subvertpy Python module. This may be packaged for your 
distribution, or otherwise available from http://launchpad.net/subvertpy.

Building
~~~~~~~~

Simply place this directory in ~/.bazaar/plugins and you should be able 
to check out branches from Subversion using bzr. Make sure the directory 
name is 'svn'. 

Packages
~~~~~~~~

Instead of installing the plugin yourself, you can also install a prepackaged 
version of it for your platform.

Windows Setup
^^^^^^^^^^^^^

The standard `Windows installers for Bazaar`_ include bzr-svn.

.. _Windows installers for Bazaar: https://launchpad.net/bzr/+download


Debian/Ubuntu GNU/Linux
^^^^^^^^^^^^^^^^^^^^^^^

Debian sid or experimental have packages for the latest release (packages 
are uploaded to Debian as part of the release process). 

Ubuntu's in-development release usually also has the latest version, synced from 
Debian.

GoboLinux
^^^^^^^^^

GoboLinux includes `a recipe <http://recipes.gobolinux.org/r/?list=BZR-SVN>`_ for bzr-svn.

OpenSuse Linux
^^^^^^^^^^^^^^

OpenSuse packages created by Michael Wolf are available from http://download.opensuse.org/repositories/home:/maw:/bzr/

Gentoo Linux
^^^^^^^^^^^^

An unofficial Gentoo overlay containing a bzr-svn ebuild are hosted on launchpad at https://launchpad.net/bzr-gentoo-overlay/.

Mac OS X
^^^^^^^^

Download the latest Subversion DMG from: http://svnbinaries.open.collab.net/servlets/ProjectDocumentList

bzr-svn DMGs can be found on the bzr-svn download page on Launchpad.

bzr-svn is now also available in MacPorts, and can be installed with::

  $ sudo port install bzr-svn

..
	vim: ft=rest
