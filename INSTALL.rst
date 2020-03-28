Installation
------------

Requirements
~~~~~~~~~~~~

Breezy
^^^^^^

You will need a recent version of Breezy, usually the release of Breezy
released in the same month as the release of brz-svn you are using. brz-svn
will warn if the Breezy release used is too old or too new.

subvertpy
^^^^^^^^^

You need the subvertpy Python module. This may be packaged for your
distribution, or otherwise available from http://launchpad.net/subvertpy.

Building
~~~~~~~~

Simply place this directory in ~/.config/breezy/plugins and you should be able
to check out branches from Subversion using brz. Make sure the directory
name is 'svn'.

..
	vim: ft=rest
