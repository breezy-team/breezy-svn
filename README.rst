Breezy support for Subversion branches, working trees and repositories
======================================================================

.. contents::

Introduction
------------

brz-svn is a plugin that allows Breezy_ direct access to Subversion_ 
repositories. It allows most Breezy commands to work directly against Subversion 
repositories, as if you were using Breezy with a native Breezy repository.

.. _Breezy: http://bazaar-vcs.org/
.. _Subversion: http://subversion.tigris.org/

Documentation
-------------

brz-svn can be used through the regular Breezy user interface, see the 
`Breezy Documentation Overview`_ for documentation on that.

.. _Breezy Documentation Overview: Documentation

Some brz-svn specific issues are answered by the FAQ_.

.. _FAQ: http://samba.org/~jelmer/brz-svn/FAQ.html

See the `Breezy plugin guide`_ for a quick introduction of brz-svn itself.

.. _Breezy plugin guide: http://doc.bazaar.canonical.com/latest/en/user-guide/svn_plugin.html

Limitations
-----------

Unsupported Subversion File Properties
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some Subversion properties can currently not be represented in Breezy and are 
therefore ignored for the time being:

- 'svn:ignore'. There should be a `Repository.get_ignores(revid)` call in 
  Breezy rather than a magic '.bzrignore' file. 
  Spec at https://launchpad.net/products/bzr/+spec/new-ignore-rules

- 'svn:mime-type'

- 'svn:eol-style'. Requires eol support in Breezy.

- 'svn:keywords'. Requires keywords support in Breezy. Spec at 
  https://launchpad.net/products/bzr/+spec/bzr-keyword-expansion. 
  `#81463 <https://bugs.launchpad.net/bzr-svn/+bug/81463>`_

- 'svn:externals'. Externals should be mapped to Breezy 'by-reference' 
  nested branches and the other way around. This can't be implemented 
  until Breezys nested branch support lands.


Future Enhancements
-------------------

In the future, I also hope to support:

- use svn_ra_replay() when using servers that have Subversion 1.4. Saves a 
  couple of roundtrips when fetching history.

Some Subversion metadata can currently not be represented in Breezy 
and are therefore ignored for the time being:

Other features currently held back by Breezys feature set:

- `Tracking copies`_
 
.. _Tracking copies: https://launchpad.net/products/bzr/+spec/filecopies

- Showing SVN merges as merges in Breezy. This requires `tracking cherry-picking support`_ in Breezy

.. _tracking cherry-picking support: https://launchpad.net/products/bzr/+spec/bzr-cpick-data

Support
-------
Ask brz-svn related questions on the `Bazaar mailing list`_ or in the 
#bzr IRC channel on Freenode_.

.. _Breezy mailing list: http://lists.canonical.com/listinfo/bazaar/
.. _Freenode: http://www.freenode.net/

Bugs
----

Please file bug reports in Launchpad. The product URL for brz-svn is
https://launchpad.net/brz-svn/. 

..
	vim: ft=rest
