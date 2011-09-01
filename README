Bazaar support for Subversion branches, working trees and repositories
======================================================================

.. contents::

Introduction
------------

bzr-svn is a plugin that allows Bazaar_ direct access to Subversion_ 
repositories. It allows most bzr commands to work directly against Subversion 
repositories, as if you were using bzr with a native bzr repository.

.. _Bazaar: http://bazaar-vcs.org/
.. _Subversion: http://subversion.tigris.org/

Documentation
-------------

bzr-svn can be used through the regular Bazaar user interface, see the 
`Bazaar Documentation Overview`_ for documentation on that.

.. _Bazaar Documentation Overview: Documentation

Some bzr-svn specific issues are answered by the FAQ_.

.. _FAQ: http://samba.org/~jelmer/bzr-svn/FAQ.html

See the `bzr plugin guide`_ for a quick introduction of bzr-svn itself.

.. _bzr plugin guide: http://doc.bazaar.canonical.com/latest/en/user-guide/svn_plugin.html

Limitations
-----------

Unsupported Subversion File Properties
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some Subversion properties can currently not be represented in Bazaar and are 
therefore ignored for the time being:

- 'svn:ignore'. There should be a `Repository.get_ignores(revid)` call in 
  Bazaar rather than a magic '.bzrignore' file. 
  Spec at https://launchpad.net/products/bzr/+spec/new-ignore-rules

- 'svn:mime-type'

- 'svn:eol-style'. Requires eol support in Bazaar.

- 'svn:keywords'. Requires keywords support in Bazaar. Spec at 
  https://launchpad.net/products/bzr/+spec/bzr-keyword-expansion. 
  `#81463 <https://bugs.launchpad.net/bzr-svn/+bug/81463>`_

- 'svn:externals'. Externals should be mapped to Bazaar 'by-reference' 
  nested branches and the other way around. This can't be implemented 
  until Bazaars nested branch support lands.


Future Enhancements
-------------------

In the future, I also hope to support:

- use svn_ra_replay() when using servers that have Subversion 1.4. Saves a 
  couple of roundtrips when fetching history.

Some Subversion metadata can currently not be represented in Bazaar 
and are therefore ignored for the time being:

Other features currently held back by Bazaars feature set:

- `Tracking copies`_
 
.. _Tracking copies: https://launchpad.net/products/bzr/+spec/filecopies

- Showing SVN merges as merges in Bazaar. This requires `tracking cherry-picking support`_ in Bazaar

.. _tracking cherry-picking support: https://launchpad.net/products/bzr/+spec/bzr-cpick-data

Support
-------
Ask bzr-svn related questions on the `Bazaar mailing list`_ or in the 
#bzr IRC channel on Freenode_.

.. _Bazaar mailing list: http://lists.canonical.com/listinfo/bazaar/
.. _Freenode: http://www.freenode.net/

Bugs
----

Please file bug reports in Launchpad. The product URL for bzr-svn is
https://launchpad.net/bzr-svn/. 

..
	vim: ft=rest
