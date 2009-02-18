# Copyright (C) 2005-2009 Jelmer Vernooij <jelmer@samba.org>
 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

help_schemes = """Subversion Branching Schemes

Subversion is basically a versioned file system. It does not have 
any notion of branches and what is a branch in Subversion is therefor
up to the user. 

In order for Bazaar to access a Subversion repository it has to know 
what paths to consider branches. It does this by using so-called branching 
schemes. When you connect to a repository for the first time, Bazaar
will try to determine the branching scheme to use using some simple 
heuristics. It is always possible to change the branching scheme it should 
use later.

There are some conventions in use in Subversion for repository layouts. 
The most common one is probably the trunk/branches/tags 
layout, where the repository contains a "trunk" directory with the main 
development branch, other branches in a "branches" directory and tags as 
subdirectories of a "tags" directory. This branching scheme is named 
"trunk" in Bazaar.

Another option is simply having just one branch at the root of the repository. 
This scheme is called "none" by Bazaar.

The branching scheme bzr-svn should use for a repository can be set in the 
configuration file ~/.bazaar/subversion.conf.

Branching schemes are only used for version 3 of the Bzr<->Svn mappings.
"""



