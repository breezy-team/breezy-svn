# Copyright (C) 2007-2009 Jelmer Vernooij <jelmer@samba.org>

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


"""Stores per-repository settings."""


import os
from subvertpy import (
    properties,
    )

from bzrlib import (
    osutils,
    trace,
    )
from bzrlib.config import (
    ConfigObj,
    IniBasedConfig,
    config_dir,
    ensure_config_dir_exists,
    GlobalConfig,
    LocationConfig,
    Config,
    STORE_BRANCH,
    STORE_GLOBAL,
    STORE_LOCATION,
    )
from bzrlib.errors import (
    BzrError,
    )


def as_bool(str):
    """Parse a string as a boolean.

    FIXME: This must be duplicated somewhere.
    """
    if str is None:
        return None
    _bools = { 'yes': True, 'no': False,
               'on': True, 'off': False,
               '1': True, '0': False,
               'true': True, 'false': False }
    if isinstance(str, list):
        raise ValueError
    try:
        return _bools[str.lower()]
    except KeyError:
        raise ValueError


# Settings are stored by UUID.
# Data stored includes default branching scheme and locations the repository
# was seen at.

def subversion_config_filename():
    """Return per-user configuration ini file filename."""
    return osutils.pathjoin(config_dir(), 'subversion.conf')


class SubversionUUIDConfig(IniBasedConfig):
    """UUID-based Subversion configuration."""

    def __init__(self, uuid):
        name_generator = subversion_config_filename
        super(SubversionUUIDConfig, self).__init__(name_generator)
        self.uuid = uuid
        if not self.uuid in self._get_parser():
            self._get_parser()[self.uuid] = {}

    def __getitem__(self, name):
        return self._get_parser()[self.uuid][name]

    def __setitem__(self, name, value):
        self._get_parser()[self.uuid][name] = value

    def _get_user_option(self, option_name):
        try:
            return self[option_name]
        except KeyError:
            return None

    def get_bool(self, name):
        return self._get_parser().get_bool(self.uuid, name)

    def set_user_option(self, name, value):
        """Change a user option.

        :param name: Name of the option.
        :param value: Value of the option.
        """
        conf_dir = os.path.dirname(self._get_filename())
        ensure_config_dir_exists(conf_dir)
        self[name] = value
        f = open(self._get_filename(), 'wb')
        self._get_parser().write(f)
        f.close()


class SvnRepositoryConfig(Config):
    """Per-repository settings."""

    def __init__(self, url, uuid):
        super(SvnRepositoryConfig, self).__init__()
        self.url = url
        self.uuid = uuid
        self._uuid_config = None
        self._location_config = None
        self._global_config = None
        self.option_sources = (self._get_location_config,
                               self._get_uuid_config,
                               self._get_global_config)

    def _get_location_config(self):
        if self._location_config is None:
            self._location_config = LocationConfig(self.url)
        return self._location_config

    def _get_global_config(self):
        if self._global_config is None:
            self._global_config = GlobalConfig()
        return self._global_config

    def _get_uuid_config(self):
        if self._uuid_config is None:
            self._uuid_config = SubversionUUIDConfig(self.uuid)
        return self._uuid_config

    def set_branching_scheme(self, scheme, guessed_scheme, mandatory=False):
        """Change the branching scheme.

        :param scheme: New branching scheme.
        :param guessed_scheme: Guessed scheme.
        """
        self.set_user_option('branching-scheme', str(scheme))
        if (guessed_scheme != scheme or
            self.get_user_option('branching-scheme-guess') is not None):
            self.set_user_option('branching-scheme-guess',
                                 guessed_scheme or scheme)
        if (mandatory or
            self.get_user_option('branching-scheme-mandatory') is not None):
            self.set_user_option('branching-scheme-mandatory', str(mandatory))

    def _get_user_option(self, option_name, use_global=True):
        """See Config._get_user_option."""
        for source in self.option_sources:
            if not use_global and source == self._get_global_config:
                continue
            value = source()._get_user_option(option_name)
            if value is not None:
                return value
        return None

    def get_bool(self, option_name, use_global=True):
        """See Config.get_bool."""
        ret = self._get_user_option(option_name, use_global)
        if ret is None:
            raise KeyError
        return as_bool(ret)

    def set_user_option(self, name, value):
        self._get_uuid_config().set_user_option(name, value)

    def get_layout(self):
        return self._get_user_option("layout", use_global=False)

    def get_guessed_layout(self):
        return self._get_user_option("guessed-layout", use_global=False)

    def set_layout(self, layout):
        return self.set_user_option("layout", str(layout))

    def set_guessed_layout(self, layout):
        return self.set_user_option("guessed-layout", str(layout))

    def get_branches(self):
        branches_str = self._get_user_option("branches", use_global=False)
        if branches_str is None:
            return None
        return [b.encode("utf-8") for b in branches_str.split(";") if b != ""]

    def get_tags(self):
        tags_str = self._get_user_option("tags", use_global=False)
        if tags_str is None:
            return None
        return [t.encode("utf-8") for t in tags_str.split(";") if t != ""]

    def set_tags(self, tags):
        self.set_user_option("tags", "".join(["%s;" % t.decode("utf-8") for t in tags]))

    def set_branches(self, branches):
        self.set_user_option("branches", "".join(["%s;" % b.decode("utf-8") for b in branches]))

    def get_reuse_revisions(self):
        ret = self._get_user_option("reuse-revisions")
        if ret is None:
            return "other-branches"
        assert ret in ("none", "other-branches", "removed-branches")
        return ret

    def get_branching_scheme(self):
        """Get the branching scheme.

        :return: BranchingScheme instance.
        """
        from bzrlib.plugins.svn.mapping3.scheme import BranchingScheme
        schemename = self._get_user_option("branching-scheme", use_global=False)
        if schemename is not None:
            return BranchingScheme.find_scheme(schemename.encode('ascii'))
        return None

    def get_default_mapping(self):
        """Get the default mapping.

        :return Mapping name.
        """
        return self._get_user_option("default-mapping", use_global=True)

    def get_guessed_branching_scheme(self):
        """Get the guessed branching scheme.

        :return: BranchingScheme instance.
        """
        from bzrlib.plugins.svn.mapping3.scheme import BranchingScheme
        schemename = self._get_user_option("branching-scheme-guess",
                                           use_global=False)
        if schemename is not None:
            return BranchingScheme.find_scheme(schemename.encode('ascii'))
        return None

    def get_supports_change_revprop(self):
        """Check whether or not the repository supports changing existing
        revision properties."""
        try:
            return self.get_bool("supports-change-revprop")
        except KeyError:
            return None

    def get_use_cache(self):
        try:
            if self.get_bool("use-cache"):
                return set(["log", "fileids", "revids", "revinfo"])
            return set()
        except ValueError:
            val = self._get_user_option("use-cache")
            if not isinstance(val, list):
                ret = set([val])
            else:
                ret = set(val)
            if len(ret - set(["log", "fileids", "revids", "revinfo"])) != 0:
                raise BzrError("Invalid setting 'use-cache': %r" % val)
            return ret
        except KeyError:
            return None

    def branching_scheme_is_mandatory(self):
        """Check whether or not the branching scheme for this repository
        is mandatory.
        """
        try:
            return self.get_bool("branching-scheme-mandatory")
        except KeyError:
            return False

    def get_locations(self):
        """Find the locations this repository has been seen at.

        :return: Set with URLs.
        """
        val = self._get_user_option("locations", use_global=False)
        if val is None:
            return set()
        return set(val.split(";"))

    def add_location(self, location):
        """Add a location for this repository.

        :param location: URL of location to add.
        """
        locations = self.get_locations()
        locations.add(location.rstrip("/"))
        self.set_user_option('locations', ";".join(list(locations)))



class BranchConfig(Config):
    """Subversion branch configuration."""

    def __init__(self, url, uuid):
        super(BranchConfig, self).__init__()
        self._location_config = None
        self._uuid_config = None
        self._global_config = None
        self.url = url
        self.uuid = uuid
        self.option_sources = (self._get_location_config,
                               self._get_uuid_config,
                               self._get_global_config)

    def _get_location_config(self):
        if self._location_config is None:
            self._location_config = LocationConfig(self.url)
        return self._location_config

    def _get_uuid_config(self):
        if self._uuid_config is None:
            self._uuid_config = SubversionUUIDConfig(self.uuid)
        return self._uuid_config

    def _get_global_config(self):
        if self._global_config is None:
            self._global_config = GlobalConfig()
        return self._global_config

    def get_log_strip_trailing_newline(self):
        """Check whether or not trailing newlines should be stripped in the
        Subversion log message (where support by the bzr<->svn mapping used)."""
        try:
            return self.get_bool("log-strip-trailing-newline")
        except KeyError:
            return False

    def get_bool(self, option_name, use_global=True):
        """See Config.get_bool."""
        ret = self._get_user_option(option_name, use_global)
        if ret is None:
            raise KeyError
        return as_bool(ret)

    def get_override_svn_revprops(self):
        """Check whether or not bzr-svn should attempt to override Subversion revision
        properties after committing."""
        try:
            if self.get_bool("override-svn-revprops"):
                return [properties.PROP_REVISION_DATE, properties.PROP_REVISION_AUTHOR]
            return []
        except ValueError:
            val = self._get_user_option("override-svn-revprops")
            if not isinstance(val, list):
                return [val]
            return val
        except KeyError:
            return None

    def _get_user_option(self, option_name, use_global=True):
        """See Config._get_user_option."""
        for source in self.option_sources:
            if not use_global and source == self._get_global_config:
                continue
            value = source()._get_user_option(option_name)
            if value is not None:
                return value
        return None

    def get_append_revisions_only(self, default=None):
        """Check whether it is possible to remove revisions from the mainline.
        """
        try:
            return self.get_bool("append_revisions_only")
        except KeyError:
            return default

    def _get_user_id(self):
        """Get the user id from the 'email' key in the current section."""
        return self._get_user_option('email')

    def set_user_option(self, name, value, store=STORE_LOCATION,
        warn_masked=False):
        if store == STORE_GLOBAL:
            self._get_global_config().set_user_option(name, value)
        elif store == STORE_BRANCH:
            raise NotImplementedError("Saving in branch config not supported for Subversion branches")
        else:
            self._get_location_config().set_user_option(name, value, store)
        if not warn_masked:
            return
        if store in (STORE_GLOBAL, STORE_BRANCH):
            mask_value = self._get_location_config().get_user_option(name)
            if mask_value is not None:
                trace.warning('Value "%s" is masked by "%s" from'
                              ' locations.conf', value, mask_value)
            else:
                if store == STORE_GLOBAL:
                    branch_config = self._get_branch_data_config()
                    mask_value = branch_config.get_user_option(name)
                    if mask_value is not None:
                        trace.warning('Value "%s" is masked by "%s" from'
                                      ' branch.conf', value, mask_value)

    def get_push_merged_revisions(self):
        """Check whether merged revisions should be pushed."""
        try:
            return self.get_bool("push_merged_revisions")
        except KeyError:
            return None


class PropertyConfig(object):
    """ConfigObj-like class that looks at Subversion file ids."""

    def __init__(self, tree, path, prefix=""):
        self.properties = tree.get_file_properties(tree.path2id(path), path)
        self.prefix = prefix

    def __getitem__(self, option_name):
        return self.properties[self.prefix+option_name]

    def __setitem__(self, option_name, value):
        raise NotImplementedError(self.set_user_option) # Should be setting the property..

    def __contains__(self, option_name):
        return (self.prefix+option_name) in self.properties

    def get(self, option_name, default=None):
        try:
            return self[option_name]
        except KeyError:
            return default


class NoSubversionBuildPackageConfig(Exception):
    """Raised when no svn-buildpackage configuration can be found."""


class SubversionBuildPackageConfig(object):
    """Configuration object that behaves similar to svn-buildpackage when it reads its config."""

    def __init__(self, tree):
        from bzrlib.plugins.svn.workingtree import SvnWorkingTree
        from bzrlib.plugins.svn.tree import SubversionTree
        if (isinstance(tree, SvnWorkingTree) and
            os.path.exists(os.path.join(tree.abspath("."), ".svn", "svn-layout"))):
            self.wt_layout_path = os.path.join(tree.abspath("."), ".svn", "svn-layout")
            self.option_source = ConfigObj(self.wt_layout_path, encoding="utf-8")
        elif tree.has_filename("debian/svn-layout"):
            self.option_source = ConfigObj(tree.get_file(tree.path2id("debian/svn-layout"), "debian/svn-layout"), encoding="utf-8")
        elif isinstance(tree, SubversionTree) and tree.has_filename("debian"):
            self.option_source = PropertyConfig(tree, "debian", "svn-bp:")
        else:
            raise NoSubversionBuildPackageConfig()
        self.tree = tree

    def get_merge_with_upstream(self):
        props = self.tree.get_file_properties(self.tree.path2id("debian"), "debian")
        return "mergeWithUpstream" in props

    def __getitem__(self, option_name):
        if self.option_source is None:
            raise KeyError
        return self.option_source[option_name]

    def __contains__(self, option_name):
        if self.option_source is None:
            return False
        return (option_name in self.option_source)

    def get(self, option_name, default=None):
        try:
            return self[option_name]
        except KeyError:
            return default

    def __setitem__(self, option_name, value):
        self.option_source[option_name] = value
