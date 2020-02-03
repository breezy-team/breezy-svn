# Copyright (C) 2007-2010 Jelmer Vernooij <jelmer@samba.org>

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

from __future__ import absolute_import

import os
from subvertpy import (
    properties,
    )

from breezy import (
    atomicfile,
    config as _mod_bzr_config,
    osutils,
    trace,
    transport,
    )
from breezy.bedding import (
    config_dir,
    ensure_config_dir_exists,
    )
from breezy.config import (
    ConfigObj,
    GlobalConfig,
    LocationConfig,
    Config,
    STORE_BRANCH,
    STORE_GLOBAL,
    STORE_LOCATION,
    )
from breezy.errors import (
    BzrError,
    )


def as_bool(str):
    """Parse a string as a boolean.

    FIXME: This must be duplicated somewhere.
    """
    if str is None:
        return None
    _bools = {
        'yes': True, 'no': False,
        'on': True, 'off': False,
        '1': True, '0': False,
        'true': True, 'false': False,
        }
    if isinstance(str, list):
        raise ValueError
    try:
        return _bools[str.lower()]
    except KeyError:
        raise ValueError


# Settings are stored by UUID.
# Data stored includes default branching scheme and locations the repository
# was seen at.


class SubversionStore(_mod_bzr_config.LockableIniFileStore):

    def __init__(self, possible_transports=None):
        t = transport.get_transport_from_path(
            config_dir(),
            possible_transports=possible_transports)
        super(SubversionStore, self).__init__(t, 'subversion.conf')
        self.id = 'subversion'


class UUIDMatcher(_mod_bzr_config.SectionMatcher):
    """UUID-based Subversion section matcher."""

    def __init__(self, store, uuid):
        super(UUIDMatcher, self).__init__(store)
        self.uuid = uuid

    def match(self, section):
        return section.id == self.uuid


class SvnBranchStack(_mod_bzr_config._CompatibleStack):
    """SvnBranch stack providing UUID specific options."""

    def __init__(self, branch_base, repository_uuid):
        lstore = _mod_bzr_config.LocationStore()
        loc_matcher = _mod_bzr_config.LocationMatcher(lstore, branch_base)
        svn_store = SubversionStore()
        uuid_matcher = UUIDMatcher(svn_store, repository_uuid)
        gstore = _mod_bzr_config.GlobalStore()
        super(SvnBranchStack, self).__init__(
            [self._get_overrides,
             loc_matcher.get_sections,
             uuid_matcher.get_sections,
             gstore.get_sections],
            # All modifications go to the corresponding section in
            # locations.conf
            lstore, branch_base)


class SvnRepositoryStack(_mod_bzr_config._CompatibleStack):
    """SvnRepository stack providing UUID specific options."""

    def __init__(self, repository):
        lstore = _mod_bzr_config.LocationStore()
        loc_matcher = _mod_bzr_config.LocationMatcher(lstore, repository.base)
        svn_store = SubversionStore()
        uuid_matcher = UUIDMatcher(svn_store,
                                   getattr(repository, 'uuid', None))
        gstore = _mod_bzr_config.GlobalStore()
        super(SvnRepositoryStack, self).__init__(
            [self._get_overrides,
             loc_matcher.get_sections,
             uuid_matcher.get_sections,
             gstore.get_sections],
            # All modifications go to the corresponding section in
            # locations.conf
            lstore, repository.base)
        self.repository = repository


class SubversionUUIDConfig(_mod_bzr_config.LockableConfig):
    """UUID-based Subversion configuration."""

    def __init__(self, uuid):
        super(SubversionUUIDConfig, self).__init__(self._get_filename())
        self.uuid = uuid
        if self.uuid not in self._get_parser():
            self._get_parser()[self.uuid] = {}

    def _get_filename(self):
        return osutils.pathjoin(config_dir(), 'subversion.conf')

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
        file_name = self._get_filename()
        ensure_config_dir_exists(os.path.dirname(file_name))
        atomic_file = atomicfile.AtomicFile(file_name)
        self[name] = value
        self._get_parser().write(atomic_file)
        atomic_file.commit()
        atomic_file.close()


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
        if (mandatory or self.get_user_option(
                'branching-scheme-mandatory') is not None):
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
        from .mapping3.scheme import BranchingScheme
        schemename = self._get_user_option(
            "branching-scheme", use_global=False)
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
        from .mapping3.scheme import BranchingScheme
        schemename = self._get_user_option("branching-scheme-guess",
                                           use_global=False)
        if schemename is not None:
            return BranchingScheme.find_scheme(schemename.encode('ascii'))
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

    def get_default_stack_on(self):
        return None

    def set_default_stack_on(self, value):
        raise BzrError("Cannot set configuration")


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

    def get_bool(self, option_name, use_global=True):
        """See Config.get_bool."""
        ret = self._get_user_option(option_name, use_global)
        if ret is None:
            raise KeyError
        return as_bool(ret)

    def _get_user_option(self, option_name, use_global=True):
        """See Config._get_user_option."""
        for source in self.option_sources:
            if not use_global and source == self._get_global_config:
                continue
            value = source()._get_user_option(option_name)
            if value is not None:
                return value
        return None

    def _get_user_id(self):
        """Get the user id from the 'email' key in the current section."""
        return self._get_user_option('email')

    def _get_change_editor(self):
        return self.get_user_option('change_editor')

    def set_user_option(self, name, value, store=STORE_LOCATION,
                        warn_masked=False):
        if store == STORE_GLOBAL:
            self._get_global_config().set_user_option(name, value)
        elif store == STORE_BRANCH:
            raise NotImplementedError(
                "Saving in branch config not supported for Subversion "
                "branches")
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

    def has_explicit_nickname(self):
        return False


class PropertyConfig(object):
    """ConfigObj-like class that looks at Subversion file ids."""

    def __init__(self, tree, path, prefix=""):
        self.properties = tree.get_file_properties(path)
        self.prefix = prefix

    def __getitem__(self, option_name):
        return self.properties[self.prefix+option_name]

    def __setitem__(self, option_name, value):
        # Should be setting the property..
        raise NotImplementedError(self.set_user_option)

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
    """Configuration that behaves similar to svn-buildpackage."""

    def __init__(self, tree):
        from .workingtree import SvnWorkingTree
        from .tree import SubversionTree
        if (isinstance(tree, SvnWorkingTree) and
                os.path.exists(os.path.join(
                    tree.abspath("."), ".svn", "svn-layout"))):
            self.wt_layout_path = os.path.join(
                    tree.abspath("."), ".svn", "svn-layout")
            self.option_source = ConfigObj(
                self.wt_layout_path, encoding="utf-8")
        elif tree.has_filename("debian/svn-layout"):
            layout_file = tree.get_file("debian/svn-layout")
            self.option_source = ConfigObj(layout_file, encoding="utf-8")
        elif isinstance(tree, SubversionTree) and tree.has_filename("debian"):
            self.option_source = PropertyConfig(tree, "debian", "svn-bp:")
        else:
            raise NoSubversionBuildPackageConfig()
        self.tree = tree

    def get_merge_with_upstream(self):
        props = self.tree.get_file_properties("debian")
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


svn_layout_option = _mod_bzr_config.Option(
    'layout', default=None,
    help='''\
The mandated bzr-svn repository layout to use.

See `bzr help svn-layout` for details.
''')

svn_guessed_layout_option = _mod_bzr_config.Option(
    'guessed-layout',
    default=None,
    help='''\
The bzr-svn repository layout that bzr-svn guessed.

See `bzr help svn-layout` for details.
''')


def svn_paths_from_store(text):
    return [b.encode("utf-8") for b in text.split(";") if b != ""]


svn_branches_option = _mod_bzr_config.Option(
    'branches', default=None,
    from_unicode=svn_paths_from_store,
    help='''\
Paths in the Subversion repository to consider branches.

This should be a comma-separated list of paths, each relative to the
repository root. Asterisks may be used as wildcards, and will match
a single path element.

See `bzr help svn-layout` for more information on Subversion repository
layouts.
''')


svn_tags_option = _mod_bzr_config.Option(
    'tags', default=None,
    from_unicode=svn_paths_from_store,
    help='''\
Paths in the Subversion repository to consider tags.

This should be a comma-separated list of paths, each relative to the
repository root. Asterisks may be used as wildcards, and will match
a single path element.

See `bzr help svn-layout` for more information on Subversion repository
layouts.
''')


def override_svn_revprops_from_store(text):
    val = _mod_bzr_config.bool_from_store(text)
    if val is not None:
        if val:
            return [
                properties.PROP_REVISION_DATE, properties.PROP_REVISION_AUTHOR]
        else:
            return []
    else:
        # FIXME: Check for allowed values ?
        return text.split(",")


svn_override_revprops = _mod_bzr_config.Option(
    'override-svn-revprops', default=None,
    from_unicode=override_svn_revprops_from_store,
    help='''\
Which Subversion revision properties to override (comma-separated list).

This is done after a revision is pushed from bzr, and requires the
pre-revprop-change-hook to be enabled in the repository.

Possible values:

 * svn:author=committer - set the svn author to the committer of the bzr
                          revision
 * svn:author=author, svn:author - set the svn author to the first author of
                                   the bzr revision
 * svn:date - override the date to be the same as the commit timestamp of
              the bzr revision
''')

svn_log_strip_trailing_new_line = _mod_bzr_config.Option(
    'log-strip-trailing-newline',
    default=False, from_unicode=_mod_bzr_config.bool_from_store,
    help='''\
Whether trailing newlines should be stripped in the Subversion log message.

This only applies to revisions created by bzr-svn.
''')
svn_push_merged_revisions = _mod_bzr_config.Option(
    'push_merged_revisions',
    default=False, from_unicode=_mod_bzr_config.bool_from_store,
    help='''\
Whether to push merged revisions.
''')

svn_allow_metadata_in_fileprops = _mod_bzr_config.Option(
    'allow_metadata_in_file_properties',
    default=False, from_unicode=_mod_bzr_config.bool_from_store,
    help='''\
Whether to store bzr-svn-specific metadata in Subversion file properties.

This is usually a bad idea, and not necessary with newer versions of
Subversion, which support revision properties. This setting is present for
compatibility with older versions of bzr-svn.
''')
