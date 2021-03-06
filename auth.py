# Copyright (C) 2005-2009 Jelmer Vernooij <jelmer@samba.org>

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


"""Authentication token retrieval."""

from __future__ import absolute_import

import subvertpy
from subvertpy import (
    ra,
    )

from breezy.config import (
    AuthenticationConfig,
    CredentialStore,
    )
from six import (
    text_type,
    )
from breezy.trace import (
    mutter,
    )
from breezy.ui import (
    ui_factory,
    )


class SubversionAuthenticationConfig(AuthenticationConfig):
    """Simple extended version of AuthenticationConfig that can provide
    the information Subversion requires.
    """
    def __init__(self, scheme, host, port, path, file=None):
        super(SubversionAuthenticationConfig, self).__init__(file)
        if scheme.startswith('svn+http'):
            self.scheme = scheme[len("svn+"):]
        else:
            self.scheme = scheme
        self.host = host
        self.port = port
        self.path = path

    def get_svn_username(self, realm, may_save):
        """Look up a Subversion user name in the Bazaar authentication cache.

        :param realm: Authentication realm (optional)
        :param may_save: Whether or not the username should be saved.
        """
        mutter("Obtaining username for SVN connection")
        username = self.get_user(self.scheme, host=self.host, path=self.path,
                                 realm=realm, ask=True)
        return (username.encode('utf-8'), False)

    def get_svn_simple(self, realm, username, may_save):
        """Look up a Subversion user name+password combination in the Bazaar
        authentication cache.

        :param realm: Authentication realm (optional)
        :param username: Username, if it is already known, or None.
        :param may_save: Whether or not the username should be saved.
        """
        mutter("Obtaining username and password for SVN connection %r"
               "(username: %r)", realm, username)
        if username is None:
            username = self.get_user(
                self.scheme, host=self.host, path=self.path, realm=realm,
                ask=True)
        password = self.get_password(
            self.scheme, host=self.host, path=self.path, user=username,
            realm=realm, prompt=u"%s %s password" % (realm, username))
        if isinstance(password, text_type):
            password = password.encode('utf-8')
        return (username.encode("utf-8"), password, False)

    def get_svn_ssl_server_trust(self, realm, failures, cert_info, may_save):
        """Return a Subversion auth provider that verifies SSL server trust.

        :param realm: Realm name (optional)
        :param failures: Failures to check for (bit field, SVN_AUTH_SSL_*)
        :param cert_info: Certificate information
        :param may_save: Whether this information may be stored.
        """
        mutter("Verifying SSL server: %s", realm)
        credentials = self.get_credentials(self.scheme, host=self.host)
        if (credentials is not None and
                "verify_certificates" in credentials and
                credentials["verify_certificates"] is False):
            accepted_failures = (
                    subvertpy.SSL_NOTYETVALID +
                    subvertpy.SSL_EXPIRED +
                    subvertpy.SSL_CNMISMATCH +
                    subvertpy.SSL_UNKNOWNCA +
                    subvertpy.SSL_OTHER)
        else:
            accepted_failures = 0
        return (accepted_failures, False)

    def get_svn_username_prompt_provider(self, retries):
        """Return a Subversion auth provider for retrieving the username, as
        accepted by svn_auth_open().

        :param retries: Number of allowed retries.
        """
        return ra.get_username_prompt_provider(
            self.get_svn_username, retries)

    def get_svn_simple_prompt_provider(self, retries):
        """Return a Subversion auth provider for retrieving a
        username+password combination, as accepted by svn_auth_open().

        :param retries: Number of allowed retries.
        """
        return ra.get_simple_prompt_provider(self.get_svn_simple, retries)

    def get_svn_ssl_server_trust_prompt_provider(self):
        """Return a Subversion auth provider for checking
        whether a SSL server is trusted."""
        return ra.get_ssl_server_trust_prompt_provider(
            self.get_svn_ssl_server_trust)

    def get_svn_auth_providers(self):
        """Return a list of auth providers for this authentication file.
        """
        return [self.get_svn_username_prompt_provider(1),
                self.get_svn_simple_prompt_provider(1),
                self.get_svn_ssl_server_trust_prompt_provider()]


def get_ssl_client_cert_pw(realm, may_save):
    """Simple SSL client certificate password prompter.

    :param realm: Realm, optional.
    :param may_save: Whether the password can be cached.
    """
    password = ui_factory.get_password(
            "Please enter password for client certificate[realm=%s]" % realm)
    return (password, False)


def get_ssl_client_cert_pw_provider(tries):
    return ra.get_ssl_client_cert_pw_prompt_provider(
                get_ssl_client_cert_pw, tries)


def get_stock_svn_providers():
    providers = [
        ra.get_simple_provider(),
        ra.get_username_provider(),
        ra.get_ssl_client_cert_file_provider(),
        ra.get_ssl_client_cert_pw_file_provider(),
        ra.get_ssl_server_trust_file_provider(),
        ]

    if getattr(ra, 'get_windows_simple_provider', None):
        providers.append(ra.get_windows_simple_provider())

    if getattr(ra, 'get_keychain_simple_provider', None):
        providers.append(ra.get_keychain_simple_provider())

    if getattr(ra, 'get_windows_ssl_server_trust_provider', None):
        providers.append(ra.get_windows_ssl_server_trust_provider())

    return providers


def create_auth_baton(url):
    """Create an authentication baton for the specified URL.

    :param url: URL to create auth baton for.
    """
    try:
        import urllib.parse as urlparse
    except ImportError:  # python < 3
        import urlparse
    parsed_url = urlparse.urlsplit(url)

    auth_config = SubversionAuthenticationConfig(
        parsed_url.scheme, parsed_url.hostname, parsed_url.port,
        parsed_url.path)

    # Specify Subversion providers first, because they use file data
    # rather than prompting the user.
    providers = []
    providers += get_stock_svn_providers()
    providers += auth_config.get_svn_auth_providers()
    providers += [get_ssl_client_cert_pw_provider(1)]

    auth_baton = ra.Auth(providers)
    if parsed_url.username is not None:
        auth_baton.set_parameter(
            subvertpy.AUTH_PARAM_DEFAULT_USERNAME, parsed_url.username)
    if parsed_url.password is not None:
        auth_baton.set_parameter(
            subvertpy.AUTH_PARAM_DEFAULT_PASSWORD, parsed_url.password)
    return auth_baton


class SubversionCredentialStore(CredentialStore):
    """Credentials provider that reads ~/.subversion/auth/"""

    def __init__(self):
        super(SubversionCredentialStore, self).__init__()
        self.auth = ra.Auth([ra.get_simple_provider()])

    def _get_realm(self, credentials):
        if credentials.get('port') is None:
            import socket
            try:
                credentials['port'] = socket.getservbyname(
                    credentials['scheme'])
            except socket.error:
                mutter("Unable to look up default port for %(scheme)s" %
                       credentials)
                return None
        return "<%(scheme)s://%(host)s:%(port)s> %(realm)s" % credentials

    def decode_password(self, credentials):
        svn_realm = self._get_realm(credentials)
        if svn_realm is None:
            return None
        creds = self.auth.credentials("svn.simple", svn_realm)
        try:
            (username, password, may_save) = next(creds)
        except StopIteration:
            return None
        assert type(password) == str
        if credentials.get("user") not in (username, None):
            # Subversion changed the username
            return None
        return password

    def get_credentials(self, scheme, host, port=None, user=None, path=None,
                        realm=None):
        assert isinstance(realm, str) or realm is None
        credentials = {
            "scheme": scheme,
            "host": host,
            "port": port,
            "realm": realm,
            "user": user,
            }
        svn_realm = self._get_realm(credentials)
        if svn_realm is None:
            return None
        creds = self.auth.credentials("svn.simple", svn_realm)
        try:
            (username, password, may_save) = next(creds)
        except StopIteration:
            return None
        credentials['user'] = username
        credentials['password'] = password
        return credentials
