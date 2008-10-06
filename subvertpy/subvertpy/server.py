# Copyright (C) 2006 Jelmer Vernooij <jelmer@samba.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

import copy
import os
import time

from subvertpy import ERR_RA_SVN_UNKNOWN_CMD, NODE_DIR, NODE_FILE, NODE_UNKNOWN, NODE_NONE, ERR_UNSUPPORTED_FEATURE
from subvertpy.marshall import marshall, unmarshall, literal, MarshallError


class ServerBackend:

    def open_repository(self, location):
        raise NotImplementedError(self.open_repository)


class ServerRepositoryBackend:
    
    def get_uuid(self):
        raise NotImplementedError(self.get_uuid)

    def get_latest_revnum(self):
        raise NotImplementedError(self.get_latest_revnum)

    def log(self, send_revision, target_path, start_rev, end_rev, changed_paths,
            strict_node, limit):
        raise NotImplementedError(self.log)

    def update(self, editor, revnum, target_path, recurse=True):
        raise NotImplementedError(self.update)

    def check_path(self, path, revnum):
        raise NotImplementedError(self.check_path)

    def stat(self, path, revnum):
        raise NotImplementedError(self.stat)

    def rev_proplist(self, revnum):
        raise NotImplementedError(self.rev_proplist)

    def get_locations(self, path, peg_revnum, revnums):
        raise NotImplementedError(self.get_locations)


MAJOR_VERSION = 1
MINOR_VERSION = 2
CAPABILITIES = ["edit-pipeline"]
MECHANISMS = ["ANONYMOUS"]


class SVNServer:
    def __init__(self, backend, recv_fn, send_fn, logf=None):
        self.backend = backend
        self.recv_fn = recv_fn
        self.send_fn = send_fn
        self.inbuffer = ""
        self._stop = False
        self._logf = logf

    def send_greeting(self):
        self.send_success(
            MAJOR_VERSION, MINOR_VERSION, [literal(x) for x in MECHANISMS], 
            [literal(x) for x in CAPABILITIES])

    def send_mechs(self):
        self.send_success([literal(x) for x in MECHANISMS], "")

    def send_failure(self, *contents):
        self.send_msg([literal("failure"), list(contents)])

    def send_success(self, *contents):
        self.send_msg([literal("success"), list(contents)])

    def send_ack(self):
        self.send_success([], "")

    def send_unknown(self, cmd):
        self.send_failure([ERR_RA_SVN_UNKNOWN_CMD, 
            "Unknown command '%s'" % cmd, __file__, 52])

    def get_latest_rev(self):
        self.send_ack()
        self.send_success(self.repo_backend.get_latest_revnum())

    def check_path(self, path, rev):
        if len(rev) == 0:
            revnum = None
        else:
            revnum = rev[0]
        kind = self.repo_backend.check_path(path, revnum)
        self.send_ack()
        self.send_success(literal({NODE_NONE: "none", 
                           NODE_DIR: "dir",
                           NODE_FILE: "file",
                           NODE_UNKNOWN: "unknown"}[kind]))

    def log(self, target_path, start_rev, end_rev, changed_paths, 
            strict_node, limit=None, include_merged_revisions=False, 
            all_revprops=None, revprops=None):
        def send_revision(revno, author, date, message, changed_paths=None):
            changes = []
            if changed_paths is not None:
                for p, (action, cf, cr) in changed_paths.items():
                    if cf is not None:
                        changes.append((p, literal(action), (cf, cr)))
                    else:
                        changes.append((p, literal(action), ()))
            self.send_msg([changes, revno, [author], [date], [message]])
        self.send_ack()
        if len(start_rev) == 0:
            start_revnum = None
        else:
            start_revnum = start_rev[0]
        if len(end_rev) == 0:
            end_revnum = None
        else:
            end_revnum = end_rev[0]
        self.repo_backend.log(send_revision, target_path, start_revnum, 
                              end_revnum, changed_paths, strict_node, limit)
        self.send_msg(literal("done"))
        self.send_success()

    def open_backend(self, url):
        import urllib
        (rooturl, location) = urllib.splithost(url)
        self.repo_backend, self.relpath = self.backend.open_repository(location)

    def reparent(self, parent):
        self.open_backend(parent)
        self.send_ack()
        self.send_success()

    def stat(self, path, rev):
        if len(rev) == 0:
            revnum = None
        else:
            revnum = rev[0]
        self.send_ack()
        dirent = self.repo_backend.stat(path, revnum)
        if dirent is None:
            self.send_success([])
        else:
            self.send_success([dirent["name"], dirent["kind"], dirent["size"],
                          dirent["has-props"], dirent["created-rev"],
                          dirent["created-date"], dirent["last-author"]])

    def commit(self, logmsg, locks, keep_locks=False, rev_props=None):
        self.send_failure([ERR_UNSUPPORTED_FEATURE, 
            "commit not yet supported", __file__, 42])

    def rev_proplist(self, revnum):
        self.send_ack()
        revprops = self.repo_backend.rev_proplist(revnum)
        self.send_success(revprops.items())

    def rev_prop(self, revnum, name):
        self.send_ack()
        revprops = self.repo_backend.rev_proplist(revnum)
        if name in revprops:
            self.send_success([revprops[name]])
        else:
            self.send_success()

    def get_locations(self, path, peg_revnum, revnums):
        self.send_ack()
        locations = self.repo_backend.get_locations(path, peg_revnum, revnums)
        for rev, path in locations.items():
            self.send_msg([rev, path])
        self.send_msg(literal("done"))
        self.send_success()

    def update(self, rev, target, recurse, depth=None, send_copyfrom_param=True):
        self.send_ack()
        while True:
            msg = self.recv_msg()
            assert msg[0] in ["set-path", "finish-report"]
            if msg[0] == "finish-report":
                break

        self.send_ack()

        class Editor:

            def __init__(self, conn):
                self.conn = conn

            def set_target_revision(self, rev):
                self.conn.send_msg(["target-rev", rev])

            def open_root(self, base_revision=None):
                id = generate_random_id()
                self.send_msg(["open-root", [base_revision, tree.inventory.root.file_id]])
                return DirectoryEditor(self.conn, id)

            def close(self):
                self.conn.send_msg(["close-edit", []])

        class DirectoryEditor:

            def __init__(self, conn, id):
                self.conn = conn
                self.id = id

            def add_file(self, path):
                child = generate_random_id()
                self.conn.send_msg(["add-file", [path, self.id, child]])
                return FileEditor(self.conn, child)

            def add_directory(self, path):
                child = generate_random_id()
                self.conn.send_msg(["add-dir", [path, self.id, child]])
                return DirectoryEditor(self.conn, child)

            def close(self):
                self.conn.send_msg(["close-dir", [self.id]])

        class FileEditor:

            def __init__(self, conn, id):
                self.conn = conn
                self.id = id

            def close(self):
                self.conn.send_msg(["close-file", [self.id]])

        if len(rev) == 0:
            revnum = None
        else:
            revnum = rev[0]
        self.repo_backend.update(Editor(self), revnum, target, recurse)
        self.send_success()

    commands = {
            "get-latest-rev": get_latest_rev,
            "log": log,
            "update": update,
            "check-path": check_path,
            "reparent": reparent,
            "stat": stat,
            "commit": commit,
            "rev-proplist": rev_proplist,
            "rev-prop": rev_prop,
            "get-locations": get_locations,
            # FIXME: get-dated-rev
            # FIXME: get-file
            # FIXME: get-dir
            # FIXME: check-path
            # FIXME: switch
            # FIXME: status
            # FIXME: diff
            # FIXME: get-file-revs
            # FIXME: replay
    }

    def send_auth_request(self):
        pass

    def serve(self):
        self.send_greeting()
        (version, capabilities, url) = self.recv_msg()
        self.capabilities = capabilities
        self.version = version
        self.url = url
        self.mutter("client supports:")
        self.mutter("  version %r" % version)
        self.mutter("  capabilities %r " % capabilities)
        self.send_mechs()

        (mech, args) = self.recv_msg()
        # TODO: Proper authentication
        self.send_success()

        self.open_backend(url)
        self.send_success(self.repo_backend.get_uuid(), url)

        # Expect:
        while not self._stop:
            ( cmd, args ) = self.recv_msg()
            if not self.commands.has_key(cmd):
                self.mutter("client used unknown command %r" % cmd)
                self.send_unknown(cmd)
                return
            else:
                self.commands[cmd](self, *args)

    def close(self):
        self._stop = True

    def recv_msg(self):
        # FIXME: Blocking read?
        while True:
            try:
                self.inbuffer += self.recv_fn(512)
                (self.inbuffer, ret) = unmarshall(self.inbuffer)
                self.mutter("IN: %r" % ret)
                return ret
            except MarshallError, e:
                self.mutter('ERROR: %r' % e)
                raise

    def send_msg(self, data):
        self.mutter("OUT: %r" % data)
        self.send_fn(marshall(data))

    def mutter(self, text):
        if self._logf is not None:
            self._logf.write("%s\n" % text)
