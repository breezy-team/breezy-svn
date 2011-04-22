#!/usr/bin/python
# Copyright (C) 2011 Jelmer Vernooij <jelmer@samba.org>

# Trivial wrapper around subunit to filter out any tests
# that are known to be failing.

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

from subunit import (
    ProtocolTestCase,
    TestProtocolClient,
    )

from subunit.test_results import TestResultFilter
import sys

expectedfail = set()

f = open("expectedfail", "r")
try:
    expectedfail.update([l.strip("\n") for l in f.readlines()])
finally:
    f.close()


class ExpectedFailTestResultFilter(TestResultFilter):

    def __init__(self, result, filter_error=False, filter_failure=False,
            filter_success=True, filter_skip=False,
            filter_predicate=None, fixup_expected_failures=None):
        super(ExpectedFailTestResultFilter, self).__init__(
            result=result, filter_error=filter_error,
            filter_failure=filter_failure, filter_success=filter_success,
            filter_skip=filter_skip, filter_predicate=filter_predicate)
        if fixup_expected_failures is None:
            self._fixup_expected_failures = frozenset()
        else:
            self._fixup_expected_failures = fixup_expected_failures

    def addError(self, test, err=None, details=None):
        if (self.filter_predicate(test, 'error', err, details)):
            if test.id() in self._fixup_expected_failures:
                self._buffered_calls.append(
                    ('addExpectedFailure', [test, err], {'details': details}))
            else:
                self._buffered_calls.append(
                    ('addError', [test, err], {'details': details}))
        else:
            self._filtered()

    def addFailure(self, test, err=None, details=None):
        if (self.filter_predicate(test, 'failure', err, details)):
            if test.id() in self._fixup_expected_failures:
                self._buffered_calls.append(
                    ('addExpectedFailure', [test, err], {'details': details}))
            else:
                self._buffered_calls.append(
                    ('addFailure', [test, err], {'details': details}))
        else:
            self._filtered()

    def addSuccess(self, test, details=None):
        if (self.filter_predicate(test, 'success', None, details)):
            if test.id() in self._fixup_expected_failures:
                self._buffered_calls.append(
                    ('addUnexpectedSuccess', [test], {'details': details}))
            else:
                self._buffered_calls.append(
                    ('addSuccess', [test], {'details': details}))
        else:
            self._filtered()


result = TestProtocolClient(sys.stdout)
result = ExpectedFailTestResultFilter(result,
        fixup_expected_failures=expectedfail, filter_success=False)
test = ProtocolTestCase(sys.stdin, passthrough=None)
test.run(result)
sys.exit(0)
