# Copyright (C) 2006-2009 Jelmer Vernooij <jelmer@samba.org>

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
"""Random utility objects."""

class lazy_dict(object):

    def __init__(self, initial, create_fn, *args):
        self.initial = initial
        self.create_fn = create_fn
        self.args = args
        self.dict = None
        self.is_loaded = False

    def _ensure_init(self):
        if self.dict is None:
            self.dict = self.create_fn(*self.args)
            self.create_fn = None
            self.is_loaded = True

    def setdefault(self, key, default):
        try:
            return self[key]
        except KeyError:
            self[key] = default
            return self[key]

    def __len__(self):
        self._ensure_init()
        return len(self.dict)

    def __getitem__(self, key):
        if key in self.initial:
            return self.initial.__getitem__(key)
        self._ensure_init()
        return self.dict.__getitem__(key)

    def __delitem__(self, key):
        self._ensure_init()
        return self.dict.__delitem__(key)

    def __setitem__(self, key, value):
        self._ensure_init()
        return self.dict.__setitem__(key, value)

    def __contains__(self, key):
        if key in self.initial:
            return True
        self._ensure_init()
        return self.dict.__contains__(key)

    def get(self, key, default=None):
        if key in self.initial:
            return self.initial[key]
        self._ensure_init()
        return self.dict.get(key, default)

    def has_key(self, key):
        if self.initial.has_key(key):
            return True
        self._ensure_init()
        return self.dict.has_key(key)

    def keys(self):
        self._ensure_init()
        return self.dict.keys()

    def values(self):
        self._ensure_init()
        return self.dict.values()

    def items(self):
        self._ensure_init()
        return self.dict.items()

    def iteritems(self):
        self._ensure_init()
        return self.dict.iteritems()

    def __iter__(self):
        self._ensure_init()
        return self.dict.__iter__()

    def __str__(self):
        self._ensure_init()
        return str(self.dict)

    def __repr__(self):
        self._ensure_init()
        return repr(self.dict)

    def __eq__(self, other):
        self._ensure_init()
        return self.dict.__eq__(other)

    def __ne__(self, other):
        self._ensure_init()
        return self.dict.__ne__(other)

    def update(self, other):
        self._ensure_init()
        return self.dict.update(other)


class lazy_readonly_list(object):

    def __init__(self, iterator):
        self._iterator = iterator
        self._list = []

    def _next(self):
        ret = self._iterator.next()
        self._list.append(ret)
        return ret

    def __iter__(self):
        class Iterator(object):

            def __init__(self, list, next):
                self._list = list
                self._next = next
                self._idx = 0

            def next(self):
                if len(self._list) > self._idx:
                    ret = self._list[self._idx]
                    self._idx += 1
                    return ret
                self._idx += 1
                return self._next()

        return Iterator(self._list, self._next)


class ListBuildingIterator(object):
    """Simple iterator that iterates over a list, and calling an iterator
    once all items in the list have been iterated.

    The list may be updated while the iterator is running.
    """

    def __init__(self, base_list, it):
        self.base_list = base_list
        self.i = -1
        self.it = it

    def next(self):
        """Return the next item."""
        self.i+=1
        try:
            return self.base_list[self.i]
        except IndexError:
            return self.it()



