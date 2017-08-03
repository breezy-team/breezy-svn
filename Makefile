# Simple Makefile for Bazaar plugin
# Copyright (C) 2008 Jelmer Vernooij <jelmer@samba.org>
#   
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#   
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#   
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

DEBUGGER ?= 
BRZ ?= $(shell which brz)
BRZ_OPTIONS ?= -Derror
PYTHON ?= $(shell which python)
SETUP ?= $(PYTHON) ./setup.py
PYDOCTOR ?= pydoctor
CTAGS ?= ctags
PYLINT ?= pylint
RST2HTML ?= $(if $(shell which rst2html.py 2>/dev/null), rst2html.py, rst2html)
TESTS ?= "^breezy.plugins.svn." Subversion Svn
DESTDIR ?=

REST_DOCS = README FAQ AUTHORS

all:: build build-inplace $(patsubst %,%.html,$(REST_DOCS))

build::
	$(SETUP) build

build-inplace::
	$(SETUP) build_ext --inplace

build-inplace-debug::
	$(SETUP) build_ext --inplace --debug

install::
ifneq ($(DESTDIR),)
	$(SETUP) install --root "$(DESTDIR)"
else
	$(SETUP) install
endif

clean::
	$(SETUP) clean
	rm -f *.so

check:: build-inplace
	BRZ_PLUGINS_AT=svn@$(shell pwd) $(DEBUGGER) $(PYTHON) $(PYTHON_OPTIONS) $(BRZ) $(BRZ_OPTIONS) selftest $(TEST_OPTIONS) $(TESTS)

coverage::
	$(MAKE) check BRZ_OPTIONS="--coverage coverage"

check-verbose::
	$(MAKE) check TEST_OPTIONS=-v

check-one::
	$(MAKE) check TEST_OPTIONS=--one

check-random::
	$(MAKE) check TEST_OPTIONS="--random=now --verbose --one"

valgrind-check:: build-inplace-debug
	$(MAKE) check DEBUGGER="valgrind --suppressions=/usr/lib/valgrind/python.supp $(VALGRIND_OPTIONS)"

leak-check:: 
	$(MAKE) valgrind-check VALGRIND_OPTIONS="--leak-check=full --show-reachable=yes --num-callers=200 --leak-resolution=med --log-file=leaks.log"

gdb-check:: build-inplace-debug
	$(MAKE) check DEBUGGER="gdb --args $(GDB_OPTIONS)"

strace-check::
	$(MAKE) check DEBUGGER="strace $(STRACE_OPTIONS)"

show-plugins::
	BRZ_PLUGINS_AT=svn@$(shell pwd) $(BRZ) plugins -v

lint::
	PYTHONPATH=$(PYTHONPATH):subvertpy $(PYLINT) -f parseable *.py */*.py

pydoctor::
	$(PYDOCTOR) --make-html -c brz-svn.cfg

FAQ.html README.html AUTHORS.html: %.html: %
	$(RST2HTML) $< > $@

tags::
	$(CTAGS) -R .

ctags:: tags

homepage.html: wiki.py README INSTALL
	python wiki.py | tail -n +2 | rst2html > $@

.PHONY: update-pot po/brz-svn.pot
update-pot: po/brz-svn.pot

TRANSLATABLE_PYFILES:=$(shell find . -name '*.py' \
		| grep -v 'tests/' \
		)

po/brz-svn.pot: $(PYFILES) $(DOCFILES)
	BRZ_PLUGINS_AT=svn@$(shell pwd) brz export-pot \
          --plugin=svn > po/brz-svn.pot
	echo $(TRANSLATABLE_PYFILES) | xargs \
	  xgettext --package-name "brz-svn" \
	  --msgid-bugs-address "<bazaar@lists.canonical.com>" \
	  --copyright-holder "Jelmer Vernooij <jelmer@samba.org>" \
	  --from-code ISO-8859-1 --sort-by-file --join --add-comments=i18n: \
	  -d brz-svn -p po -o brz-svn.pot
