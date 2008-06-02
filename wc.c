/*
 * Copyright © 2008 Jelmer Vernooij <jelmer@samba.org>
 * -*- coding: utf-8 -*-
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
#include <Python.h>
#include <apr_general.h>
#include <svn_wc.h>
#include <stdbool.h>

#include "util.h"

PyAPI_DATA(PyTypeObject) Entry_Type;
PyAPI_DATA(PyTypeObject) Adm_Type;


static svn_error_t *py_ra_report_set_path(void *baton, const char *path, long revision, int start_empty, const char *lock_token, apr_pool_t *pool)
{
    PyObject *self = (PyObject *)baton, *py_lock_token, *ret;
    if (lock_token == NULL) {
        py_lock_token = Py_None;
	} else {
        py_lock_token = PyString_FromString(lock_token);
	}
	ret = PyObject_CallFunction(self, "set_path", "slbO", path, revision, start_empty, py_lock_token);
	if (ret == NULL)
		return py_svn_error();
    return NULL;
}

static svn_error_t *py_ra_report_delete_path(void *baton, const char *path, apr_pool_t *pool)
{
    PyObject *self = (PyObject *)baton, *ret;
	ret = PyObject_CallFunction(self, "delete_path", "s", path);
	if (ret == NULL)
		return py_svn_error();
    return NULL;
}

static svn_error_t *py_ra_report_link_path(void *report_baton, const char *path, const char *url, long revision, int start_empty, const char *lock_token, apr_pool_t *pool)
{
    PyObject *self = (PyObject *)report_baton, *ret, *py_lock_token;
    if (lock_token == NULL) {
        py_lock_token = Py_None;
	} else { 
        py_lock_token = PyString_FromString(lock_token);
	}
	ret = PyObject_CallFunction(self, "link_path", "sslbO", path, url, revision, start_empty, py_lock_token);
	if (ret == NULL)
		return py_svn_error();
    return NULL;
}

static svn_error_t *py_ra_report_finish(void *baton, apr_pool_t *pool)
{
    PyObject *self = (PyObject *)baton, *ret;
	ret = PyObject_CallFunction(self, "finish", NULL);
	if (ret == NULL)
		return py_svn_error();
    return NULL;
}

static svn_error_t *py_ra_report_abort(void *baton, apr_pool_t *pool)
{
    PyObject *self = (PyObject *)baton, *ret;
	ret = PyObject_CallFunction(self, "abort", NULL);
	if (ret == NULL)
		return py_svn_error();
    return NULL;
}

svn_ra_reporter2_t py_ra_reporter = {
	.finish_report = py_ra_report_finish,
	.abort_report = py_ra_report_abort,
	.link_path = py_ra_report_link_path,
	.delete_path = py_ra_report_delete_path,
	.set_path = py_ra_report_set_path,
};



/**
 * Get libsvn_wc version information.
 *
 * :return: tuple with major, minor, patch version number and tag.
 */
static PyObject *version(PyObject *self)
{
    const svn_version_t *ver = svn_wc_version();
    return Py_BuildValue("(iiis)", ver->major, ver->minor, 
						 ver->patch, ver->tag);
}

static svn_error_t *py_wc_found_entry(const char *path, const svn_wc_entry_t *entry, void *walk_baton, apr_pool_t *pool)
{
    PyObject *fn = (PyObject *)walk_baton, *ret;
    /* FIXME: entry */
	ret = PyObject_CallFunction(fn, "s", path);
	if (ret == NULL)
		return py_svn_error();
    return NULL;
}

static svn_wc_entry_callbacks_t py_wc_entry_callbacks = {
	.found_entry = py_wc_found_entry
};

static void py_wc_notify_func(void *baton, const svn_wc_notify_t *notify, apr_pool_t *pool)
{
    /* FIXME */
}

typedef struct {
	PyObject_HEAD
	apr_pool_t *pool;
	svn_wc_entry_t *entry;
} EntryObject;

static void entry_dealloc(PyObject *self)
{
	apr_pool_destroy(((EntryObject *)self)->pool);
}

PyTypeObject Entry_Type = {
	PyObject_HEAD_INIT(&PyType_Type) 0,
	.tp_name = "wc.Entry",
	.tp_basicsize = sizeof(EntryObject),
	.tp_dealloc = entry_dealloc,
};

static PyObject *py_entry(const svn_wc_entry_t *entry)
{
	EntryObject *ret = PyObject_New(EntryObject, &Entry_Type);
	if (ret == NULL)
		return NULL;

	ret->pool = Pool(NULL);
	ret->entry = svn_wc_entry_dup(entry, ret->pool);
    return (PyObject *)ret;
}

typedef struct {
	PyObject_HEAD
    svn_wc_adm_access_t *adm;
    apr_pool_t *pool;
} AdmObject;

static PyObject *adm_init(PyTypeObject *self, PyObject *args, PyObject *kwargs)
{
	PyObject *associated;
	char *path;
	bool write_lock=false;
	int depth=0;
	PyObject *cancel_func=Py_None;
	svn_wc_adm_access_t *parent_wc;
	AdmObject *ret;

	if (!PyArg_ParseTuple(args, "Os|biO", &associated, &path, &write_lock, &depth, &cancel_func))
		return NULL;

	ret = PyObject_New(AdmObject, &Adm_Type);
	if (ret == NULL)
		return NULL;

	ret->pool = Pool(NULL);
	if (associated == Py_None) {
		parent_wc = NULL;
	} else {
		parent_wc = ((AdmObject *)associated)->adm;
	}
	if (!check_error(svn_wc_adm_open3(&ret->adm, parent_wc, path, 
                     write_lock, depth, py_cancel_func, cancel_func, 
                     ret->pool)))
		return NULL;

	return (PyObject *)ret;
}

static PyObject *adm_access_path(PyObject *self)
{
	AdmObject *admobj = (AdmObject *)self;
	return PyString_FromString(svn_wc_adm_access_path(admobj->adm));
}

static PyObject *adm_locked(PyObject *self)
{
	AdmObject *admobj = (AdmObject *)self;
	return PyBool_FromLong(svn_wc_adm_locked(admobj->adm));
}

static PyObject *adm_prop_get(PyObject *self, PyObject *args)
{
	char *name, *path;
	AdmObject *admobj = (AdmObject *)self;
	const svn_string_t *value;
	apr_pool_t *temp_pool;
	PyObject *ret;

	if (!PyArg_ParseTuple(args, "ss", &name, &path))
		return NULL;

	temp_pool = Pool(admobj->pool);
	RUN_SVN_WITH_POOL(temp_pool, svn_wc_prop_get(&value, name, path, admobj->adm, temp_pool));
	if (value == NULL) {
		ret = Py_None;
	} else {
		ret = PyString_FromStringAndSize(value->data, value->len);
	}
	apr_pool_destroy(temp_pool);
	return ret;
}

static PyObject *adm_prop_set(PyObject *self, PyObject *args)
{
	char *name, *value, *path; 
	AdmObject *admobj = (AdmObject *)self;
	bool skip_checks=false;
	apr_pool_t *temp_pool;
	int vallen;
	svn_string_t *cvalue;

	if (!PyArg_ParseTuple(args, "ss#s|b", &name, &value, &vallen, &path, &skip_checks))
		return NULL;

	temp_pool = Pool(admobj->pool);
	cvalue = svn_string_ncreate(value, vallen, temp_pool);
	RUN_SVN_WITH_POOL(temp_pool, svn_wc_prop_set2(name, cvalue, path, admobj->adm, 
				skip_checks, temp_pool));
	apr_pool_destroy(temp_pool);

	return Py_None;
}

static PyObject *adm_entries_read(PyObject *self, PyObject *args)
{
	apr_hash_t *entries;
	AdmObject *admobj = (AdmObject *)self;
	apr_pool_t *temp_pool;
	bool show_hidden=false;
	apr_hash_index_t *idx;
	const char *key;
	apr_ssize_t klen;
	svn_wc_entry_t *entry;
	PyObject *py_entries;

	if (!PyArg_ParseTuple(args, "|b", &show_hidden))
		return NULL;

	temp_pool = Pool(admobj->pool);
	RUN_SVN_WITH_POOL(temp_pool, svn_wc_entries_read(&entries, admobj->adm, 
				 show_hidden, temp_pool));
	py_entries = PyDict_New();
	idx = apr_hash_first(temp_pool, entries);
	while (idx != NULL) {
		apr_hash_this(idx, (const void **)&key, &klen, (void **)&entry);
		PyDict_SetItemString(py_entries, key, py_entry(entry));
		idx = apr_hash_next(idx);
	}
	apr_pool_destroy(temp_pool);
	return py_entries;
}

static PyObject *adm_walk_entries(PyObject *self, PyObject *args)
{
	char *path;
	PyObject *callbacks; 
	bool show_hidden=false;
	PyObject *cancel_func=Py_None;
	apr_pool_t *temp_pool;
	AdmObject *admobj = (AdmObject *)self;

	if (!PyArg_ParseTuple(args, "sO|bO", &path, &callbacks, &show_hidden, &cancel_func))
		return NULL;

	temp_pool = Pool(admobj->pool);
	RUN_SVN_WITH_POOL(temp_pool, svn_wc_walk_entries2(path, admobj->adm, 
				&py_wc_entry_callbacks, (void *)callbacks,
				show_hidden, py_cancel_func, (void *)cancel_func,
				temp_pool));
	apr_pool_destroy(temp_pool);

	return Py_None;
}

static PyObject *adm_entry(PyObject *self, PyObject *args)
{
	char *path;
	bool show_hidden=false;
	apr_pool_t *temp_pool;
	AdmObject *admobj = (AdmObject *)self;
	const svn_wc_entry_t *entry;

	if (!PyArg_ParseTuple(args, "s|b", &path, &show_hidden))
		return NULL;

	temp_pool = Pool(admobj->pool);
	RUN_SVN_WITH_POOL(temp_pool, svn_wc_entry(&entry, path, admobj->adm, show_hidden, temp_pool));
	apr_pool_destroy(temp_pool);

	return py_entry(entry);
}

static PyObject *adm_get_prop_diffs(PyObject *self, PyObject *args)
{
	char *path;
	apr_pool_t *temp_pool;
	apr_array_header_t *propchanges;
	apr_hash_t *original_props;
	AdmObject *admobj = (AdmObject *)self;
	apr_hash_index_t *idx;
	svn_string_t *string;
	const char *key;
	apr_ssize_t klen;
	svn_prop_t *el;
	int i;
	PyObject *py_propchanges, *py_orig_props;

	if (!PyArg_ParseTuple(args, "s", &path))
		return NULL;

	temp_pool = Pool(admobj->pool);
	RUN_SVN_WITH_POOL(temp_pool, svn_wc_get_prop_diffs(&propchanges, &original_props, 
				path, admobj->adm, temp_pool));
	py_propchanges = PyList_New(propchanges->nelts);
	for (i = 0; i < propchanges->nelts; i++) {
		el = (svn_prop_t *)(propchanges->elts + (i * propchanges->elt_size));
		PyList_SetItem(py_propchanges, i, 
					   Py_BuildValue("(ss#)", el->name, el->value->data, el->value->len));
	}
	py_orig_props = PyDict_New();
	idx = apr_hash_first(temp_pool, original_props);
	while (idx != NULL) {
		apr_hash_this(idx, (const void **)&key, &klen, (void **)&string);
		PyDict_SetItemString(py_orig_props, key, PyString_FromStringAndSize(string->data, string->len));
		idx = apr_hash_next(idx);
	}
	apr_pool_destroy(temp_pool);
	return Py_BuildValue("(OO)", py_propchanges, py_orig_props);
}

static PyObject *adm_add(PyObject *self, PyObject *args)
{
	char *path, *copyfrom_url=NULL;
	svn_revnum_t copyfrom_rev=-1; 
	PyObject *cancel_func=Py_None, *notify_func=Py_None;
	AdmObject *admobj = (AdmObject *)self;
	apr_pool_t *temp_pool;

	temp_pool = Pool(admobj->pool);

	if (!PyArg_ParseTuple(args, "s|zlOO", &path, &copyfrom_url, &copyfrom_rev, &cancel_func, &notify_func))
		return NULL;

	RUN_SVN_WITH_POOL(temp_pool, svn_wc_add2(path, admobj->adm, copyfrom_url, 
							copyfrom_rev, py_cancel_func, 
							(void *)cancel_func,
							py_wc_notify_func, 
							(void *)notify_func, 
							temp_pool));
	apr_pool_destroy(temp_pool);

	return Py_None;
}

static PyObject *adm_copy(PyObject *self, PyObject *args)
{
	AdmObject *admobj = (AdmObject *)self;
	char *src, *dst; 
	PyObject *cancel_func=Py_None, *notify_func=Py_None;
	apr_pool_t *temp_pool;

	if (!PyArg_ParseTuple(args, "ss|OO", &src, &dst, &cancel_func, &notify_func))
		return NULL;

	temp_pool = Pool(admobj->pool);
	RUN_SVN_WITH_POOL(temp_pool, svn_wc_copy2(src, admobj->adm, dst,
							py_cancel_func, (void *)cancel_func,
							py_wc_notify_func, (void *)notify_func, 
							temp_pool));
	apr_pool_destroy(temp_pool);

	return Py_None;
}

static PyObject *adm_delete(PyObject *self, PyObject *args)
{
	AdmObject *admobj = (AdmObject *)self;
	apr_pool_t *temp_pool;
	char *path;
	PyObject *cancel_func=Py_None, *notify_func=Py_None;

	if (!PyArg_ParseTuple(args, "s|OO", &path, &cancel_func, &notify_func))
		return NULL;

	temp_pool = Pool(admobj->pool);
	RUN_SVN_WITH_POOL(temp_pool, svn_wc_delete2(path, admobj->adm, 
							py_cancel_func, (void *)cancel_func,
							py_wc_notify_func, (void *)notify_func, 
							temp_pool));
	apr_pool_destroy(temp_pool);

	return Py_None;
}

static PyObject *adm_crawl_revisions(PyObject *self, PyObject *args)
{
	char *path;
	PyObject *reporter;
	bool restore_files=true, recurse=true, use_commit_times=true;
	PyObject *notify_func=Py_None;
	apr_pool_t *temp_pool;
	AdmObject *admobj = (AdmObject *)self;
	svn_wc_traversal_info_t *traversal_info;

	if (!PyArg_ParseTuple(args, "sO|bbbO", &path, &reporter, &restore_files, &recurse, &use_commit_times,
						  &notify_func))
		return NULL;

	temp_pool = Pool(admobj->pool);
	traversal_info = svn_wc_init_traversal_info(temp_pool);
	RUN_SVN_WITH_POOL(temp_pool, svn_wc_crawl_revisions2(path, admobj->adm, 
				&py_ra_reporter, (void *)reporter, 
				restore_files, recurse, use_commit_times, 
				py_wc_notify_func, (void *)notify_func,
				traversal_info, temp_pool));
	apr_pool_destroy(temp_pool);

	return Py_None;
}

static PyObject *adm_get_update_editor(PyObject *self, PyObject *args)
{
	char *target;
	bool use_commit_times=true, recurse=true;
	PyObject * notify_func=Py_None, *cancel_func=Py_None;
	char *diff3_cmd=NULL;
	const svn_delta_editor_t *editor;
	AdmObject *admobj = (AdmObject *)self;
	void *edit_baton;
	apr_pool_t *pool;
	svn_revnum_t *latest_revnum;

	if (!PyArg_ParseTuple(args, "s|bbOOz", &target, &use_commit_times, &recurse, &notify_func, &cancel_func, &diff3_cmd))
		return NULL;

	pool = Pool(NULL);
	latest_revnum = (svn_revnum_t *)apr_palloc(pool, sizeof(svn_revnum_t));
	if (!check_error(svn_wc_get_update_editor2(latest_revnum, admobj->adm, target, 
				use_commit_times, recurse, py_wc_notify_func, (void *)notify_func, 
				py_cancel_func, (void *)cancel_func, diff3_cmd, &editor, &edit_baton, 
				NULL, pool))) {
		apr_pool_destroy(pool);
		return NULL;
	}
	return new_editor_object(editor, edit_baton, pool, &Editor_Type);
}

static PyObject *adm_close(PyObject *self)
{
	AdmObject *admobj = (AdmObject *)self;
	if (admobj->adm != NULL) {
		svn_wc_adm_close(admobj->adm);
		admobj->adm = NULL;
	}

	return Py_None;
}

static void adm_dealloc(PyObject *self)
{
	apr_pool_destroy(((AdmObject *)self)->pool);
}

static PyMethodDef adm_methods[] = { 
	{ "prop_set", adm_prop_set, METH_VARARGS, NULL },
	{ "access_path", (PyCFunction)adm_access_path, METH_NOARGS, NULL },
	{ "prop_get", adm_prop_get, METH_VARARGS, NULL },
	{ "entries_read", adm_entries_read, METH_VARARGS, NULL },
	{ "walk_entries", adm_walk_entries, METH_VARARGS, NULL },
	{ "locked", (PyCFunction)adm_locked, METH_NOARGS, NULL },
	{ "get_prop_diffs", adm_get_prop_diffs, METH_VARARGS, NULL },
	{ "add", adm_add, METH_VARARGS, NULL },
	{ "copy", adm_copy, METH_VARARGS, NULL },
	{ "delete", adm_delete, METH_VARARGS, NULL },
	{ "crawl_revisions", adm_crawl_revisions, METH_VARARGS, NULL },
	{ "get_update_editor", adm_get_update_editor, METH_VARARGS, NULL },
	{ "close", (PyCFunction)adm_close, METH_NOARGS, NULL },
	{ "entry", (PyCFunction)adm_entry, METH_VARARGS, NULL },
	{ NULL }
};

PyTypeObject Adm_Type = {
	PyObject_HEAD_INIT(&PyType_Type) 0,
	.tp_name = "wc.Adm",
	.tp_basicsize = sizeof(AdmObject),
	.tp_new = adm_init,
	.tp_dealloc = adm_dealloc,
	.tp_methods = adm_methods,
};

/** 
 * Determine the revision status of a specified working copy.
 *
 * :return: Tuple with minimum and maximum revnums found, whether the 
 * working copy was switched and whether it was modified.
 */
static PyObject *revision_status(PyObject *self, PyObject *args, PyObject *kwargs)
{
	char *kwnames[] = { "wc_path", "trail_url", "committed", "cancel_func", NULL };
	char *wc_path, *trail_url=NULL;
	bool committed=false;
	PyObject *cancel_func=Py_None, *ret;
     svn_wc_revision_status_t *revstatus;
    apr_pool_t *temp_pool;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|zbO", kwnames, &wc_path, &trail_url, &committed, 
						  &cancel_func))
		return NULL;

    temp_pool = Pool(NULL);
    RUN_SVN_WITH_POOL(temp_pool, svn_wc_revision_status(&revstatus, wc_path, trail_url,
                 committed, py_cancel_func, cancel_func, temp_pool));
    ret = Py_BuildValue("(llbb)", revstatus->min_rev, revstatus->max_rev, 
            revstatus->switched, revstatus->modified);
    apr_pool_destroy(temp_pool);
    return ret;
}

static PyObject *is_normal_prop(PyObject *self, PyObject *args)
{
	char *name;

	if (!PyArg_ParseTuple(args, "s", &name))
		return NULL;

    return PyBool_FromLong(svn_wc_is_normal_prop(name));
}

static PyObject *is_wc_prop(PyObject *self, PyObject *args)
{
	char *name;

	if (!PyArg_ParseTuple(args, "s", &name))
		return NULL;

    return PyBool_FromLong(svn_wc_is_wc_prop(name));
}

static PyObject *is_entry_prop(PyObject *self, PyObject *args)
{
	char *name;

	if (!PyArg_ParseTuple(args, "s", &name))
		return NULL;

    return PyBool_FromLong(svn_wc_is_entry_prop(name));
}

static PyObject *get_adm_dir(PyObject *self)
{
    apr_pool_t *pool;
	PyObject *ret;
	const char *dir;
    pool = Pool(NULL);
    dir = svn_wc_get_adm_dir(pool);
	ret = PyString_FromString(dir);
    apr_pool_destroy(pool);
    return ret;
}

static PyObject *get_pristine_copy_path(PyObject *self, PyObject *args)
{
    apr_pool_t *pool;
    const char *pristine_path;
	char *path;
	PyObject *ret;

	if (!PyArg_ParseTuple(args, "s", &path))
		return NULL;

    pool = Pool(NULL);
	RUN_SVN_WITH_POOL(pool, svn_wc_get_pristine_copy_path(path, &pristine_path, pool));
	ret = PyString_FromString(pristine_path);
	apr_pool_destroy(pool);
    return ret;
}

static PyObject *get_default_ignores(PyObject *self, PyObject *args)
{
    apr_array_header_t *patterns;
    apr_pool_t *pool;
    char **pattern;
    apr_hash_t *hash_config;
	apr_ssize_t idx;
	int i = 0;
	PyObject *pyk, *pyv, *config;
	PyObject *ret;

	if (!PyArg_ParseTuple(args, "O", &config))
		return NULL;

    pool = Pool(NULL);
    hash_config = apr_hash_make(pool);
	while (PyDict_Next(config, &idx, &pyk, &pyv))
        apr_hash_set(hash_config, (char *)PyString_AsString(pyk), PyString_Size(pyk), (char *)PyString_AsString(pyv));
    RUN_SVN_WITH_POOL(pool, svn_wc_get_default_ignores(&patterns, hash_config, pool));
    ret = PyList_New(patterns->nelts);
    pattern = (char **)apr_array_pop(patterns);
    while (pattern != NULL) {
		PyList_SetItem(ret, i, PyString_FromString(*pattern));
		i++;
        pattern = (char **)apr_array_pop(patterns);
	}
    apr_pool_destroy(pool);
    return ret;
}

static PyObject *ensure_adm(PyObject *self, PyObject *args, PyObject *kwargs)
{
	char *path, *uuid, *url;
	char *repos=NULL; 
	svn_revnum_t rev=-1;
    apr_pool_t *pool;
	char *kwnames[] = { "path", "uuid", "url", "repos", "rev", NULL };

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "sss|sl", kwnames, 
									 &path, &uuid, &url, &repos, &rev))
		return NULL;

    pool = Pool(NULL);
    RUN_SVN_WITH_POOL(pool, 
					  svn_wc_ensure_adm2(path, uuid, url, repos, rev, pool));
    apr_pool_destroy(pool);
	return Py_None;
}

static PyObject *check_wc(PyObject *self, PyObject *args)
{
	char *path;
    apr_pool_t *pool;
    int wc_format;

	if (!PyArg_ParseTuple(args, "s", &path))
		return NULL;

    pool = Pool(NULL);
	RUN_SVN_WITH_POOL(pool, svn_wc_check_wc(path, &wc_format, pool));
    apr_pool_destroy(pool);
    return PyLong_FromLong(wc_format);
}

static PyMethodDef wc_methods[] = {
	{ "check_wc", check_wc, METH_VARARGS, NULL },
	{ "ensure_adm", (PyCFunction)ensure_adm, METH_KEYWORDS|METH_VARARGS, NULL },
	{ "get_default_ignores", get_default_ignores, METH_VARARGS, NULL },
	{ "get_adm_dir", (PyCFunction)get_adm_dir, METH_NOARGS, NULL },
	{ "get_pristine_copy_path", get_pristine_copy_path, METH_VARARGS, NULL },
	{ "is_normal_prop", is_normal_prop, METH_VARARGS, NULL },
	{ "is_entry_prop", is_entry_prop, METH_VARARGS, NULL },
	{ "is_wc_prop", is_wc_prop, METH_VARARGS, NULL },
	{ "revision_status", (PyCFunction)revision_status, METH_KEYWORDS|METH_VARARGS, NULL },
	{ "version", (PyCFunction)version, METH_NOARGS, NULL },
	{ NULL }
};

void initwc(void)
{
	PyObject *mod;

	if (PyType_Check(&Entry_Type) < 0)
		return;

	if (PyType_Check(&Adm_Type) < 0)
		return;

	apr_initialize();

	mod = Py_InitModule3("wc", wc_methods, "Working Copies");
	if (mod == NULL)
		return;

	PyModule_AddObject(mod, "SCHEDULE_NORMAL", PyLong_FromLong(0));
	PyModule_AddObject(mod, "SCHEDULE_ADD", PyLong_FromLong(1));
	PyModule_AddObject(mod, "SCHEDULE_DELETE", PyLong_FromLong(2));
	PyModule_AddObject(mod, "SCHEDULE_REPLACE", PyLong_FromLong(3));

	PyModule_AddObject(mod, "Adm", (PyObject *)&Adm_Type);
	Py_INCREF(&Adm_Type);
}
