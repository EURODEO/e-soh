/*
 * (C) Copyright 2023, Eumetnet
 *
 * This file is part of the E-SOH Norbufr BUFR en/decoder interface
 *
 * Author: istvans@met.no
 *
 */

#ifndef _BUFRESOHMSG_PY_H_
#define _BUFRESOHMSG_PY_H_

#include <list>
#include <string>

#include "Oscar.h"
#include "Tables.h"

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

static std::map<int, TableB *> tb;
static std::map<int, TableC *> tc;
static std::map<int, TableD *> td;

static Oscar oscar;

static std::string bufr_input_schema;

long init_bufrtables_py(std::string tables_dir);
std::list<std::string> bufresohmsg_py(std::string fname);
long destroy_bufrtables_py(std::string s);

bool norbufr_init_oscar(std::string oscardb_dir);
bool norbufr_init_schema_template(std::string schema_path);

#endif
