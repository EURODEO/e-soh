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

static std::map<int, TableB> tb;
static std::map<int, TableC> tc;
static std::map<int, TableD> td;

static Oscar oscar;

static std::string bufr_input_schema;

static std::list<std::string> esoh_bufr_log;

static std::string default_shadow_wigos_py("0-578-2024-");

bool norbufr_init_bufrtables(std::string tables_dir);
bool norbufr_update_bufrtables(std::string tables_dir);
std::list<std::string> norbufr_bufresohmsg(std::string fname);
std::list<std::string> norbufr_bufresohmsgmem(char *buf, int size);
std::list<std::string> norbufr_log();
void norbufr_log_clear();

bool norbufr_init_oscar(std::string oscardb_dir);
bool norbufr_init_schema_template(std::string schema_path);
void norbufr_set_default_wigos(std::string s);

#endif
