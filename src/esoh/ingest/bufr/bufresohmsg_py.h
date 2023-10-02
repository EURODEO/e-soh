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

#include "Oscar.h"

static std::map<int,TableB *> *tb = new std::map<int,TableB *>;
static std::map<int,TableC *> *tc = new std::map<int,TableC *>;
static std::map<int,TableD *> *td = new std::map<int,TableD *>;

static Oscar oscar;

long init_bufrtables_py(std::string tables_dir );
std::string bufresohmsg_py(long tableB_ptr, std::string fname);
long destroy_bufrtables_py(std::string s);

bool norbufr_init_oscar(std::string oscardb_dir);

#endif

