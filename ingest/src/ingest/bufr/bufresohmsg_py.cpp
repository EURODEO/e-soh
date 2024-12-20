/*
 * (C) Copyright 2023, Eumetnet
 *
 * This file is part of the E-SOH Norbufr BUFR en/decoder interface
 *
 * Author: istvans@met.no
 *
 */

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <list>
#include <sstream>
#include <string>

#include "Descriptor.h"
#include "ESOHBufr.h"
#include "Tables.h"
#include "bufresohmsg_py.h"

bool norbufr_init_bufrtables(std::string tables_dir) {

  if (tb.size() || tc.size() || td.size())
    return false;
  std::string tbl_dir;
  if (tables_dir.size()) {
    tbl_dir = tables_dir;
  } else {
    tbl_dir = "/usr/share/eccodes/definitions/bufr/tables/0/wmo";
  }

  std::string eccBtable_dir(tbl_dir);
  std::string eccDtable_dir(tbl_dir);

  for (const auto &entry : std::filesystem::directory_iterator(eccBtable_dir)) {
    auto vers = std::stoi(entry.path().filename().string());
    TableB tb_e(entry.path().string() + "/element.table");
    tb[vers] = tb_e;
    TableC tc_e(entry.path().string() + "/codetables");
    tc[vers] = tc_e;
  }

  for (const auto &entry : std::filesystem::directory_iterator(eccDtable_dir)) {
    auto vers = std::stoi(entry.path().filename().string());
    TableD tb_d(entry.path().string() + "/sequence.def");
    td[vers] = tb_d;
  }
  if (!tb.size() || !tc.size() || !td.size()) {
    return false;
  }

  return true;
}

bool norbufr_update_bufrtables(std::string tables_dir) {
  tb.clear();
  tc.clear();
  td.clear();
  return norbufr_init_bufrtables(tables_dir);
}

bool norbufr_init_oscar(std::string oscardb_dir) {
  bool ret = oscar.addStation(oscardb_dir.c_str());
  return ret;
}

bool norbufr_init_schema_template(std::string schema_path) {

  if (schema_path.size()) {
    std::string def_msg;
    std::ifstream msgTemplate(schema_path.c_str(), std::ios_base::in);
    char c;
    while (msgTemplate.get(c)) {
      def_msg += c;
    }
    bufr_input_schema = def_msg;
    if (!def_msg.size()) {
      return false;
    }
  }

  return true;
}

std::list<std::string> norbufr_bufresohmsg(std::string fname) {

  std::list<std::string> ret;

  std::ifstream bufrFile(fname.c_str(),
                         std::ios_base::in | std::ios_base::binary);

  std::filesystem::path file_path(fname);

  while (bufrFile.good()) {

    ESOHBufr *bufr = new ESOHBufr;
    bufr->setBufrId(file_path.filename());
    bufr->setOscar(&oscar);
    bufr->setMsgTemplate(bufr_input_schema);
    bufr->setShadowWigos(default_shadow_wigos_py);

    if (bufrFile >> *bufr) {

      bufr->setTableB(
          &tb.at(bufr->getVersionMaster() &&
                         tb.find(bufr->getVersionMaster()) != tb.end()
                     ? bufr->getVersionMaster()
                     : tb.rbegin()->first));
      bufr->setTableC(
          &tc.at(bufr->getVersionMaster() &&
                         tc.find(bufr->getVersionMaster()) != tc.end()
                     ? bufr->getVersionMaster()
                     : tc.rbegin()->first));
      bufr->setTableD(
          &td.at(bufr->getVersionMaster() &&
                         td.find(bufr->getVersionMaster()) != td.end()
                     ? bufr->getVersionMaster()
                     : td.rbegin()->first));

      bufr->extractDescriptors();

      std::list<std::string> msg = bufr->msg();
      bufr->logToCsvList(esoh_bufr_log);
      ret.insert(ret.end(), msg.begin(), msg.end());
    }
    delete bufr;
  }

  return ret;
}

std::list<std::string> norbufr_bufresohmsgmem(char *api_buf, int api_size) {

  std::list<std::string> ret;
  uint64_t position = 0;

  while (position < static_cast<uint64_t>(api_size)) {

    ESOHBufr *bufr = new ESOHBufr;
    // TODO:
    // bufr->setBufrId(file_path.filename());
    bufr->setOscar(&oscar);
    bufr->setMsgTemplate(bufr_input_schema);
    bufr->setShadowWigos(default_shadow_wigos_py);

    uint64_t n = bufr->fromBuffer(api_buf, position, api_size);
    if (n == ULONG_MAX)
      position = ULONG_MAX;
    if (n > position) {
      position = n;
      bufr->setTableB(
          &tb.at(bufr->getVersionMaster() &&
                         tb.find(bufr->getVersionMaster()) != tb.end()
                     ? bufr->getVersionMaster()
                     : tb.rbegin()->first));
      bufr->setTableC(
          &tc.at(bufr->getVersionMaster() &&
                         tc.find(bufr->getVersionMaster()) != tc.end()
                     ? bufr->getVersionMaster()
                     : tc.rbegin()->first));
      bufr->setTableD(
          &td.at(bufr->getVersionMaster() &&
                         td.find(bufr->getVersionMaster()) != td.end()
                     ? bufr->getVersionMaster()
                     : td.rbegin()->first));
      bufr->extractDescriptors();

      std::list<std::string> msg = bufr->msg();
      bufr->logToCsvList(esoh_bufr_log);
      ret.insert(ret.end(), msg.begin(), msg.end());
    }
    delete bufr;
  }

  return ret;
}

std::string norbufr_bufrprint(std::string fname) {

  std::stringstream ret;

  std::ifstream bufrFile(fname.c_str(),
                         std::ios_base::in | std::ios_base::binary);

  while (bufrFile.good()) {

    ESOHBufr *bufr = new ESOHBufr;

    if (bufrFile >> *bufr) {

      bufr->setTableB(
          &tb.at(bufr->getVersionMaster() &&
                         tb.find(bufr->getVersionMaster()) != tb.end()
                     ? bufr->getVersionMaster()
                     : tb.rbegin()->first));
      bufr->setTableC(
          &tc.at(bufr->getVersionMaster() &&
                         tc.find(bufr->getVersionMaster()) != tc.end()
                     ? bufr->getVersionMaster()
                     : tc.rbegin()->first));
      bufr->setTableD(
          &td.at(bufr->getVersionMaster() &&
                         td.find(bufr->getVersionMaster()) != td.end()
                     ? bufr->getVersionMaster()
                     : td.rbegin()->first));

      bufr->extractDescriptors();

      ret << *bufr;
    }
  }

  return ret.str();
}

std::list<std::string> norbufr_log() { return esoh_bufr_log; }

void norbufr_log_clear() { esoh_bufr_log.clear(); }
void norbufr_set_default_wigos(std::string s) { default_shadow_wigos_py = s; }

PYBIND11_MODULE(bufresohmsg_py, m) {
  m.doc() = "bufresoh E-SOH MQTT message generator plugin";

  m.def("init_bufrtables_py", &norbufr_init_bufrtables, "Init BUFR Tables");
  m.def("update_bufrtables_py", &norbufr_update_bufrtables, "Init BUFR Tables");

  m.def("bufresohmsg_py", &norbufr_bufresohmsg,
        "bufresoh MQTT message generator");
  m.def("bufresohmsgmem_py", &norbufr_bufresohmsgmem,
        "bufresoh MQTT message generator");
  m.def("bufrprint_py", &norbufr_bufrprint, "Print bufr message");
  m.def("bufrlog_py", &norbufr_log, "Get bufr log messages list");
  m.def("bufrlog_clear_py", &norbufr_log_clear, "Clear log messages list");

  m.def("init_oscar_py", &norbufr_init_oscar, "Init OSCAR db");
  m.def("init_bufr_schema_py", &norbufr_init_schema_template,
        "Init BUFR schema");
  m.def("bufr_sdwigos_py", &norbufr_set_default_wigos,
        "Set default shadow WIGOS Id");
}
