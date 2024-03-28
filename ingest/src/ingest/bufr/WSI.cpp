/*
 * (C) Copyright 2023, Eumetnet
 *
 * This file is part of the E-SOH Norbufr BUFR en/decoder interface
 *
 * Author: istvans@met.no
 *
 */

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <sstream>

#include "NorBufrIO.h"
#include "WSI.h"

WSI::WSI() {
  wigos_id_series = 0;
  wigos_issuer_id = 0;
  wigos_issue_num = 0;
  wigos_local_id = "";
}

WSI::WSI(std::string s) { from_string(s); }

WSI::WSI(const char *c) { from_string(std::string(c)); }

std::string WSI::to_string() const {
  std::stringstream ss;
  ss << wigos_id_series;
  ss << "-";
  ss << wigos_issuer_id;
  ss << "-";
  ss << wigos_issue_num;
  ss << "-";
  ss << wigos_local_id;

  return ss.str();
}

void WSI::setWigosIdSeries(int wid) { wigos_id_series = wid; }

void WSI::setWigosIssuerId(uint16_t wis) { wigos_issuer_id = wis; }

void WSI::setWigosIssueNum(uint16_t wisn) { wigos_issue_num = wisn; }

void WSI::setWigosLocalId(std::string wlid) {
  wigos_local_id = NorBufrIO::strTrim(wlid);
}

void WSI::setWmoId(int wlid) {
  std::stringstream ss;
  ss << std::setw(5) << std::setfill('0') << wlid;
  wigos_local_id = ss.str();
  setWigosIssuerId(20000);
}

int WSI::getWigosIdSeries() const { return wigos_id_series; }

uint16_t WSI::getWigosIssuerId() const { return wigos_issuer_id; }

uint16_t WSI::getWigosIssueNum() const { return wigos_issue_num; }

std::string WSI::getWigosLocalId() const { return wigos_local_id; }

bool WSI::from_string(std::string s) {
  wigos_id_series = wigos_issue_num = wigos_issuer_id = 0;
  wigos_local_id = "";
  int cnt = std::count(s.begin(), s.end(), '-');
  const int tmp_size = 10;
  char tmp[tmp_size];
  std::stringstream ss;
  ss << s;
  if (cnt == 3) {
    ss >> wigos_id_series;
    ss.getline(tmp, tmp_size, '-');
    ss >> wigos_issuer_id;
    ss.getline(tmp, tmp_size, '-');
    ss >> wigos_issue_num;
    ss.getline(tmp, tmp_size, '-');
  }

  ss >> wigos_local_id;
  return true;
}

std::ostream &operator<<(std::ostream &os, const WSI &w) {
  os << w.to_string();
  return os;
}

bool operator==(const WSI &lhs, const WSI &rhs) {
  return ((lhs.wigos_id_series == rhs.wigos_id_series) &&
          (lhs.wigos_issuer_id == rhs.wigos_issuer_id) &&
          (lhs.wigos_issue_num == rhs.wigos_issue_num) &&
          (lhs.wigos_local_id == rhs.wigos_local_id));
}

bool operator<(const WSI &lhs, const WSI &rhs) {
  if (lhs.wigos_id_series < rhs.wigos_id_series)
    return true;
  if (lhs.wigos_id_series > rhs.wigos_id_series)
    return false;
  if (lhs.wigos_issuer_id < rhs.wigos_issuer_id)
    return true;
  if (lhs.wigos_issuer_id > rhs.wigos_issuer_id)
    return false;
  if (lhs.wigos_issue_num < rhs.wigos_issue_num)
    return true;
  if (lhs.wigos_issue_num > rhs.wigos_issue_num)
    return false;
  if (lhs.wigos_local_id < rhs.wigos_local_id)
    return true;

  return false;
}

bool operator<=(const WSI &lhs, const WSI &rhs) {
  return ((lhs == rhs) || (lhs < rhs));
}

bool operator>(const WSI &lhs, const WSI &rhs) { return (!(lhs <= rhs)); }

bool operator>=(const WSI &lhs, const WSI &rhs) {
  return ((lhs == rhs) || !(lhs < rhs));
}
