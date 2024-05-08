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

bool WSI::setWigosIdSeries(int wid) {
  wigos_id_series = wid;
  return true;
}

bool WSI::setWigosIssuerId(uint16_t wis) {
  if (wis > wigos_range)
    return false;
  wigos_issuer_id = wis;
  return true;
}

bool WSI::setWigosIssueNum(uint16_t wisn) {
  if (wisn > wigos_range)
    return false;
  wigos_issue_num = wisn;
  return true;
}

bool WSI::setWigosLocalId(std::string wlid) {
  std::string tmp = NorBufrIO::strTrim(wlid);
  if (tmp.size() > wigos_local_max_len)
    return false;
  wigos_local_id = tmp;
  return true;
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
  int cnt = std::count(s.begin(), s.end(), '-');
  const int tmp_size = 16;
  char tmp[tmp_size];
  uint16_t val_id_series;
  uint16_t val_issue_num;
  uint16_t val_issuer_id;
  std::stringstream ss;
  ss << s;
  if (cnt == 3) {
    ss >> val_id_series;
    ss.getline(tmp, tmp_size, '-');
    ss >> val_issuer_id;
    if (val_issuer_id > wigos_range) {
      return false;
    }
    ss.getline(tmp, tmp_size, '-');
    ss >> val_issue_num;
    if (val_issue_num > wigos_range) {
      return false;
    }
    ss.getline(tmp, tmp_size, '-');
  } else {
    return false;
  }

  if (ss.fail() || ss.bad()) {
    return false;
  }

  ss >> wigos_local_id;
  if (ss.bad()) {
    return false;
  }

  if (wigos_local_id.size() > 16) {
    wigos_local_id = "";
    return false;
  } else {
    wigos_id_series = val_id_series;
    wigos_issue_num = val_issue_num;
    wigos_issuer_id = val_issuer_id;
  }

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
