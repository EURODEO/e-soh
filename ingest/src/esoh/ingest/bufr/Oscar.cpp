/*
 * (C) Copyright 2023, Eumetnet
 *
 * This file is part of the E-SOH Norbufr BUFR en/decoder interface
 *
 * Author: istvans@met.no
 *
 */

#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>

#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include <rapidjson/istreamwrapper.h>

#include "Oscar.h"
#include "WSI.h"

Oscar::Oscar() {}

// from WMO OSCAR stationSearchResults file
Oscar::Oscar(const char *filename) {
  stations.clear();
  addStation(filename);
}

// from WMO OSCAR stationSearchResults file
bool Oscar::addStation(const char *filename) {
  std::ifstream oscarFile(filename, std::ios_base::in | std::ios_base::binary);
  rapidjson::IStreamWrapper isw(oscarFile);

  d.ParseStream(isw);

  if (d.HasMember("stationSearchResults")) {
    int i = 0;
    for (auto &v : d["stationSearchResults"].GetArray()) {
      i++;
      if (v.HasMember("wigosId")) {
        addStation(v);

      } else {
        // std::cerr << "Wigos ID missing: " << v["id"].GetUint() << "\n";
      }
    }
  }

  return true;
}

bool Oscar::addStation(rapidjson::Value &v) {
  rapidjson::Document d;

  std::string wigosId = v["wigosId"].GetString();
  WSI ws(wigosId);
  stations[ws] = v;

  return true;
}

std::string Oscar::findWigosId(WSI wsi) const {
  std::string ret = "";
  const rapidjson::Value &v = findStation(wsi);
  if (v != oscar_not_found) {
    ret = v["wigosId"].GetString();
  }

  return ret;
}

const rapidjson::Value &Oscar::findStation(WSI wsi) const {
  const rapidjson::Value &ret = this->operator[](wsi);
  if (ret != oscar_not_found)
    return ret;

  for (auto &v : stations) {
    if (v.second.HasMember("wigosStationIdentifiers") &&
        v.second["wigosStationIdentifiers"].Size() > 0) {
      for (auto &stid : v.second["wigosStationIdentifiers"].GetArray()) {
        WSI wsi_station(stid["wigosStationIdentifier"].GetString());
        if (wsi_station.getWigosLocalId() == wsi.getWigosLocalId()) {
          int wid = wsi_station.getWigosIssuerId();
          int wid_q = wsi.getWigosIssuerId();
          if (wid_q > 0 && wid_q < 10000 && wid > 0 && wid < 10000) {
            // Different Country codes ?
            if (wid != wid_q) {
              continue;
            }
          }
          return v.second;
        }
      }
    }
  }

  return oscar_not_found;
}

const rapidjson::Value &Oscar::operator[](WSI wsi) const {
  if (stations.count(wsi) == 1) {
    const rapidjson::Value &ret = stations.at(wsi);
    return ret;
  }

  return oscar_not_found;
}

std::string Oscar::to_string(WSI wsi) const {
  std::string ret;

  const rapidjson::Value &v = this->operator[](wsi);
  if (v == oscar_not_found)
    return "";
  rapidjson::StringBuffer sb;
  rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(sb);
  v.Accept(writer);
  ret = sb.GetString();

  return ret;
}

int Oscar::size() const { return stations.size(); }
