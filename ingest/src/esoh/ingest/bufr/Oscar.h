/*
 * (C) Copyright 2023, Eumetnet
 *
 * This file is part of the E-SOH Norbufr BUFR en/decoder interface
 *
 * Author: istvans@met.no
 *
 */

#ifndef _OSCAR_
#define _OSCAR_

#include <map>
#include <string>

#include "rapidjson/document.h"

#include "CountryCodes.h"
#include "WSI.h"

static const rapidjson::Value oscar_not_found;

class Oscar {
public:
  Oscar();
  Oscar(const char *filename);
  bool addStation(const char *filename);
  bool addStation(rapidjson::Value &);
  std::string findWigosId(WSI) const;
  const rapidjson::Value &findStation(WSI) const;
  const rapidjson::Value &operator[](WSI) const;
  std::string to_string(WSI) const;
  int size() const;

private:
  rapidjson::Document d;
  std::map<WSI, rapidjson::Value> stations;
};

#endif
