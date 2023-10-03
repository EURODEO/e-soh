/*
 * (C) Copyright 2023, met.no
 *
 * This file is part of the Norbufr BUFR en/decoder
 *
 * Author: istvans@met.no
 *
 */

#ifndef _TABLES_H_
#define _TABLES_H_

#include <list>
#include <map>
#include <string>
#include <vector>

#include "Descriptor.h"

class TableBase {};

class TableA {
public:
  TableA();
  std::string operator[](int);

private:
  std::vector<std::string> tableA;
};

class TableB {
public:
  TableB();
  TableB(std::string f);

  const DescriptorMeta &at(DescriptorId d, bool ignore_throw = false) const;

private:
  void clear();
  bool readWMO(std::string f);
  bool readECCodes(std::string filename);
  std::map<DescriptorId, DescriptorMeta> tableB;
  DescriptorMeta dm0;
};

class TableC {
public:
  TableC();
  TableC(std::string f);
  std::string codeStr(DescriptorId di, int c);

private:
  void clear();
  bool readWMO(std::string f);
  bool readECCodes(std::string filename);
  std::map<DescriptorId, std::map<int, std::string>> tableC;
};

class TableD {
public:
  TableD();
  TableD(std::string f);

  std::list<DescriptorId> expandDescriptor(const DescriptorId d,
                                           bool recursive = false) const;
  const std::list<DescriptorId> &
  at(DescriptorId d,
     bool ignore_throw = false) const; // TODO: exception: out_of_range
  ssize_t size() const;

private:
  void clear();
  bool readWMO(std::string f);
  bool readECCodes(std::string filename);
  std::map<DescriptorId, std::list<DescriptorId>> tableD;
};

#endif
