/*
 * (C) Copyright 2023, met.no
 *
 * This file is part of the Norbufr BUFR en/decoder
 *
 * Author: istvans@met.no
 *
 */

#ifndef _NORBUF_H_
#define _NORBUF_H_

#include <list>

#include "Descriptor.h"
#include "LogBuffer.h"
#include "Sections.h"
#include "Tables.h"

const LogLevel norbufr_default_loglevel = LogLevel::TRACE;

class NorBufr : public Section1,
                public Section2,
                public Section3,
                public Section4 {

public:
  NorBufr();
  ~NorBufr();

  std::streampos fromBuffer(char *ext_buf, std::streampos ext_buf_pos,
                            std::streampos ext_buf_size);
  void setTableDir(std::string s);
  ssize_t extractDescriptors(int ss = 0, ssize_t subsb = 0);
  bool saveBuffer(std::string) const;
  double getValue(const Descriptor &d, double v) const;
  uint64_t getBitValue(const Descriptor &d, uint64_t v) const;
  int getValue(const Descriptor &d, int v) const;
  std::string getValue(const Descriptor &d, std::string s,
                       bool with_unit = true) const;
  void setTableB(TableB *tb) { tabB = tb; }
  void setTableC(TableC *tc) { tabC = tc; }
  void setTableD(TableD *td) { tabD = td; }
  uint64_t length() const;
  void print(const DescriptorId df, const std::string filter,
             const DescriptorId dv) const;
  void printValue(DescriptorId df) const;
  std::ostream &printDetail(std::ostream &os = std::cout);

  // TODO: add local/external tables

  void freeBuffer();

  void logToCsvList(std::list<std::string> &list, char delimiter = ';',
                    LogLevel l = LogLevel::UNKNOWN) const;
  void logToJsonList(std::list<std::string> &list,
                     LogLevel l = LogLevel::UNKNOWN) const;

  void setBufrId(std::string);

private:
  bool setSections(int slen);
  void clearTable();
  void clear();
  long checkBuffer();
  std::vector<DescriptorMeta *>::iterator findMeta(DescriptorMeta *dm);
  DescriptorMeta *addMeta(DescriptorMeta *dm);
  uint64_t uncompressDescriptor(std::list<DescriptorId>::iterator &it,
                                ssize_t &sb, ssize_t &subsetsb,
                                uint16_t *repeatnum = 0);

protected:
  ssize_t len;
  uint8_t edition;

  std::string table_dir;
  TableA *tabA;
  TableB *tabB;
  TableC *tabC;
  TableD *tabD;

  uint8_t *buffer;

  std::string bufr_id;

  std::vector<std::list<Descriptor>> desc;
  std::vector<DescriptorMeta *> extraMeta;

  // Subset start bit
  std::vector<ssize_t> subsets;

  // Section4 uncompressed bits
  std::vector<bool> ucbits;

  mutable LogBuffer lb;

  friend std::istream &operator>>(std::istream &is, NorBufr &bufr);
  friend std::ostream &operator<<(std::ostream &is, NorBufr &bufr);
};

#endif
