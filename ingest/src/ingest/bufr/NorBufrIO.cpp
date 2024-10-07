/*
 * (C) Copyright 2023, met.no
 *
 * This file is part of the Norbufr BUFR en/decoder
 *
 * Author: istvans@met.no
 *
 */

#include <algorithm>
#include <bitset>
#include <cstring>
#include <iostream>

#include "NorBufrIO.h"

uint64_t NorBufrIO::readBytes(std::istream &is, ssize_t size) {
  uint64_t j = 0;
  uint64_t ret = is.get();
  for (int i = 1; i < size; ++i) {
    j = is.get();
    ret *= 0x100;
    ret += j;
  }
  return ret;
}

uint64_t NorBufrIO::readBytes(char *buf, ssize_t size) {
  uint64_t j = 0;
  uint64_t ret = buf[0];
  if (size) {
    for (int i = 1; i < size; ++i) {
      j = buf[i];
      ret *= 0x100;
      ret += j;
    }
  }
  return ret;
}

uint64_t NorBufrIO::findBytes(std::istream &is, const char *seq,
                              unsigned int size) {
  uint64_t j = 0;
  char c;
  uint64_t position = is.tellg();
  std::cerr << "POS: " << position << "\n";
  while (j < size && is.get(c)) {
    if (c == seq[j]) {
      ++j;
    } else {
      if (c == seq[0]) {
        j = 1;
      } else {
        j = 0;
      }
    }
    ++position;
  }
  return (j < size ? ULONG_MAX : position - size);
}

uint64_t NorBufrIO::findBytes(char *buf, unsigned int buf_size, const char *seq,
                              unsigned int size) {
  uint64_t j = 0;
  char c;
  uint64_t position = 0;
  while (j < size && position < buf_size) {
    c = buf[position];
    if (c == seq[j]) {
      ++j;
    } else {
      if (c == seq[0]) {
        j = 1;
      } else {
        j = 0;
      }
    }
    ++position;
  }
  return (j < size ? ULONG_MAX : position - size);
}

uint64_t NorBufrIO::getBytes(uint8_t *buffer, int size) {
  uint64_t ret = buffer[0];
  if (size > 8) {
    std::cerr << "::getBytes: ERROR, size: " << size << "\n";
  }

  for (int i = 1; i < size; ++i) {
    ret <<= 8;
    ret |= buffer[i];
  }

  return ret;
}

std::vector<bool> NorBufrIO::valueToBitVec(uint64_t value, int datawidth) {
  std::vector<bool> ret;
  std::bitset<64> bs(value);
  for (int i = datawidth - 1; i >= 0; --i) {
    ret.push_back(bs[i]);
  }

  return ret;
}

uint64_t NorBufrIO::getBitValue(const uint64_t startbit, const int datawidth,
                                const bool missing_mask,
                                const std::vector<bool> &bits) {
  bool missing = true;
  uint64_t ret = 0;
  if (startbit + datawidth <= bits.size()) {
    for (int i = 0; i < datawidth; ++i) {
      ret *= 2;
      ret += bits[startbit + i];
      if (missing && bits[startbit + i] == 0)
        missing = false;
    }
  }
  if (missing && missing_mask && datawidth > 1)
    return ULONG_MAX;

  return ret;
}

std::string NorBufrIO::getBitStrValue(const uint64_t startbit,
                                      const int datawidth,
                                      const std::vector<bool> &bits) {
  std::string ret;
  for (int i = 0; i < datawidth; ++i) {
    if (startbit + datawidth > bits.size() || bits[startbit + i])
      ret += "1";
    else
      ret += "0";
  }
  return ret;
}

std::string NorBufrIO::getBitStr(const uint64_t startbit, const int datawidth,
                                 const std::vector<bool> &bits) {
  std::string ret;
  for (int i = 0; i < datawidth; i += 8) {
    char c = NorBufrIO::getBitValue(startbit + i, 8, true, bits);
    ret += std::string(1, c);
  }
  return ret;
}

std::vector<bool> NorBufrIO::getBitVec(const uint64_t startbit,
                                       const int datawidth,
                                       const std::vector<bool> &bits) {
  std::vector<bool> ret;
  for (int i = 0; i < datawidth; ++i) {
    if (startbit + datawidth < bits.size())
      ret.push_back(bits[startbit + i]);
  }
  return ret;
}

std::string NorBufrIO::strTrim(const std::string s) {
  const auto str_begin = s.find_first_not_of(' ');
  std::string ret;
  if (str_begin != std::string::npos) {
    const auto str_end = s.find_last_not_of(' ');
    const auto str_range = str_end - str_begin + 1;
    ret = s.substr(str_begin, str_range);
  }
  return ret;
}

void NorBufrIO::strPrintable(std::string &s) {
  s.erase(std::remove_if(s.begin(), s.end(),
                         [](unsigned char c) { return !std::isprint(c); }),
          s.end());
}

ssize_t NorBufrIO::strisotime(char *date_str, size_t date_max,
                              const struct timeval *date, bool usec) {
  const char *uformat = "%FT%H:%M:%S.000000%z";
  const char *format = "%FT%H:%M:%S%z";
  const char *fmt = format;

  if (usec) {
    fmt = uformat;
  }

  size_t dl = strftime(date_str, date_max, fmt, gmtime(&(date->tv_sec)));

  // Copy microseconds into the date char string
  if (usec && dl > 26) {
    char usec[8];
    sprintf(usec, "%06ld", date->tv_usec);
    memcpy(date_str + 20, usec, 6);
  }

  // Change Timezone +0000 to +00:00
  if (dl > 4) {
    ++dl;
    date_str[dl] = '\0';
    date_str[dl - 1] = date_str[dl - 2];
    date_str[dl - 2] = date_str[dl - 3];
    date_str[dl - 3] = ':';
  }

  return dl;
}

void NorBufrIO::filterStr(std::string &s,
                          const std::list<std::pair<char, char>> &repl_chars) {

  for (auto rch : repl_chars) {
    if (rch.second) {
      std::replace(s.begin(), s.end(), rch.first, rch.second);
    } else {
#if __cplusplus >= 202002L
      std::erase(s, rch.first);
#else
      s.erase(std::remove(s.begin(), s.end(), rch.first), s.end());
#endif
    }
  }

  return;
}
