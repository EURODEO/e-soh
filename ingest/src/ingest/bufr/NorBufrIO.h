/*
 * (C) Copyright 2023, met.no
 *
 * This file is part of the Norbufr BUFR en/decoder
 *
 * Author: istvans@met.no
 *
 */

#ifndef _NORBUFIO_H_
#define _NORBUFIO_H_

#include <climits>
#include <fstream>
#include <list>
#include <vector>

namespace NorBufrIO {

uint64_t readBytes(std::istream &is, ssize_t size);
uint64_t readBytes(char *buf, ssize_t size);

uint64_t findBytes(std::istream &is, const char *seq, unsigned int size);
uint64_t findBytes(char *, unsigned int buf_size, const char *seq,
                   unsigned int size);

uint64_t getBytes(uint8_t *buffer, int size);

std::vector<bool> valueToBitVec(const uint64_t value, const int datawidth);

uint64_t getBitValue(const uint64_t startbit, const int datawidth,
                     const bool missing_mask, const std::vector<bool> &bits);

std::string getBitStrValue(const uint64_t startbit, const int datawidth,
                           const std::vector<bool> &bits);

std::string getBitStr(const uint64_t startbit, const int datawidth,
                      const std::vector<bool> &bits);

std::vector<bool> getBitVec(const uint64_t startbit, const int datawidth,
                            const std::vector<bool> &bits);

std::string strTrim(std::string s);
void strPrintable(std::string &s);
ssize_t strisotime(char *date_str, size_t date_max, const time_t *date,
                   bool usec = false);
ssize_t strisotime(char *date_str, size_t date_max, const struct timeval *date,
                   bool usec = false);

void filterStr(std::string &s,
               const std::list<std::pair<char, char>> &repl_chars);

std::istream &getElement(std::istream &is, char *dest, const int size,
                         const char endch);

} // namespace NorBufrIO

#endif
