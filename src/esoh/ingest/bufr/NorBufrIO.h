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
#include <vector>

namespace NorBufrIO {

uint64_t readBytes(std::istream &is, ssize_t size);

unsigned long findBytes(std::ifstream &is, const char *seq, unsigned int size);

unsigned long getBytes(uint8_t *buffer, int size);

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
} // namespace NorBufrIO

#endif
