/*
 * (C) Copyright 2023, met.no
 *
 * This file is part of the Norbufr BUFR en/decoder
 *
 * Author: istvans@met.no
 *
 */

#ifndef _LOGBUFFER_H_
#define _LOGBUFFER_H_

#include <list>
#include <string>
#include <vector>

#include "LogEntry.h"

class LogBuffer {

public:
  LogBuffer(LogLevel l = LogLevel::TRACE, int max_size = 5000);
  ~LogBuffer();
  bool addLogEntry(std::string);
  bool addLogEntry(LogEntry);
  void clear();
  std::string toCsv(char delimiter = ';', LogLevel l = LogLevel::UNKNOWN) const;
  std::string toJson(LogLevel l = LogLevel::UNKNOWN) const;
  void toCsvList(std::list<std::string> &list, char delimiter = ';',
                 LogLevel l = LogLevel::UNKNOWN) const;
  void toJsonList(std::list<std::string> &list,
                  LogLevel l = LogLevel::UNKNOWN) const;
  void setLogLevel(LogLevel l, bool clean = true);

private:
  std::list<LogEntry> buffer;
  uint32_t max_size;

protected:
  LogLevel log_level;
};

#endif
