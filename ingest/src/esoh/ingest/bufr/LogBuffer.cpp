/*
 * (C) Copyright 2023, met.no
 *
 * This file is part of the Norbufr BUFR en/decoder
 *
 * Author: istvans@met.no
 *
 */

#include <iostream>
#include <sstream>

#include "LogBuffer.h"

LogBuffer::LogBuffer(LogLevel l, int max) {
  log_level = l;
  max_size = max;
}

LogBuffer::~LogBuffer() {}

bool LogBuffer::addLogEntry(std::string t) {
  if (buffer.size() == max_size)
    return false;
  LogEntry e(t);
  addLogEntry(e);

  return true;
}

bool LogBuffer::addLogEntry(LogEntry e) {
  if (log_level > e.getLogLevel())
    return false;
  if (buffer.size() == max_size)
    return false;
  buffer.push_back(e);

  if (max_size && (buffer.size() + 1 == max_size)) {
    LogEntry log_full_entry("LogBuffer is full", LogLevel::FATAL, __func__);
    buffer.push_back(log_full_entry);
  }

  return true;
}

void LogBuffer::clear() { buffer.clear(); }

std::string LogBuffer::toCsv(char delimiter, LogLevel l) const {
  std::string ret;
  for (auto e : buffer) {
    if (e.getLogLevel() >= l)
      ret += e.toCsv(delimiter) + "\n";
  }
  return ret;
}

void LogBuffer::toCsvList(std::list<std::string> &list, char delimiter,
                          LogLevel l) const {
  for (auto e : buffer) {
    if (e.getLogLevel() >= l)
      list.push_back(e.LogEntry::toCsv(delimiter));
  }
}

std::string LogBuffer::toJson(LogLevel l) const {
  std::string ret;
  for (auto e : buffer) {
    if (e.getLogLevel() >= l)
      ret += e.toJson() + "\n";
  }
  return ret;
}

void LogBuffer::toJsonList(std::list<std::string> &list, LogLevel l) const {
  for (auto e : buffer) {
    if (e.getLogLevel() >= l)
      list.push_back(e.LogEntry::toJson());
  }
}

void LogBuffer::setLogLevel(LogLevel l, bool clean) {
  log_level = l;
  if (clean) {
    auto predicate = [l](LogEntry _le) { return l >= _le.getLogLevel(); };
    buffer.remove_if(predicate);
  }
}