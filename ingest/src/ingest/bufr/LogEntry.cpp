#include <sstream>

#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"

#include "LogEntry.h"
#include "NorBufrIO.h"

LogEntry::LogEntry() {}

LogEntry::~LogEntry() {}

LogEntry::LogEntry(std::string msg, LogLevel l, std::string mid,
                   std::string bid)
    : log_msg(msg), log_level(l), module_id(mid), bufr_id(bid) {
  gettimeofday(&tv, 0);
}

std::string LogEntry::toCsv(char delimiter) const {
  std::stringstream ss;
  ss << entryTime() << delimiter << LogLevelStr[log_level] << delimiter
     << module_id << delimiter << bufr_id << delimiter << log_msg << delimiter;

  return ss.str();
}

std::string LogEntry::toJson() const {
  std::string ret;

  rapidjson::Document message;
  rapidjson::Document::AllocatorType &message_allocator =
      message.GetAllocator();
  message.Parse(log_message_template);

  rapidjson::Value datetime;
  datetime.SetString(entryTime().c_str(), message_allocator);
  message["datetime"] = datetime;

  rapidjson::Value log_level_str;
  log_level_str.SetString(LogLevelStr[log_level].c_str(), message_allocator);
  message["loglevel"] = log_level_str;

  rapidjson::Value bufrid;
  bufrid.SetString(bufr_id.c_str(), message_allocator);
  message["bufrid"] = bufrid;

  rapidjson::Value moduleid;
  moduleid.SetString(module_id.c_str(), message_allocator);
  message["moduleid"] = moduleid;

  rapidjson::Value logmsg;
  logmsg.SetString(log_msg.c_str(), message_allocator);
  message["logmsg"] = logmsg;

  rapidjson::StringBuffer sb;
  rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(sb);
  message.Accept(writer);
  ret = sb.GetString();

  return ret;
}

LogLevel LogEntry::getLogLevel() const { return log_level; }

std::string LogEntry::entryTime() const {

  struct tm entry_tm;
  gmtime_r(&(tv.tv_sec), &entry_tm);

  const int date_len = 50;
  char date_str[date_len];
  size_t dl = NorBufrIO::strisotime(date_str, date_len, &tv, true);
  std::string ret(date_str, dl);

  return ret;
}
