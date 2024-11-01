/*
 * (C) Copyright 2023, Eumetnet
 *
 * This file is part of the E-SOH Norbufr BUFR en/decoder interface
 *
 * Author: istvans@met.no
 *
 */

#include <time.h>

#include <algorithm>
#include <bitset>
#include <cmath>
#include <cstdlib>
#include <iomanip>
#include <list>
#include <sstream>

#include "ESOHBufr.h"
#include "WSI.h"

ESOHBufr::ESOHBufr() {
  oscar = 0;
  lb.setLogLevel(LogLevel::WARN);
  const char *message_template = " { \
        \"id\" : \"\", \
        \"version\" : \"v4.0\", \
        \"type\" : \"Feature\", \
        \"geometry\" : \"null\", \
        \"properties\" : { \
            \"data_id\": \"data_id\", \
            \"metadata_id\": \"metatata_id\", \
            \"datetime\" : \"null\", \
            \"Conventions\" : \"Default BUFR Conventions\", \
            \"summary\" : \"Default Summary\", \
            \"license\" : \"http//spdx.org/licenses/CC-BY-4.0(CC-BY-4.0)\", \
            \"naming_authority\" : \"no.met\", \
            \"content\" : { \
                \"encoding\": \"utf-8\", \
                \"standard_name\": \"\", \
                \"unit\": \"\", \
                \"size\": 0, \
                \"value\": \"\"} \
             }, \
        \"links\" : [ \
            { \
                \"href\" : \"Default BUFR links\", \
                \"rel\" : \"canonical\" \
            } \
          ] \
        }";
  setMsgTemplate(message_template);
  shadow_wigos.from_string(default_shadow_wigos);
  initTimeInterval();
}

void ESOHBufr::setOscar(Oscar *o) { oscar = o; }

void ESOHBufr::setMsgTemplate(std::string s) {
  if (s.size()) {
    msg_template = s;
  }
}

std::list<std::string> ESOHBufr::msg() const {

  lb.addLogEntry(LogEntry("Starting ESOH message generation", LogLevel::TRACE,
                          __func__, bufr_id));

  std::list<std::string> ret;

  rapidjson::Document message;

  if (message.Parse(msg_template.c_str()).HasParseError()) {
    lb.addLogEntry(LogEntry("ESOH message tempate parsing Error!!!",
                            LogLevel::ERROR, __func__, bufr_id));
    return ret;
  }

  rapidjson::Value &properties = message["properties"];

  rapidjson::Document::AllocatorType &message_allocator =
      message.GetAllocator();

  // pubtime
  rapidjson::Value pubtime;
  {
    struct timeval tv;
    gettimeofday(&tv, 0);
    const int date_len = 50;
    char date_str[date_len];
    size_t dl = NorBufrIO::strisotime(date_str, date_len, &tv);
    pubtime.SetString(date_str, static_cast<rapidjson::SizeType>(dl),
                      message_allocator);
  }
  properties.AddMember("pubtime", pubtime, message_allocator);

  // subsets
  int subsetnum = 0;
  for (auto s : desc) {
    lb.addLogEntry(LogEntry("Starting ESOH message generation", LogLevel::DEBUG,
                            __func__, bufr_id));
    double lat = std::numeric_limits<double>::quiet_NaN();
    double lon = std::numeric_limits<double>::quiet_NaN();
    double hei = std::numeric_limits<double>::quiet_NaN();
    double sensor_level = 0.0;
    char sensor_level_active = 0;
    std::string period_str;
    std::string period_beg;
    std::string period_end;
    bool period_update = false;
    bool start_end_period = false;
    std::string platform;
    bool platform_check = false;

    WSI wigos_id;

    rapidjson::Document subset_message;
    subset_message.CopyFrom(message, subset_message.GetAllocator());

    struct tm meas_datetime;
    memset(static_cast<void *>(&meas_datetime), 0, sizeof(meas_datetime));

    // for( auto v : s )
    for (std::list<Descriptor>::const_iterator ci = s.begin(); ci != s.end();
         ++ci) {
      if (sensor_level_active) {
        sensor_level_active--;
      } else {
        sensor_level = 0.0;
      }
      period_update = false;
      auto v = *ci;
      lb.addLogEntry(LogEntry("ESOH Descriptor: " + v.toString(),
                              LogLevel::TRACE, __func__, bufr_id));

      switch (v.f()) {
      case 0: // Element Descriptors
      {
        std::string value_str = getValue(v, std::string(), false);
        NorBufrIO::strPrintable(value_str);
        lb.addLogEntry(LogEntry("Element Descriotor value: " + value_str,
                                LogLevel::TRACE, __func__, bufr_id));

        if (value_str == "MISSING")
          break;

        if (v.x() >= 10 &&
            !(v.x() == 22 && (v.y() == 55 || v.y() == 56 || v.y() == 67)) &&
            v.x() != 25 && v.x() != 31 && v.x() != 35 && !platform_check) {
          // Check datetime
          if (meas_datetime.tm_mday == 0) {
            // Date missing, skip processing
            lb.addLogEntry(LogEntry(
                "Missing measure datetime, skip this subset: " +
                    std::to_string(subsetnum) + " Wigos: " +
                    wigos_id.to_string() + std::string(" ") + v.toString(),
                LogLevel::WARN, __func__, bufr_id));
            goto subset_end;
          }
          // Check datetime early or late
          if (!timeInInterval(meas_datetime)) {
            time_t mdt = mktime(&meas_datetime);
            const int cdt_len = 50;
            char cdt[50];
            NorBufrIO::strisotime(cdt, cdt_len, &mdt, false);

            lb.addLogEntry(LogEntry(
                "Skip subset " + std::to_string(subsetnum) +
                    ", datetime too late or too early: " + std::string(cdt),
                LogLevel::WARN, __func__, bufr_id));

            goto subset_end;
          }
          // Check station_id at OSCAR
          platform_check = true;
          if (wigos_id.getWigosLocalId().size()) {
            std::string wigos_oscar = oscar->findWigosId(wigos_id);
            if (wigos_oscar.size()) {
              if (wigos_oscar != wigos_id.to_string()) {
                wigos_id = WSI(wigos_oscar);
              }
              const rapidjson::Value &st_value = oscar->findStation(wigos_id);
              if (st_value.HasMember("name")) {
                setPlatformName(std::string(st_value["name"].GetString()),
                                subset_message, true);
              }
              if (std::isnan(lat)) {
                if (st_value.HasMember("latitude")) {
                  if (st_value["latitude"].IsDouble()) {
                    lat = st_value["latitude"].GetDouble();
                    setLocation(lat, lon, hei, subset_message);
                  }
                }
              }
              if (std::isnan(lon)) {
                if (st_value.HasMember("longitude")) {
                  if (st_value["longitude"].IsDouble()) {
                    lon = st_value["longitude"].GetDouble();
                    setLocation(lat, lon, hei, subset_message);
                  }
                }
              }
            }
          }
          // Missing mandatory geolocation values. Skip this subset
          if (std::isnan(lat) || std::isnan(lon)) {
            lb.addLogEntry(LogEntry(
                "Missing geolocation information, skip this subset: " +
                    std::to_string(subsetnum) + " Wigos: " +
                    wigos_id.to_string() + std::string(" ") + v.toString(),
                LogLevel::WARN, __func__, bufr_id));
            goto subset_end;
          }
          // geolocation OK, but WIGOS is missing, create shadow WIGOS ID
          if (!wigos_id.getWigosLocalId().size()) {
            wigos_id = genShadowWigosId(s, ci);
            if (!wigos_id.getWigosLocalId().size()) {
              std::stringstream llss;
              if (lat > 0) {
                llss << "N" << std::to_string(lat).substr(0, 7);
              } else {
                llss << "S" << std::to_string(-lat).substr(0, 7);
              }
              if (lon > 0) {
                llss << "E" << std::to_string(lon).substr(0, 7);
              } else {
                llss << "W" << std::to_string(-lon).substr(0, 7);
              }
              wigos_id.setWigosLocalId(llss.str());
            }
            lb.addLogEntry(LogEntry("Create shadow WIGOS ID: " +
                                        wigos_id.to_string() + std::string(" "),
                                    LogLevel::WARN, __func__, bufr_id));
            setPlatform(wigos_id.to_string(), subset_message);
          }
        }

        switch (v.x()) {
        case 1: // platform
        {
          bool skip_platform = true;
          switch (v.y()) {
          // WMO block and station ID
          case 1: {
            int wmo_block = 0;
            wmo_block = getValue(v, wmo_block);
            if (wmo_block == std::numeric_limits<int>::max()) {
              wmo_block = 0;
            }
            lb.addLogEntry(
                LogEntry("Found WMO Block number: " + std::to_string(wmo_block),
                         LogLevel::DEBUG, __func__, bufr_id));
            auto nexti = ci;
            ++nexti;
            int wmo_station = 0;
            // Is next the WMO station number?
            if (*nexti == DescriptorId(1002, true)) {
              wmo_station = getValue(*nexti, wmo_block);
              if (wmo_station == std::numeric_limits<int>::max()) {
                wmo_station = 0;
              }
              lb.addLogEntry(LogEntry("Found WMO Station number: " +
                                          std::to_string(wmo_station),
                                      LogLevel::DEBUG, __func__, bufr_id));
              ++ci;
            } else {
              lb.addLogEntry(
                  LogEntry("Missing WMO Station number after WMO block",
                           LogLevel::WARN, __func__, bufr_id));
            }
            if (!wigos_id.getWigosLocalId().size()) {
              wigos_id.setWmoId(wmo_block * 1000 + wmo_station);
            }
            skip_platform = false;
            // platform_check = true;
            break;
          }
          case 2: // see above (case 1)
          {
            break;
          }
          case 15: // Station or site name
          case 18: // Short station or site name
          case 19: // Long station or site name
          {
            lb.addLogEntry(LogEntry("Set Platform name:" + value_str,
                                    LogLevel::DEBUG, __func__, bufr_id));
            setPlatformName(value_str, subset_message, false);
            break;
          }
          case 101: // STATE IDENTIFIER
          {
            int bufr_state_id = 0;
            bufr_state_id = getValue(v, bufr_state_id);
            lb.addLogEntry(
                LogEntry("Found state Id:" + std::to_string(bufr_state_id),
                         LogLevel::DEBUG, __func__, bufr_id));
            int wigos_state_id = bufrToIsocc(bufr_state_id);
            if (!wigos_state_id)
              lb.addLogEntry(
                  LogEntry("State Id Unknown: " + std::to_string(bufr_state_id),
                           LogLevel::WARN, __func__, bufr_id));
            wigos_id.setWigosIssuerId(wigos_state_id);
            skip_platform = false;
            break;
          }
          case 102: // NATIONAL STATION NUMBER
          {
            if (wigos_id.getWigosLocalId().size() == 0) {
              wigos_id.setWigosLocalId(value_str);
            }
            skip_platform = false;
            break;
          }
          case 125: {
            int wig_ser = 0;
            wig_ser = getValue(v, wig_ser);
            if (wig_ser == std::numeric_limits<int>::max()) {
              wig_ser = 0;
            }
            wigos_id.setWigosIdSeries(wig_ser);
            skip_platform = false;
            break;
          }
          case 126: {
            int wig_iss_id = 0;
            wig_iss_id = getValue(v, wig_iss_id);
            if (wig_iss_id == std::numeric_limits<int>::max()) {
              wig_iss_id = 0;
            }
            wigos_id.setWigosIssuerId(wig_iss_id);
            skip_platform = false;
            break;
          }
          case 127: {
            int wig_iss_num = 0;
            wig_iss_num = getValue(v, wig_iss_num);
            if (wig_iss_num == std::numeric_limits<int>::max()) {
              wig_iss_num = 0;
            }
            wigos_id.setWigosIssueNum(wig_iss_num);
            skip_platform = false;
            break;
          }
          case 128: {
            if (value_str.size()) {
              wigos_id.setWigosLocalId(value_str);
              skip_platform = false;
            }
          }
          }

          if (!skip_platform) {
            setPlatform(wigos_id.to_string(), subset_message);
          }

          break;
        }
        case 2: {

          break;
        }
        case 4: // datetime
        {
          bool dateupdate = false;
          int time_disp = 0;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
          switch (v.y()) {
          case 1: {
            int raw_year = getValue(v, raw_year);
            if (raw_year != std::numeric_limits<int>::max()) {
              meas_datetime.tm_year = raw_year - 1900;
              dateupdate = true;
              // set 01 of Jan: mktime() change protection
              meas_datetime.tm_mday = 1;
            }
            break;
          }
          case 2: {
            int raw_mon = getValue(v, raw_mon);
            if (raw_mon != std::numeric_limits<int>::max()) {
              meas_datetime.tm_mon = raw_mon - 1;
              dateupdate = true;
            }
            break;
          }
          case 3: {
            int raw_day = getValue(v, raw_day);
            if (raw_day != std::numeric_limits<int>::max()) {
              meas_datetime.tm_mday = raw_day;
              dateupdate = true;
            }
            break;
          }
          case 4: {
            int raw_hour = getValue(v, raw_hour);
            if (raw_hour != std::numeric_limits<int>::max()) {
              meas_datetime.tm_hour = raw_hour;
              dateupdate = true;
            }
            break;
          }
          case 5: {
            int raw_min = getValue(v, raw_min);
            if (raw_min != std::numeric_limits<int>::max()) {
              meas_datetime.tm_min = raw_min;
              dateupdate = true;
            }
            break;
          }
          case 6: {
            int raw_sec = getValue(v, raw_sec);
            if (raw_sec != std::numeric_limits<int>::max()) {
              meas_datetime.tm_sec = raw_sec;
              dateupdate = true;
            }
            break;
          }
          case 21: { // Time period or displacement
            period_beg = "P";
            period_end = "YT";
            period_update = true;
            break;
          }
          case 22: { // Time period or displacement
            period_beg = "P";
            period_end = "MT";
            period_update = true;
            break;
          }
          case 73:   // Short time period or displacement
          case 23: { // Time period or displacement
            period_beg = "P";
            period_end = "DT";
            period_update = true;
            break;
          }
          case 74: // Short time period or displacement
          case 24: {
            period_beg = "PT";
            period_end = "H";
            period_update = true;
            break;
          }
          case 75:   // Short time period or displacement
          case 25: { // Time period or displacement
            period_beg = "PT";
            period_end = "M";
            period_update = true;
            auto pi = ci;
            pi--;
            // CI: current descriptor, period end(in minutes)
            // PI: previous descriptor, period start(in minutes)
            if (*pi == *ci) {
              start_end_period = true;
            }
            break;
          }
          case 16:   // Short time period or displacement
          case 26: { // Time period or displacement
            period_beg = "PT";
            period_end = "S";
            period_update = true;
            break;
          }
          case 86: // LONG TIME PERIOD OR DISPLACEMENT
          {
            int raw_time_disp = getValue(v, raw_time_disp);
            if (raw_time_disp != std::numeric_limits<int>::max()) {
              time_disp = raw_time_disp;
              dateupdate = true;
              period_beg = "PT";
              period_end = "S";
              period_update = true;
            }
            break;
          }
          }
#pragma GCC diagnostic pop
          if (period_update) {
            bool valid_period = true;
            if (data_category != 2 ||
                (int_data_subcategory < 4 || int_data_subcategory > 7)) {
              int time_period = 0;
              time_period = getValue(v, time_period);
              if (time_period == std::numeric_limits<int>::max()) {
                valid_period = false;
                lb.addLogEntry(LogEntry(
                    "Missing BUFR time period: " + std::to_string(time_period) +
                        ", at: " + v.toString(),
                    LogLevel::WARN, __func__, bufr_id));
              }
              if (valid_period) {
                if ((data_category == 2 && int_data_subcategory == 1) ||
                    start_end_period) {
                  if (!start_end_period)
                    time_period = -time_period;
                  if (period_beg == "PT") {
                    if (period_end == "S") {
                      time_disp += time_period;
                    } else {
                      if (period_end == "M") {
                        time_disp += time_period * 60;
                      } else {
                        if (period_end == "H") {
                          time_disp += time_period * 60 * 60;
                        } else {
                          lb.addLogEntry(LogEntry(
                              "Profile datetime is the start of measure!",
                              LogLevel::WARN, __func__, bufr_id));
                        }
                      }
                    }
                  }
                  if (start_end_period) {
                    auto pi = ci;
                    pi--;
                    if (*pi == *ci) {
                      int time_period_start = 0;
                      time_period_start = getValue(*pi, time_period_start);
                      if (time_period_start !=
                          std::numeric_limits<int>::max()) {
                        time_period = -(time_period - time_period_start);
                      }
                    }
                    start_end_period = false;
                  }
                } else {
                  if (time_period > 0) {
                    time_period = -time_period;
                    lb.addLogEntry(LogEntry("Positive BUFR time period: " +
                                                std::to_string(time_period) +
                                                ", at: " + v.toString(),
                                            LogLevel::WARN, __func__, bufr_id));
                  }
                }
                dateupdate = true;
                std::stringstream ss;
                if (!time_period) {
                  period_beg = "PT";
                  period_end = "S";
                }
                ss << period_beg << -time_period << period_end;
                period_str = ss.str();
              }
            }
            if (valid_period)
              dateupdate = true;
          }
          if (dateupdate) {
            if (v.y() == 86 || (v.y() >= 21 && v.y() <= 26) ||
                (v.y() >= 73 && v.y() <= 75)) {
              time_t meas_time = mktime(&meas_datetime);
              meas_time += time_disp;
              setDateTime(gmtime(&meas_time), subset_message, period_str);
            } else {
              setDateTime(&meas_datetime, subset_message);
            }
          }

          break;
        }
        case 5: // Latitude
        {
          if (v.y() <= 2) {
            lat = getValue(v, lat);
            LogEntry("Set latitude: " + std::to_string(lat), LogLevel::DEBUG,
                     __func__, bufr_id);
            if (!std::isnan(lat)) {
              setLocation(lat, lon, hei, subset_message);
            }
          }
          if (v.y() == 12 || v.y() == 15 || v.y() == 16) {
            double lat_disp = getValue(v, 0.0);
            if (!std::isnan(lat_disp)) {
              updateLocation(lat + lat_disp, "lat", subset_message);
            }
          }

          break;
        }
        case 6: // Longitude
        {
          if (v.y() <= 2) {
            lon = getValue(v, lon);
            LogEntry("Set longitude: " + std::to_string(lon), LogLevel::DEBUG,
                     __func__, bufr_id);
            if (!std::isnan(lon)) {
              setLocation(lat, lon, hei, subset_message);
            }
          }
          if (v.y() == 12 || v.y() == 15 || v.y() == 16) {
            double lon_disp = getValue(v, 0.0);
            if (!std::isnan(lon_disp)) {
              updateLocation(lon + lon_disp, "lon", subset_message);
            }
          }

          break;
        }
        case 7: // Height
        {
          if (v.y() == 1 || v.y() == 2 || v.y() == 7 || v.y() == 30) {
            hei = getValue(v, hei);
          }
          if (v.y() == 10) // Flight level, TODO: conversion?
          {
            hei = getValue(v, hei);
          }
          if (v.y() == 62) // Depth below sea/water surface
          {
            hei = -getValue(v, hei);
          }
          if (!std::isnan(hei)) {
            setLocation(lat, lon, hei, subset_message);
          }
          // 31: // Height of barometer
          // 32: // Height of sensor above ground
          // 33: // Height of sensor above water
          if (v.y() == 31 || v.y() == 32 || v.y() == 33) {
            sensor_level = getValue(v, sensor_level);
            if (getDataCategory() <= 1 && !std::isnan(sensor_level)) {
              sensor_level_active = 2;
            }
          }

          break;
        }
        case 10: // Pressure
        {
          if (v.y() == 4 ||
              v.y() == 51) // PRESSURE, PRESSURE REDUCED TO MEAN SEA LEVEL
          {
            auto ins_msg = addMessage(ci, subset_message, sensor_level_active,
                                      sensor_level, "point");
            if (std::find(ret.begin(), ret.end(), ins_msg) != ret.end()) {
              lb.addLogEntry(LogEntry(
                  "Non uniq value: " + v.toString() +
                      ", skip value, subset: " + std::to_string(subsetnum),
                  LogLevel::WARN, __func__, bufr_id));

            } else {
              ret.push_back(ins_msg);
            }
            // ret.push_back(addMessage(ci, subset_message, sensor_level_active,
            //                        sensor_level, "point"));
          }
          if (v.y() == 9) // Geopotential height, TODO: unit conversion?
          {
            double gpm = getValue(v, 0.0);
            if (!std::isnan(gpm)) {
              updateLocation(gpm, "hei", subset_message);
            }
          }

          break;
        }
        case 11: // Wind
        {
          if (v.y() == 1 || v.y() == 2) // WIND SPEED, WIND DIRECTION
          {
            if (!sensor_level_active && getDataCategory() <= 1) {
              sensor_level_active = 1;
              sensor_level = 10.0;
            }
            ret.push_back(addMessage(ci, subset_message, sensor_level_active,
                                     sensor_level, "point"));
          }

          break;
        }
        case 12: // Temperature
        {
          if (v.y() == 1 || v.y() == 101 || v.y() == 3 || v.y() == 103) {
            if (!sensor_level_active && getDataCategory() <= 1) {
              sensor_level_active = 1;
              sensor_level = 2.0;
            }
            ret.push_back(addMessage(ci, subset_message, sensor_level_active,
                                     sensor_level, "point"));
          }

          break;
        }

        case 13: // Humidity
        {
          if (v.y() == 3) {
            if (!sensor_level_active && getDataCategory() <= 1) {
              sensor_level_active = 1;
              sensor_level = 10.0;
            }
            ret.push_back(addMessage(ci, subset_message, sensor_level_active,
                                     sensor_level, "point"));
          }

          break;
        }

        case 22: // Oceanographic
        {

          if (v.y() == 42 || v.y() == 43 || v.y() == 45) {
            ret.push_back(addMessage(ci, subset_message, sensor_level_active,
                                     sensor_level, "point"));
          }

          break;
        }

        case 31: // Delayed Repetition descriptor, skip
        {
          if (v.x() == 31)
            break;
        }

        break;
        }
        break;
      }
      case 3: {
        switch (v.x()) {
        case 2: {
          switch (v.y()) {
          case 34: // [ 3 02 034 ] (Precipitation past 24 hours
          {
            ++ci;
            double sensor_hei = 0.0;
            if (*ci == DescriptorId(7032, true)) {
              sensor_hei = getValue(*ci, sensor_hei);
              // Height update ???
            }

            ++ci;
            double precip = 0.0;
            if (*ci == DescriptorId(13023, true)) {
              precip = getValue(*ci, precip);
              if (!std::isnan(precip)) {
                time_t start_datetime = 0;
                start_datetime = mktime(&meas_datetime);
                start_datetime -= 60 * 60 * 24;
                period_str = "PT24H";

                ret.push_back(addMessage(ci, subset_message,
                                         sensor_level_active, sensor_level,
                                         "sum", &start_datetime, period_str));
              }
            }

            break;
          }
          case 40: // [ 3 02 040 ] Precipitation measurement
          {
            ++ci;
            double sensor_hei = 0.0;
            if (*ci == DescriptorId(7032, true)) {
              sensor_hei = getValue(*ci, sensor_hei);
              // Height update ???
            }

            ++ci; // [ 1 02 002 ]

            for (int i = 0; i < 2; ++i) {
              ++ci;
              double precip = 0.0;
              int period = 0;
              bool valid_period = true;
              time_t start_datetime = 0;
              if (*ci == DescriptorId(4024, true)) {
                period = getValue(*ci, period);
                if (period == std::numeric_limits<int>::max())
                  valid_period = false;
                if (valid_period) {
                  start_datetime = mktime(&meas_datetime);
                  start_datetime += period * 60 * 60;
                  period_beg = "PT";
                  period_end = "H";
                  std::stringstream ss;
                  ss << period_beg << ((period > 0) ? period : -period)
                     << period_end;
                  period_str = ss.str();
                }
              }

              ++ci;
              if (valid_period && *ci == DescriptorId(13011, true)) {
                precip = getValue(*ci, precip);
                if (!std::isnan(precip)) {
                  ret.push_back(addMessage(ci, subset_message,
                                           sensor_level_active, sensor_level,
                                           "sum", &start_datetime, period_str));
                }
              }
            }

            break;
          }
          case 45: // [ 3 02 045 ] Radiation data (from 1 hour and 24-hour
                   // period )
          {
            time_t start_datetime = 0;
            ++ci;
            int period = 0;
            if (*ci == DescriptorId(4024, true)) {
              period = getValue(*ci, period);
              if (period != std::numeric_limits<int>::max()) {
                start_datetime = mktime(&meas_datetime);
                start_datetime += period * 60 * 60;
                period_beg = "PT";
                period_end = "H";
                std::stringstream ss;
                ss << period_beg << -period << period_end;
                period_str = ss.str();
              }
            }

            ++ci;
            double long_wave = 0.0;
            if (*ci ==
                DescriptorId(14002, true)) // [ 0 14 002 ] LONG-WAVE RADIATION,
                                           // INTEGRATED OVER PERIOD SPECIFIED
            {
              long_wave = getValue(*ci, long_wave);
              if (!std::isnan(long_wave)) {
                ret.push_back(addMessage(ci, subset_message,
                                         sensor_level_active, sensor_level,
                                         "sum", &start_datetime, period_str));
              }
            }

            ++ci;
            double short_wave = 0.0;
            if (*ci ==
                DescriptorId(14004, true)) // [ 0 14 004 ] SHORT-WAVE RADIATION,
                                           // INTEGRATED OVER PERIOD SPECIFIED
            {
              short_wave = getValue(*ci, short_wave);
              if (!std::isnan(short_wave)) {
                ret.push_back(addMessage(ci, subset_message,
                                         sensor_level_active, sensor_level,
                                         "sum", &start_datetime));
              }
            }
            ++ci; // [ 0 14 16 ] NET RADIATION, INTEGRATED OVER PERIOD SPECIFIED
            ++ci; // [ 0 14 28 ] GLOBAL SOLAR RADIATION (HIGH ACCURACY),
                  // INTEGRATED OVER PERIOD SPECIFIED
            ++ci; // [ 0 14 29 ] DIFFUSE SOLAR RADIATION (HIGH ACCURACY),
                  // INTEGRATED OVER PERIOD SPECIFIED
            ++ci; // [ 0 14 30 ] DIRECT SOLAR RADIATION (HIGH ACCURACY),
                  // INTEGRATED OVER PERIOD SPECIFIED

            break;
          }
          }
          break;
        }
        }
        break;
      }
      }
    }
  subset_end:
    subsetnum++;
  }

  return ret;
}

bool ESOHBufr::addDescriptor(
    Descriptor &v, rapidjson::Value &dest,
    rapidjson::Document::AllocatorType &message_allocator) const {

  DescriptorMeta *meta = v.getMeta();

  rapidjson::Value sid_desc;
  std::string sid = meta->name();
  std::transform(sid.begin(), sid.end(), sid.begin(), [](unsigned char c) {
    return (c == ' ' ? c = '_' : std::tolower(c));
  });
  sid_desc.SetString(sid.c_str(), message_allocator);

  rapidjson::Value ssid;

  rapidjson::Value mvalue;
  if (meta->unit() == "Numeric") {
    mvalue.SetUint64(std::stoi(getValue(v, std::string(), false)));
  } else {
    std::string tmp_value = (getValue(v, std::string(), false));
    std::string value = NorBufrIO::strTrim(tmp_value);
    mvalue.SetString(value.c_str(), message_allocator);
  }
  ssid = mvalue;
  dest.AddMember(sid_desc, ssid, message_allocator);

  return true;
}

bool ESOHBufr::addContent(const Descriptor &v, std::string cf_name,
                          char sensor_level_active, double sensor_level,
                          std::string fn, rapidjson::Document &message) const {

  const DescriptorMeta *meta = v.getMeta();
  rapidjson::Document::AllocatorType &message_allocator =
      message.GetAllocator();
  rapidjson::Value &message_properties = message["properties"];
  rapidjson::Value &content = message_properties["content"];

  // id
  std::string id;
  std::ifstream is("/proc/sys/kernel/random/uuid");
  // is >> id;

  message["id"].SetString(id.c_str(), id.length(), message_allocator);
  if (fn.size()) {
    message_properties["function"].SetString(fn.c_str(), message_allocator);
  }

  if (sensor_level_active) {
    std::stringstream ss;
    ss << std::fixed << std::setprecision(1) << sensor_level;
    rapidjson::Value r_level;
    r_level.SetString(ss.str().c_str(), message_allocator);
    // message_properties.AddMember("level", r_level, message_allocator);
    message_properties["level"] = r_level;
  }
  rapidjson::Value mvalue;
  std::string value_str = getValue(v, std::string(), false);
  if (meta->unit() == "Numeric") {
    mvalue.SetUint64(std::stoi(value_str));
  } else {
    mvalue.SetString(value_str.c_str(), message_allocator);
  }
  content["value"] = mvalue;
  content["size"] = value_str.size();
  content["standard_name"].SetString(cf_name.c_str(), message_allocator);

  if (meta->unit() == "CODE TABLE") {
    rapidjson::Value mcode;
    uint64_t cval =
        NorBufrIO::getBitValue(v.startBit(), meta->datawidth(), true,
                               (isCompressed() ? ucbits : bits));
    mcode.SetUint64(cval);
    content.AddMember("code", mcode, message_allocator);
  } else {
    if (meta->unit() != "Numeric" && meta->unit() != "CCITTIA5") {
      rapidjson::Value munit;
      // munit.SetString(meta->unit().c_str(),message_allocator);
      munit.SetString(cf_names[v].second.c_str(), message_allocator);
      content["unit"] = munit;
    }
  }

  return true;
}

bool ESOHBufr::setPlatform(std::string value,
                           rapidjson::Document &message) const {

  rapidjson::Value platform;
  rapidjson::Document::AllocatorType &message_allocator =
      message.GetAllocator();
  rapidjson::Value &message_properties = message["properties"];

  platform.SetString(value.c_str(), message_allocator);
  if (message_properties.HasMember("platform")) {
    message_properties["platform"] = platform;
  } else {
    message_properties.AddMember("platform", platform, message_allocator);
  }
  return true;
}

bool ESOHBufr::setPlatformName(std::string value, rapidjson::Document &message,
                               bool force) const {
  if (NorBufrIO::strTrim(value).size() == 0)
    return false;
  rapidjson::Document::AllocatorType &message_allocator =
      message.GetAllocator();
  rapidjson::Value &message_properties = message["properties"];
  rapidjson::Value platform_name;
  std::string platform_str = NorBufrIO::strTrim(value);
  NorBufrIO::filterStr(platform_str, repl_chars);
  platform_name.SetString(platform_str.c_str(), message_allocator);

  if (message_properties.HasMember("platform_name")) {
    rapidjson::Value &platform_old_value = message_properties["platform_name"];
    std::string platform_old_name = platform_old_value.GetString();
    if (platform_old_name != platform_str) {
      if (force) {
        message_properties["platform_name"].SetString(platform_str.c_str(),
                                                      message_allocator);
      } else {
        message_properties["platform_name"].SetString(
            std::string(platform_old_name + "," + platform_str).c_str(),
            message_allocator);
      }
    }
  } else {
    message_properties.AddMember("platform_name", platform_name,
                                 message_allocator);
  }

  return true;
}

bool ESOHBufr::setLocation(double lat, double lon, double hei,
                           rapidjson::Document &message) const {
  rapidjson::Document::AllocatorType &message_allocator =
      message.GetAllocator();
  rapidjson::Value &geometry = message["geometry"];
  if (!geometry.IsObject()) {
    geometry.SetObject();
    rapidjson::Value geometry_type;
    geometry_type.SetString("Point");
    geometry.AddMember("type", geometry_type, message_allocator);
    rapidjson::Value location(rapidjson::kObjectType);
    location.AddMember("lat", lat, message_allocator);
    location.AddMember("lon", lon, message_allocator);
    location.AddMember("hei", hei, message_allocator);
    geometry.AddMember("coordinates", location, message_allocator);
  } else {
    rapidjson::Value location(rapidjson::kObjectType);
    location.AddMember("lat", lat, message_allocator);
    location.AddMember("lon", lon, message_allocator);
    if (!std::isnan(hei)) {
      location.AddMember("hei", hei, message_allocator);
    }
    geometry["coordinates"] = location;
  }

  return true;
}

bool ESOHBufr::updateLocation(double loc_value, std::string loc_label,
                              rapidjson::Document &message) const {
  // rapidjson::Document::AllocatorType &message_allocator =
  //     message.GetAllocator();
  rapidjson::Value &geometry = message["geometry"];
  rapidjson::Value &coordinates = geometry["coordinates"];
  if (coordinates.HasMember(loc_label.c_str())) {
    coordinates[loc_label.c_str()] = loc_value;
  } else {
    lb.addLogEntry(LogEntry("Location update is not possible", LogLevel::WARN,
                            __func__, bufr_id));
  }
  return true;
}

bool ESOHBufr::setDateTime(struct tm *meas_datetime,
                           rapidjson::Document &message,
                           std::string period_str) const {

  rapidjson::Document::AllocatorType &message_allocator =
      message.GetAllocator();
  rapidjson::Value &properties = message["properties"];
  rapidjson::Value &datetime = properties["datetime"];

  const int date_len = 50;
  char date_str[date_len];
  struct timeval tv = {0, 0};
  tv.tv_sec = mktime(meas_datetime);
  size_t dl = NorBufrIO::strisotime(date_str, date_len, &tv);

  datetime.SetString(date_str, static_cast<rapidjson::SizeType>(dl),
                     message_allocator);

  if (period_str.size()) {
    properties["period"].SetString(period_str.c_str(), message_allocator);
  }

  return true;
}

bool ESOHBufr::setStartDateTime(struct tm *start_meas_datetime,
                                rapidjson::Document &message,
                                std::string period_str) const {
  rapidjson::Document::AllocatorType &message_allocator =
      message.GetAllocator();
  rapidjson::Value &properties = message["properties"];
  rapidjson::Value &datetime = properties["datetime"];
  rapidjson::Value start_datetime;
  rapidjson::Value end_datetime;

  const int date_len = 50;
  char date_str[date_len];
  struct timeval tv = {0, 0};
  tv.tv_sec = mktime(start_meas_datetime);
  size_t dl = NorBufrIO::strisotime(date_str, date_len, &tv);

  start_datetime.SetString(date_str, static_cast<rapidjson::SizeType>(dl),
                           message_allocator);
  // properties.AddMember("start_datetime", start_datetime, message_allocator);

  // datetime.SetString(date_str,static_cast<rapidjson::SizeType>(dl),message_allocator);
  end_datetime.CopyFrom(datetime, message_allocator);
  // properties.RemoveMember("datetime");
  // properties.AddMember("end_datetime", end_datetime, message_allocator);

  if (period_str.size()) {
    properties["period"].SetString(period_str.c_str(), message_allocator);
  }

  return true;
}

std::string ESOHBufr::addMessage(std::list<Descriptor>::const_iterator ci,
                                 rapidjson::Document &message,
                                 char sensor_level_active, double sensor_level,
                                 std::string fn, time_t *start_datetime,
                                 std::string period_str) const {
  std::string ret;

  rapidjson::Document new_message;
  rapidjson::Document::AllocatorType &new_message_allocator =
      new_message.GetAllocator();
  new_message.CopyFrom(message, new_message_allocator);

  if (start_datetime)
    setStartDateTime(gmtime(start_datetime), new_message, period_str);

  addContent(*ci, cf_names[*ci].first, sensor_level_active, sensor_level, fn,
             new_message);

  rapidjson::StringBuffer sb;
  rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(sb);
  new_message.Accept(writer);
  ret = sb.GetString();

  return ret;
}

bool ESOHBufr::setShadowWigos(std::string s) {
  return shadow_wigos.from_string(s);
}

void ESOHBufr::setShadowWigos(const WSI &wsi) { shadow_wigos = wsi; }

WSI ESOHBufr::genShadowWigosId(
    std::list<Descriptor> &s, std::list<Descriptor>::const_iterator &ci) const {
  WSI tmp_id = shadow_wigos;
  std::stringstream ss;
  std::string localv;
  for (std::list<Descriptor>::const_iterator di = s.begin(); di != ci; ++di) {
    if (di->f() == 0 && di->x() == 1) {
      localv = getValue(*di, localv, false);
      if (localv != "MISSING") {
        ss << NorBufrIO::strTrim(localv) << "_";
      }
    }
  }
  if (ss.str().size()) {
    localv = ss.str().substr(0, 16);
    if (localv[localv.size() - 1] == '_') {
      localv.pop_back();
    }
    NorBufrIO::filterStr(localv, repl_chars);
    tmp_id.setWigosLocalId(localv);
  }
  return tmp_id;
}

void ESOHBufr::initTimeInterval() {
  if (const char *env_dynamictime = std::getenv("DYNAMICTIME")) {
    std::string str_dynamictime(env_dynamictime);
    std::transform(str_dynamictime.begin(), str_dynamictime.end(),
                   str_dynamictime.begin(), ::tolower);
    std::istringstream is(str_dynamictime);
    is >> std::boolalpha >> dynamictime;
    lb.addLogEntry(LogEntry("Set Dynamic time:" + dynamictime, LogLevel::DEBUG,
                            __func__, bufr_id));
  }
  if (const char *env_lotime = std::getenv("LOTIME")) {
    lotime = getTimeStamp(env_lotime);
    lb.addLogEntry(LogEntry("Set Lotime:" + std::to_string(lotime),
                            LogLevel::DEBUG, __func__, bufr_id));
  }
  if (const char *env_hitime = std::getenv("HITIME")) {
    hitime = getTimeStamp(env_hitime);
    lb.addLogEntry(LogEntry("Set Hitime:" + std::to_string(hitime),
                            LogLevel::DEBUG, __func__, bufr_id));
  }
}

bool ESOHBufr::timeInInterval(time_t t) const {
  time_t current_time = time(NULL);
  if (dynamictime) {
    return (t > current_time - lotime && t < current_time - hitime);
  } else {
    return (t > lotime && t < hitime);
  }
}

bool ESOHBufr::timeInInterval(struct tm tm) const {
  time_t t = mktime(&tm);
  return timeInInterval(t);
}

int64_t getTimeStamp(const char *env_time) {
  uint64_t ret;
  if (env_time[strlen(env_time) - 1] == 'Z') {
    struct tm tm;
    memset(&tm, 0, sizeof(tm));
    strptime(env_time, "%Y-%m-%dT%H:%M:%SZ", &tm);
    ret = mktime(&tm);
  } else {
    std::istringstream is(env_time);
    is >> ret;
  }
  return ret;
}
