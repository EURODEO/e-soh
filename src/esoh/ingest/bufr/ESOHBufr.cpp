/*
 * (C) Copyright 2023, Eumetnet
 *
 * This file is part of the E-SOH Norbufr BUFR en/decoder interface
 *
 * Author: istvans@met.no
 *
 */

#include <time.h>
#include <openssl/sha.h>

#include <sstream>
#include <algorithm>
#include <bitset>
#include <list>

#include "ESOHBufr.h"
#include "WSI.h"

ESOHBufr::ESOHBufr()
{
    oscar = 0;
}

void ESOHBufr::setOscar(Oscar *o)
{
    oscar = o;
}

std::list<std::string> ESOHBufr::msg() const
{

    std::list<std::string> ret;

    rapidjson::Document message;

    const char *message_template = " { \
        \"id\" : \"\", \
        \"version\" : \"v04\", \
        \"type\" : \"Feature\", \
        \"geometry\" : \"null\", \
        \"properties\" : { \
            \"data_id\": \"data_id\", \
            \"metadata_id\": \"metatata_id\", \
            \"datetime\" : \"null\", \
            \"content\" : { \
                \"encoding\": \"utf-8\", \
                \"standard_name\": \"\", \
                \"unit\": \"\", \
                \"size\": 0, \
                \"value\": \"\"} \
             } \
        }";

    if (message.Parse(message_template).HasParseError())
    {
        std::cerr << "ESOH message parsing Error!!!\n";
    }

    rapidjson::Value &properties = message["properties"];

    rapidjson::Document::AllocatorType &message_allocator = message.GetAllocator();

    // pubtime
    rapidjson::Value pubtime;
    {
        time_t pubtime_value = time(0);
        const int date_len = 50;
        char date_str[date_len];
        size_t dl = strftime(date_str, date_len, "%FT%H:%M:%S.000000", gmtime(&pubtime_value));
        pubtime.SetString(date_str, static_cast<rapidjson::SizeType>(dl), message_allocator);
    }
    properties.AddMember("pubtime", pubtime, message_allocator);

    // subsets
    int subsetnum = 0;
    for (auto s : desc)
    {
        double lat = -99999;
        double lon = -99999;
        double hei = -99999;
        std::string platform;
        bool platform_check = false;

        WSI wigos_id;

        rapidjson::Document subset_message;
        subset_message.CopyFrom(message, subset_message.GetAllocator());
        rapidjson::Document::AllocatorType &subset_message_allocator = subset_message.GetAllocator();
        rapidjson::Value &subset_properties = subset_message["properties"];

        struct tm meas_datetime;
        memset(static_cast<void *>(&meas_datetime), 0, sizeof(meas_datetime));

        // for( auto v : s )
        for (std::list<Descriptor>::const_iterator ci = s.begin(); ci != s.end(); ++ci)
        {
            auto v = *ci;
            switch (v.f())
            {
            case 0: // Element Descriptors
            {
                std::string value_str = getValue(v, std::string(), false);
                if (value_str == "MISSING")
                    break;

                if (v.x() > 10 && !platform_check)
                {
                    // Check station_id at OSCAR
                    platform_check = true;
                    if (wigos_id.getWigosLocalId().size())
                    {
                        std::string wigos_oscar = oscar->findWigosId(wigos_id);
                        if (wigos_oscar.size())
                        {
                            if (wigos_oscar != wigos_id.to_string())
                            {
                                wigos_id = WSI(wigos_oscar);
                            }
                            const rapidjson::Value &st_value = oscar->findStation(wigos_id);
                            if (st_value.HasMember("name"))
                            {
                                setPlatformName(std::string(st_value["name"].GetString()), subset_message, true);
                            }
                        }
                    }
                }

                switch (v.x())
                {
                case 1: // platform
                {
                    bool skip_platform = true;
                    switch (v.y())
                    {
                    // WMO block and station ID
                    case 1:
                    {
                        int wmo_block = 0;
                        wmo_block = getValue(v, wmo_block);
                        auto nexti = ci;
                        ++nexti;
                        int wmo_station = 0;
                        // Is next the WMO station number?
                        if (*nexti == DescriptorId(1002, true))
                        {
                            wmo_station = getValue(*nexti, wmo_block);
                            ++ci;
                        }
                        wigos_id.setWmoId(wmo_block * 1000 + wmo_station);
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
                        setPlatformName(value_str, subset_message, false);
                        /*
                        if( NorBufrIO::strTrim(value_str).size() == 0 ) break;
                        rapidjson::Value platform_name;
                        platform_name.SetString(NorBufrIO::strTrim(value_str).c_str(),subset_message_allocator);
                        if( subset_properties.HasMember("platform_name") )
                        {
                            rapidjson::Value & platform_old_value = subset_properties["platform_name"];
                            std::string platform_old_name = platform_old_value.GetString();
                            subset_properties["platform_name"].SetString(std::string(platform_old_name + "," + value_str).c_str(),subset_message_allocator);
                        }
                        else
                        {
                            subset_properties.AddMember("platform_name",platform_name,subset_message_allocator);
                        }
                        */

                        break;
                    }
                    case 101: // STATE IDENTIFIER
                    {
                        int bufr_state_id = 0;
                        bufr_state_id = getValue(v, bufr_state_id);
                        int wigos_state_id = 0;
                        for (auto cc : country_codes)
                        {
                            if (cc.bufr_code == bufr_state_id)
                            {
                                wigos_state_id = cc.iso_code;
                                break;
                            }
                        }
                        wigos_id.setWigosIssuerId(wigos_state_id);
                        skip_platform = false;
                        break;
                    }
                    case 102: // NATIONAL STATION NUMBER
                    {
                        if (wigos_id.getWigosLocalId().size() == 0)
                        {
                            wigos_id.setWigosLocalId(value_str);
                        }
                        skip_platform = false;
                        break;
                    }
                    case 125:
                    {
                        int wig_ser = 0;
                        wig_ser = getValue(v, wig_ser);
                        wigos_id.setWigosIdSeries(wig_ser);
                        skip_platform = false;
                        break;
                    }
                    case 126:
                    {
                        int wig_iss_id = 0;
                        wig_iss_id = getValue(v, wig_iss_id);
                        wigos_id.setWigosIssuerId(wig_iss_id);
                        skip_platform = false;
                        break;
                    }
                    case 127:
                    {
                        int wig_iss_num = 0;
                        wig_iss_num = getValue(v, wig_iss_num);
                        wigos_id.setWigosIssueNum(wig_iss_num);
                        skip_platform = false;
                        break;
                    }
                    case 128:
                    {
                        // Workaroung USA wigos local identifier
                        std::string missing_wigos = "01101010000110101000011010100001101010000110101000011010100001101010000110101000011010100001101010000110101000011010100001101010";
                        for (int i = 0; i < 16; i++)
                        {
                            std::bitset<8> bs(value_str[i]);
                            if (bs.to_string<char, std::string::traits_type, std::string::allocator_type>() != missing_wigos.substr(i * 8, 8))
                            {
                                skip_platform = false;
                                break;
                            }
                        }
                        if (skip_platform)
                            break;
                        wigos_id.setWigosLocalId(value_str);
                    }
                    }

                    if (!skip_platform)
                    {
                        rapidjson::Value platform;
                        platform.SetString(wigos_id.to_string().c_str(), subset_message_allocator);
                        if (subset_properties.HasMember("platform"))
                        {
                            subset_properties["platform"] = platform;
                        }
                        else
                        {
                            subset_properties.AddMember("platform", platform, subset_message_allocator);
                        }
                    }

                    break;
                }
                case 2:
                {

                    break;
                }
                case 4: // datetime
                {
                    bool dateupdate = false;
                    int time_disp = 0;
                    switch (v.y())
                    {
                    case 1:
                    {
                        meas_datetime.tm_year = getValue(v, meas_datetime.tm_year) - 1900;
                        dateupdate = true;
                        break;
                    }
                    case 2:
                    {
                        meas_datetime.tm_mon = getValue(v, meas_datetime.tm_mon) - 1;
                        dateupdate = true;
                        break;
                    }
                    case 3:
                    {
                        meas_datetime.tm_mday = getValue(v, meas_datetime.tm_mday);
                        dateupdate = true;
                        break;
                    }
                    case 4:
                    {
                        meas_datetime.tm_hour = getValue(v, meas_datetime.tm_hour);
                        dateupdate = true;
                        break;
                    }
                    case 5:
                    {
                        meas_datetime.tm_min = getValue(v, meas_datetime.tm_min);
                        dateupdate = true;
                        break;
                    }
                    case 6:
                    {
                        meas_datetime.tm_sec = getValue(v, meas_datetime.tm_sec);
                        dateupdate = true;
                        break;
                    }
                    case 86: // LONG TIME PERIOD OR DISPLACEMENT
                    {
                        time_disp = getValue(v, meas_datetime.tm_sec);
                        dateupdate = true;
                        break;
                    }
                    }

                    if (dateupdate)
                    {
                        if (v.y() == 86)
                        {
                            time_t meas_time = mktime(&meas_datetime);
                            meas_time += time_disp;
                            setDateTime(gmtime(&meas_time), subset_message);
                        }
                        else
                        {
                            setDateTime(&meas_datetime, subset_message);
                        }
                    }

                    break;
                }
                case 5: // Latitude
                {
                    if (v.y() <= 2)
                    {
                        lat = getValue(v, lat);
                        setLocation(lat, lon, hei, subset_message);
                    }
                    if (v.y() == 12 || v.y() == 15 || v.y() == 16)
                    {
                        double lat_disp = getValue(v, 0.0);
                        updateLocation(lat + lat_disp, 0, subset_message);
                    }

                    break;
                }
                case 6: // Longitude
                {
                    if (v.y() <= 2)
                    {
                        lon = getValue(v, lon);
                        setLocation(lat, lon, hei, subset_message);
                    }
                    if (v.y() == 12 || v.y() == 15 || v.y() == 16)
                    {
                        double lon_disp = getValue(v, 0.0);
                        updateLocation(lon + lon_disp, 1, subset_message);
                    }

                    break;
                }
                case 7: // Height
                {
                    if (v.y() == 1 || v.y() == 2 || v.y() == 7 || v.y() == 30)
                    {
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
                    setLocation(lat, lon, hei, subset_message);

                    break;
                }
                case 10: // Pressure
                {
                    if (v.y() == 4 || v.y() == 51) // PRESSURE, PRESSURE REDUCED TO MEAN SEA LEVEL
                    {
                        ret.push_back(addMessage(ci, subset_message));
                    }
                    if (v.y() == 9) // Geopotential height, TODO: unit conversion?
                    {
                        double gpm = getValue(v, 0.0);
                        updateLocation(gpm, 2, subset_message);
                    }

                    break;
                }
                case 11: // Wind
                {
                    if (v.y() == 1 || v.y() == 2) // WIND SPEED, WIND DIRECTION
                    {
                        ret.push_back(addMessage(ci, subset_message));
                    }

                    break;
                }
                case 12: // Temperature
                {
                    if (v.y() == 1 || v.y() == 101 || v.y() == 3 || v.y() == 103)
                    {
                        ret.push_back(addMessage(ci, subset_message));
                    }

                    break;
                }

                case 13: // Humidity
                {
                    if (v.y() == 3)
                    {
                        ret.push_back(addMessage(ci, subset_message));
                    }

                    break;
                }

                case 22: // Oceanographic
                {

                    if (v.y() == 42 || v.y() == 43 || v.y() == 45)
                    {
                        ret.push_back(addMessage(ci, subset_message));
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
            case 3:
            {
                switch (v.x())
                {
                case 2:
                {
                    switch (v.y())
                    {
                    case 34: // [ 3 02 034 ] (Precipitation past 24 hours
                    {
                        ++ci;
                        double sensor_hei = 0.0;
                        if (*ci == DescriptorId(7032, true))
                        {
                            sensor_hei = getValue(*ci, sensor_hei);
                            // Height update ???
                        }

                        ++ci;
                        double precip = 0.0;
                        if (*ci == DescriptorId(13023, true))
                        {
                            precip = getValue(*ci, precip);
                            if (precip != std::numeric_limits<uint64_t>::max())
                            {
                                time_t start_datetime = 0;
                                start_datetime = mktime(&meas_datetime);
                                start_datetime -= 60 * 60 * 24;

                                ret.push_back(addMessage(ci, subset_message, &start_datetime));
                            }
                        }

                        break;
                    }
                    case 40: // [ 3 02 040 ] Precipitation measurement
                    {
                        ++ci;
                        double sensor_hei = 0.0;
                        if (*ci == DescriptorId(7032, true))
                        {
                            sensor_hei = getValue(*ci, sensor_hei);
                            // Height update ???
                        }

                        ++ci; // [ 1 02 002 ]

                        for (int i = 0; i < 2; ++i)
                        {
                            ++ci;
                            double precip = 0.0;
                            int period = 0;
                            time_t start_datetime = 0;
                            if (*ci == DescriptorId(4024, true))
                            {
                                period = getValue(*ci, period);
                                start_datetime = mktime(&meas_datetime);
                                start_datetime += period * 60 * 60;
                            }

                            ++ci;
                            if (*ci == DescriptorId(13011, true))
                            {
                                precip = getValue(*ci, precip);
                                if (precip != std::numeric_limits<uint64_t>::max())
                                {
                                    ret.push_back(addMessage(ci, subset_message, &start_datetime));
                                }
                            }
                        }

                        break;
                    }
                    case 45: // [ 3 02 045 ] Radiation data (from 1 hour and 24-hour period )
                    {
                        time_t start_datetime = 0;
                        ++ci;
                        int period = 0;
                        if (*ci == DescriptorId(4024, true))
                        {
                            period = getValue(*ci, period);
                            start_datetime = mktime(&meas_datetime);
                            start_datetime += period * 60 * 60;
                        }

                        ++ci;
                        double long_wave = 0.0;
                        if (*ci == DescriptorId(14002, true)) // [ 0 14 002 ] LONG-WAVE RADIATION, INTEGRATED OVER PERIOD SPECIFIED
                        {
                            long_wave = getValue(*ci, long_wave);
                            if (long_wave != std::numeric_limits<uint64_t>::max())
                            {
                                ret.push_back(addMessage(ci, subset_message, &start_datetime));
                            }
                        }

                        ++ci;
                        double short_wave = 0.0;
                        if (*ci == DescriptorId(14004, true)) // [ 0 14 004 ] SHORT-WAVE RADIATION, INTEGRATED OVER PERIOD SPECIFIED
                        {
                            short_wave = getValue(*ci, short_wave);
                            if (short_wave != std::numeric_limits<uint64_t>::max())
                            {
                                ret.push_back(addMessage(ci, subset_message, &start_datetime));
                            }
                        }
                        ++ci; // [ 0 14 16 ] NET RADIATION, INTEGRATED OVER PERIOD SPECIFIED
                        ++ci; // [ 0 14 28 ] GLOBAL SOLAR RADIATION (HIGH ACCURACY), INTEGRATED OVER PERIOD SPECIFIED
                        ++ci; // [ 0 14 29 ] DIFFUSE SOLAR RADIATION (HIGH ACCURACY), INTEGRATED OVER PERIOD SPECIFIED
                        ++ci; // [ 0 14 30 ] DIRECT SOLAR RADIATION (HIGH ACCURACY), INTEGRATED OVER PERIOD SPECIFIED

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
        subsetnum++;
    }

    return ret;
}

bool ESOHBufr::addDescriptor(Descriptor &v, rapidjson::Value &dest, rapidjson::Document::AllocatorType &message_allocator) const
{

    DescriptorMeta *meta = v.getMeta();

    rapidjson::Value sid_desc;
    std::string sid = meta->name();
    std::transform(sid.begin(), sid.end(), sid.begin(), [](unsigned char c)
                   { return (c == ' ' ? c = '_' : std::tolower(c)); });
    sid_desc.SetString(sid.c_str(), message_allocator);

    rapidjson::Value ssid;

    rapidjson::Value mvalue;
    if (meta->unit() == "Numeric")
    {
        mvalue.SetUint64(std::stoi(getValue(v, std::string(), false)));
    }
    else
    {
        std::string tmp_value = (getValue(v, std::string(), false));
        std::string value = NorBufrIO::strTrim(tmp_value);
        mvalue.SetString(value.c_str(), message_allocator);
    }
    ssid = mvalue;
    dest.AddMember(sid_desc, ssid, message_allocator);

    return true;
}

bool ESOHBufr::addContent(const Descriptor &v, std::string cf_name, rapidjson::Document &message) const
{

    const DescriptorMeta *meta = v.getMeta();
    rapidjson::Document::AllocatorType &message_allocator = message.GetAllocator();
    rapidjson::Value &message_properties = message["properties"];
    rapidjson::Value &content = message_properties["content"];

    // id
    std::string id;
    std::ifstream is("/proc/sys/kernel/random/uuid");
    is >> id;

    message["id"].SetString(id.c_str(), id.length(), message_allocator);

    rapidjson::Value mvalue;
    std::string value_str = getValue(v, std::string(), false);
    if (meta->unit() == "Numeric")
    {
        mvalue.SetUint64(std::stoi(value_str));
    }
    else
    {
        mvalue.SetString(value_str.c_str(), message_allocator);
    }
    content["value"] = mvalue;
    content["size"] = value_str.size();
    content["standard_name"].SetString(cf_name.c_str(), message_allocator);

    if (meta->unit() == "CODE TABLE")
    {
        rapidjson::Value mcode;
        uint64_t cval = NorBufrIO::getBitValue(v.startBit(), meta->datawidth(), true, (isCompressed() ? ucbits : bits));
        mcode.SetUint64(cval);
        content.AddMember("code", mcode, message_allocator);
    }
    else
    {
        if (meta->unit() != "Numeric" && meta->unit() != "CCITTIA5")
        {
            rapidjson::Value munit;
            // munit.SetString(meta->unit().c_str(),message_allocator);
            munit.SetString(cf_names[v].second.c_str(), message_allocator);
            content["unit"] = munit;
        }
    }

    return true;
}

bool ESOHBufr::setPlatformName(std::string value, rapidjson::Document &message, bool force) const
{
    if (NorBufrIO::strTrim(value).size() == 0)
        return false;
    rapidjson::Document::AllocatorType &message_allocator = message.GetAllocator();
    rapidjson::Value &message_properties = message["properties"];
    rapidjson::Value platform_name;
    platform_name.SetString(NorBufrIO::strTrim(value).c_str(), message_allocator);

    if (message_properties.HasMember("platform_name"))
    {
        rapidjson::Value &platform_old_value = message_properties["platform_name"];
        std::string platform_old_name = platform_old_value.GetString();
        if (platform_old_name != value)
        {
            if (force)
            {
                message_properties["platform_name"].SetString(value.c_str(), message_allocator);
            }
            else
            {
                message_properties["platform_name"].SetString(std::string(platform_old_name + "," + value).c_str(), message_allocator);
            }
        }
    }
    else
    {
        message_properties.AddMember("platform_name", platform_name, message_allocator);
    }

    return true;
}

bool ESOHBufr::setLocation(double lat, double lon, double hei, rapidjson::Document &message) const
{
    rapidjson::Document::AllocatorType &message_allocator = message.GetAllocator();
    rapidjson::Value &geometry = message["geometry"];
    if (!geometry.IsObject())
    {
        geometry.SetObject();
        rapidjson::Value geometry_type;
        geometry_type.SetString("Point");
        geometry.AddMember("type", geometry_type, message_allocator);
        rapidjson::Value location(rapidjson::kArrayType);
        location.PushBack(lat, message_allocator);
        location.PushBack(lon, message_allocator);
        location.PushBack(hei, message_allocator);
        geometry.AddMember("coordinates", location, message_allocator);
    }
    else
    {
        rapidjson::Value location(rapidjson::kArrayType);
        location.PushBack(lat, message_allocator);
        location.PushBack(lon, message_allocator);
        if (hei > -99990)
            location.PushBack(hei, message_allocator);
        geometry["coordinates"] = location;
    }

    return true;
}

bool ESOHBufr::updateLocation(double loc_value, int loc_index, rapidjson::Document &message) const
{
    rapidjson::Document::AllocatorType &message_allocator = message.GetAllocator();
    rapidjson::Value &geometry = message["geometry"];
    rapidjson::Value &coordinates = geometry["coordinates"];
    if (coordinates.Size() <= static_cast<rapidjson::SizeType>(loc_index))
    {
        geometry["coordinates"].PushBack(loc_value, message_allocator);
    }
    else
    {
        coordinates[loc_index] = loc_value;
    }
    return true;
}

bool ESOHBufr::setDateTime(struct tm *meas_datetime, rapidjson::Document &message) const
{

    rapidjson::Document::AllocatorType &message_allocator = message.GetAllocator();
    rapidjson::Value &properties = message["properties"];
    rapidjson::Value &datetime = properties["datetime"];

    const int date_len = 50;
    char date_str[date_len];
    size_t dl = strftime(date_str, date_len, "%FT%H:%M:%S.000000", meas_datetime);

    datetime.SetString(date_str, static_cast<rapidjson::SizeType>(dl), message_allocator);

    return true;
}

bool ESOHBufr::setStartDateTime(struct tm *start_meas_datetime, rapidjson::Document &message) const
{
    rapidjson::Document::AllocatorType &message_allocator = message.GetAllocator();
    rapidjson::Value &properties = message["properties"];
    rapidjson::Value &datetime = properties["datetime"];
    rapidjson::Value start_datetime;
    rapidjson::Value end_datetime;

    const int date_len = 50;
    char date_str[date_len];
    size_t dl = strftime(date_str, date_len, "%FT%H:%M:%S.000000", start_meas_datetime);

    start_datetime.SetString(date_str, static_cast<rapidjson::SizeType>(dl), message_allocator);
    properties.AddMember("start_datetime", start_datetime, message_allocator);

    // datetime.SetString(date_str,static_cast<rapidjson::SizeType>(dl),message_allocator);
    end_datetime.CopyFrom(datetime, message_allocator);
    properties.RemoveMember("datetime");
    properties.AddMember("end_datetime", end_datetime, message_allocator);

    return true;
}

std::string ESOHBufr::addMessage(std::list<Descriptor>::const_iterator ci, rapidjson::Document &message, time_t *start_datetime) const
{
    std::string ret;

    rapidjson::Document new_message;
    rapidjson::Document::AllocatorType &new_message_allocator = new_message.GetAllocator();
    new_message.CopyFrom(message, new_message_allocator);

    if (start_datetime)
        setStartDateTime(gmtime(start_datetime), new_message);

    addContent(*ci, cf_names[*ci].first, new_message);

    rapidjson::StringBuffer sb;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(sb);
    new_message.Accept(writer);
    ret = sb.GetString();

    return ret;
}
