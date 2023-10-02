/*
 * (C) Copyright 2023, Eumetnet
 *
 * This file is part of the E-SOH Norbufr BUFR en/decoder interface
 *
 * Author: istvans@met.no
 *
 */


#ifndef _ESOHBUFR_
#define _ESOHBUFR_

#include <string>
#include <map>

#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"

#include "NorBufr.h"
#include "Oscar.h"


// Default BUFR-CF map
static std::map<DescriptorId,std::pair<std::string, std::string > > cf_names = {
    { DescriptorId(10004,true), { "air_pressure", "Pa" } },
    { DescriptorId(10051,true), { "air_pressure_at_mean_sea_level", "Pa" } },

    { DescriptorId(11001,true), { "wind_from_direction", "degree" } },
    { DescriptorId(11002,true), { "wind_speed", "m s-1" } },

    { DescriptorId(12001,true), { "air_temperature", "K" } },
    { DescriptorId(12004,true), { "air_temperature", "K" } },
    { DescriptorId(12101,true), { "air_temperature", "K" } },
    { DescriptorId(12104,true), { "air_temperature", "K" } },
    { DescriptorId(12003,true), { "dew_point_temperature", "K" } },
    { DescriptorId(12006,true), { "dew_point_temperature", "K" } },
    { DescriptorId(12103,true), { "dew_point_temperature", "K" } },
    { DescriptorId(12106,true), { "dew_point_temperature", "K" } },

    { DescriptorId(13003,true), { "relative_humidity", "1" } },

    { DescriptorId(13011,true), { "precipitation_amount", "kg m-2" } },
    { DescriptorId(13023,true), { "precipitation_amount", "kg m-2" } },

    { DescriptorId(20001,true), { "visibility_in_air", "m" } },

    { DescriptorId(14002,true), { "integral_wrt_time_of_surface_downwelling_longwave_flux_in_air", "W s m-2" } },
    { DescriptorId(14004,true), { "integral_wrt_time_of_surface_downwelling_shortwave_flux_in_air", "W s m-2" } },
    { DescriptorId(14012,true), { "integral_wrt_time_of_surface_net_downward_longwave_flux", "W s m-2" } },
    { DescriptorId(14013,true), { "integral_wrt_time_of_surface_net_downward_shortwave_flux", "W s m-2" } },

    { DescriptorId(22042,true), { "sea_water_temperature", "K"  } },
    { DescriptorId(22043,true), { "sea_water_temperature", "K"  } },
    { DescriptorId(22045,true), { "sea_water_temperature", "K"  } }

};

class ESOHBufr : public NorBufr
{

public:
    ESOHBufr();
    std::string msg() const;
    void setOscar(Oscar *);
private:
    std::string addMessage(std::list<Descriptor>::const_iterator ci, rapidjson::Document & message, time_t * start_datetime = 0 ) const;
    bool addDescriptor(Descriptor & D, rapidjson::Value & dest, rapidjson::Document::AllocatorType &) const;
    bool addContent(const Descriptor & D, std::string cf_name, rapidjson::Document &) const;
    bool setPlatformName(std::string v, rapidjson::Document & message, bool force = true) const;
    bool setLocation(double lat, double lon, double hei, rapidjson::Document &) const;
    bool updateLocation(double loc, int loc_index, rapidjson::Document & message) const;

    bool setDateTime(struct tm *, rapidjson::Document &) const;
    bool setStartDateTime(struct tm *, rapidjson::Document &) const;
    Oscar * oscar;

};


#endif

