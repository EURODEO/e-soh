/*
 * (C) Copyright 2023, Eumetnet
 *
 * This file is part of the E-SOH Norbufr BUFR en/decoder interface
 *
 * Author: istvans@met.no
 *
 */

/* Wigos Station ID */

#ifndef _WSI_H_
#define _WSI_H_

#include <stdint.h>

#include <string>

#include "CountryCodes.h"

class WSI
{
public:
    WSI();
    WSI(std::string);
    WSI(const char *);
    bool from_string(std::string s);
    void setWigosIdSeries(int);
    void setWigosIssuerId(uint16_t);
    void setWigosIssueNum(uint16_t);
    void setWigosLocalId(std::string);
    void setWmoId(int);
    std::string to_string() const;
    int getWigosIdSeries() const;
    uint16_t getWigosIssuerId() const;
    uint16_t getWigosIssueNum() const;
    std::string getWigosLocalId() const;

    friend std::ostream & operator<<(std::ostream & os,  const WSI & w);
    friend bool operator<(const WSI &lhs, const WSI &rhs);
    friend bool operator==(const WSI &lhs, const WSI &rhs);


private:
    int wigos_id_series;
    uint16_t wigos_issuer_id;
    uint16_t wigos_issue_num;
    std::string wigos_local_id;
};




#endif

