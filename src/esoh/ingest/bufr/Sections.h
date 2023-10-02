 /*
 * (C) Copyright 2023, met.no
 *
 * This file is part of the Norbufr BUFR en/decoder
 *
 * Author: istvans@met.no
 *
 */

#ifndef _SECTIONS_H
#define _SECTIONS_H

#include <sys/types.h>
#include <stdint.h>
#include <time.h>

#include <fstream>
#include <vector>
#include <list>

#include "Descriptor.h"

class SectionBase
{
public:
    SectionBase();
    bool fromBuffer(uint8_t * buffer, int size);
    void clear();
    ssize_t length() const;

protected:
    uint8_t *   buffer;
    ssize_t     len;
    uint8_t     zero; // zero, edition num
};

class Section1 : public SectionBase
{
public:
    Section1();
    bool fromBuffer(uint8_t * buffer, int size, uint8_t edition = 4);

    bool optSection() const;
    int getMasterTable() const;
    int getCentre() const;
    int getSubCentre() const;
    int getUpdateSeqNum() const;
    int getOptionalSelection() const;
    int getDataCategory() const;
    int getDataSubCategory() const;
    int getLocalDataSubCategory() const;
    int getVersionMaster() const;
    int getVersionLocal() const;


protected:
    void clear();

    uint8_t     master_table;
    uint16_t    centre;
    uint16_t    subcentre;
    uint8_t     upd_seq_num;
    uint8_t     optional_section;
    uint8_t     data_category;
    uint8_t     int_data_subcategory;
    uint8_t     local_data_subcategory;
    uint8_t     version_master;
    uint8_t     version_local;
    struct tm   bufr_time;
    std::vector<uint8_t> local_data;

    friend std::ostream & operator<<( std::ostream & is, Section1 & sec);

};

class Section2 : public SectionBase
{
public:
    Section2();
    bool fromBuffer(uint8_t * buffer, int size);

protected:
    void clear();
    std::vector<uint8_t> local_data;

    friend std::ostream & operator<<( std::ostream & os, Section2 & sec);

};

class Section3 : public SectionBase
{
public:
    Section3();
    bool fromBuffer(uint8_t * buffer, int size);

    bool isObserved() const;
    bool isCompressed() const;
    uint16_t subsetNum() const;

protected:
    void clear();
    uint16_t    subsets;
    uint8_t     obs_comp;

    std::list<DescriptorId> sec3_desc;

    friend std::ostream & operator<<( std::ostream & os, Section3 & sec);

};


class Section4 : public SectionBase
{
public:
    Section4();
    bool fromBuffer(uint8_t * buffer, int size);
    uint64_t bitSize() const;

protected:
    void clear();

    std::vector<bool> bits;

    friend std::ostream & operator<<( std::ostream & os, Section4 & sec);

};


#endif

