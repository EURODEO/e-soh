 /*
 * (C) Copyright 2023, met.no
 *
 * This file is part of the Norbufr BUFR en/decoder
 *
 * Author: istvans@met.no
 *
 */

#include <iostream>
#include <bitset>

#include "NorBufrIO.h"


uint64_t NorBufrIO::readBytes( std::istream & is, ssize_t size )
{
    uint64_t j = 0;
    uint64_t ret = is.get();
    for( int i=1; i<size; ++i )
    {
        j = is.get();
        ret *= 0x100;
        ret += j;
    }
    return ret;
}

unsigned long NorBufrIO::findBytes( std::ifstream & is, const char * seq, unsigned int size)
{
    unsigned long j=0;
    char c;
    unsigned long position=is.tellg();
    while ( j<size && is.get(c) )
    {
        if( c == seq[j] )
        {
          ++j;
        }
        else
        {
            if( c == seq[0] )
            {
                j=1;
            }
            else
            {
                j=0;
            }
        }
        ++position;
    }
    return (j<size?ULONG_MAX:position-size);
}

unsigned long NorBufrIO::getBytes( uint8_t * buffer, int size)
{
    unsigned long ret = buffer[0];
    if( size > 8 )
    {
        std::cerr << "::getBytes: ERROR, size: " << size << "\n";
    }

    for( int i=1; i<size; ++i)
    {
        ret <<= 8;
        ret |= buffer[i];
    }

    return ret;
}

std::vector<bool> NorBufrIO::valueToBitVec(uint64_t value, int datawidth)
{
    std::vector<bool> ret;
    std::bitset<64> bs(value);
    for(int i = datawidth-1; i>=0; --i)
    {
        ret.push_back(bs[i]);
    }

    return ret;
}

uint64_t NorBufrIO::getBitValue(const uint64_t startbit, const int datawidth, const bool missing_mask,  const std::vector<bool> & bits )
{
    bool missing = true;
    uint64_t ret = 0;
    if( startbit+datawidth <= bits.size() )
    {
        for(int i=0; i < datawidth; ++i)
        {
            ret *= 2;
            ret += bits[startbit+i];
            if( missing && bits[startbit+i] == 0 ) missing = false;
        }
    }
    if( missing && missing_mask && datawidth > 1 ) return ULONG_MAX;

    return ret;
}

std::string NorBufrIO::getBitStrValue(const uint64_t startbit, const int datawidth, const std::vector<bool> & bits)
{
    std::string ret;
    for(int i=0; i < datawidth; ++i)
    {
        if( startbit+datawidth > bits.size() || bits[startbit+i] ) ret += "1";
        else ret += "0";
    }
    return ret;
}

std::string NorBufrIO::getBitStr(const uint64_t startbit, const int datawidth, const std::vector<bool> & bits)
{
    std::string ret;
    for(int i=0; i < datawidth; i+=8)
    {
        char c = NorBufrIO::getBitValue(startbit+i,8,true,bits);
        ret += std::string(1,c);
    }
    return ret;


}

std::vector<bool> NorBufrIO::getBitVec(const uint64_t startbit, const int datawidth, const std::vector<bool> & bits)
{
    std::vector<bool> ret;
    for(int i=0; i<datawidth; ++i)
    {
        if( startbit+datawidth < bits.size() ) ret.push_back(bits[startbit+i]);
    }
    return ret;
}

std::string NorBufrIO::strTrim(const std::string s)
{
    const auto str_begin = s.find_first_not_of(' ');
    std::string ret;
    if( str_begin != std::string::npos )
    {
        const auto str_end = s.find_last_not_of(' ');
        const auto str_range = str_end - str_begin + 1;
        ret = s.substr(str_begin,str_range);
    }
    return ret;
}

