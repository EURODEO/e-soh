 /*
 * (C) Copyright 2023, met.no
 *
 * This file is part of the Norbufr BUFR en/decoder
 *
 * Author: istvans@met.no
 *
 */

#ifndef _DESCRIPTOR_H_
#define _DESCRIPTOR_H_

#include <stdint.h>

#include <iostream>
#include <vector>

#include "NorBufrIO.h"


class DescriptorMeta
{
public:
    DescriptorMeta();
    DescriptorMeta(std::string name, std::string unit, int scale, int reference, uint64_t datawidth );

    std::string name() const;
    std::string unit() const;
    int scale() const;
    int reference() const;
    uint64_t datawidth() const;
    uint8_t assocwidth() const;
    void setDatawidth( uint64_t dw );
    void setScale(int sc );
    void setReference(int ref);
    void setName(std::string n);
    void setUnit(std::string n);
    void setAssocwidth(uint8_t);

private:
    std::string name_value;
    std::string unit_value;
    int scale_value;
    int reference_value;
    uint64_t datawidth_value;
    uint8_t assocwidth_value;


    friend bool operator==(const DescriptorMeta &lhs, const DescriptorMeta &rhs);
    friend bool operator!=(const DescriptorMeta &lhs, const DescriptorMeta &rhs);
    friend std::ostream & operator<<(std::ostream & os,  const DescriptorMeta & d);

};

class DescriptorId
{
public:
    DescriptorId();
    DescriptorId(uint8_t F, uint8_t X, uint8_t Y);
    DescriptorId(uint8_t FX, uint8_t Y);
    DescriptorId(int FX, int Y);
    DescriptorId(int F, int X, int Y);
    DescriptorId(int FXY, bool v = false);
    DescriptorId(std::string s);
    uint8_t f() const;
    uint8_t x() const;
    uint8_t y() const;
    int toInt() const;
    std::string toString() const;

    friend std::ostream & operator<<(std::ostream & os,  const DescriptorId & d);
    friend bool operator<(const DescriptorId &lhs, const DescriptorId &rhs);
    friend bool operator==(const DescriptorId &lhs, const DescriptorId &rhs);

private:
    bool fromString(std::string s);

protected:
    uint8_t F;
    uint8_t X;
    uint8_t Y;

};

class Descriptor : public DescriptorId
{
public:
    Descriptor();
    Descriptor(DescriptorId did ,ssize_t sb = 0 );
    DescriptorId getDescriptorId() const;
    void setMeta(DescriptorMeta * dm);
    DescriptorMeta * getMeta();
    const DescriptorMeta * getMeta() const;
    ssize_t startBit() const;
    void setStartBit( ssize_t sb);
    std::ostream & printDetail( std::ostream & os = std::cout ) const;

protected:
    ssize_t startbit;
    DescriptorMeta *Dm;

    friend std::ostream & operator<<(std::ostream & os,  const Descriptor & d);

};

#endif

