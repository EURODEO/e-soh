/*
 * (C) Copyright 2023, met.no
 *
 * This file is part of the Norbufr BUFR en/decoder
 *
 * Author: istvans@met.no
 *
 */

#include <iomanip>

#include "Descriptor.h"

#define DESCDEBUG

DescriptorId::DescriptorId() { F = X = Y = 0; }

DescriptorId::DescriptorId(uint8_t _F, uint8_t _X, uint8_t _Y)
    : F(_F & 0x3), X(_X & 0x3f), Y(_Y) {
#ifdef DESCDEBUG
  if (_F != F || _X != X) {
    std::cerr << "Warinig, truncated descriptor: F[" << static_cast<int>(_F)
              << "=>" << static_cast<int>(F) << "] X[" << static_cast<int>(_X)
              << "=>" << static_cast<int>(X) << "]\n";
  }
#endif
}

DescriptorId::DescriptorId(uint8_t _FX, uint8_t _Y)
    : F(_FX >> 6), X(_FX & 0x3f), Y(_Y) {}

DescriptorId::DescriptorId(int _FX, int _Y)
    : F((_FX >> 6) & 0x3), X(_FX & 0x3f), Y(_Y & 0xff) {
#ifdef DESCDEBUG
  int t = F;
  t <<= 6;
  t |= X;
  if (Y != _Y || t != static_cast<unsigned char>(_FX)) {
    std::cerr << "?Warinig, truncated descriptor: FX[" << _FX
              << " F=" << static_cast<int>(F) << " X=" << static_cast<int>(X)
              << "] Y[" << _Y << " Y=" << static_cast<int>(Y) << "]\n";
  }
#endif
}

DescriptorId::DescriptorId(int _F, int _X, int _Y)
    : F(_F & 0x3), X(_X & 0x3f), Y(_Y & 0xff) {
#ifdef DESCDEBUG
  if (_F != F || _X != X || _Y != Y) {
    std::cerr << "Warinig, truncated descriptor: F[" << _F << "=>"
              << static_cast<int>(F) << "] X[" << _X << "=>"
              << static_cast<int>(X) << "] Y[" << _Y << "=>"
              << static_cast<int>(Y) << "]\n";
  }
#endif
}

DescriptorId::DescriptorId(int _FXY, bool visible) {
  // convert int as a visible string, int(102143) => F=1,X=02,Y=143
  if (visible) {
    fromString(std::to_string(_FXY));
  } else {
    F = (_FXY >> 14) & 0x3;
    X = ((_FXY >> 8) & 0x3f);
    Y = (_FXY & 0xff);

#ifdef DESCDEBUG
    int t = F;
    t <<= 6;
    t |= X;
    t <<= 8;
    t |= Y;
    if (t != _FXY) {
      std::cerr << "Warinig, truncated descriptor: FXY[" << _FXY
                << " F=" << std::setfill('0') << static_cast<int>(F)
                << " X=" << std::setw(2) << static_cast<int>(X)
                << " Y=" << std::setw(3) << static_cast<int>(Y) << "]\n";
    }
#endif
  }
}

DescriptorId::DescriptorId(std::string s) { fromString(s); }

bool DescriptorId::fromString(std::string s) {
  // remove_if(s.begin(),s.end(),isspace);
  if (s.length() < 6) {
    std::string ss;
    for (unsigned int i = 0; i < 6 - s.length(); ++i) {
      ss += "0";
    }
    s = ss + s;
  }

  std::string fs(s, 0, 1);
  std::string xs(s, 1, 2);
  std::string ys(s, 3, 3);

  F = stoi(fs) & 0x03;
  X = stoi(xs) & 0x3f;
  Y = stoi(ys) & 0xff;

#ifdef DESCDEBUG
  if (stoi(fs) != F || stoi(xs) != X || stoi(ys) != Y) {
    std::cerr << "Warinig, truncated descriptor: " << s << " => "
              << std::setfill('0') << static_cast<int>(F) << std::setw(2)
              << static_cast<int>(X) << std::setw(3) << static_cast<int>(Y)
              << "\n";
    return false;
  }
#endif

  return true;
}

uint8_t DescriptorId::f() const { return F; }

uint8_t DescriptorId::x() const { return X; }

uint8_t DescriptorId::y() const { return Y; }

int DescriptorId::toInt() const {
  int ret = F * 100000 + X * 1000 + Y;
  return ret;
}

std::string DescriptorId::toString() const {
  std::stringstream ss;
  ss << toInt();
  return ss.str();
}

std::ostream &operator<<(std::ostream &os, const DescriptorId &d) {
  os << std::setw(1) << "[ " << std::setfill('0') << static_cast<int>(d.F)
     << " " << std::setw(2) << std::setfill('0') << static_cast<int>(d.X) << " "
     << std::setw(3) << std::setfill('0') << static_cast<int>(d.Y) << " ]";

  return os;
}

bool operator==(const DescriptorId &lhs, const DescriptorId &rhs) {
  return ((lhs.F == rhs.F) && (lhs.X == rhs.X) && (lhs.Y == rhs.Y));
}

bool operator<(const DescriptorId &lhs, const DescriptorId &rhs) {
  if (lhs.F < rhs.F)
    return true;
  if (lhs.F > rhs.F)
    return false;
  if (lhs.X < rhs.X)
    return true;
  if (lhs.X > rhs.X)
    return false;
  if (lhs.Y < rhs.Y)
    return true;

  return false;
}

bool operator<=(const DescriptorId &lhs, const DescriptorId &rhs) {
  return ((lhs == rhs) || (lhs < rhs));
}

bool operator>(const DescriptorId &lhs, const DescriptorId &rhs) {
  return (!(lhs <= rhs));
}

bool operator>=(const DescriptorId &lhs, const DescriptorId &rhs) {
  return ((lhs == rhs) || !(lhs < rhs));
}

DescriptorMeta::DescriptorMeta() {
  name_value = "";
  unit_value = "";
  scale_value = 0;
  reference_value = 0;
  datawidth_value = 0;
  assocwidth_value = 0;
}

DescriptorMeta::DescriptorMeta(std::string _name, std::string _unit, int _scale,
                               int _reference, uint64_t _datawidth)
    : name_value(_name), unit_value(_unit), scale_value(_scale),
      reference_value(_reference), datawidth_value(_datawidth) {
  if (unit_value == "Code table")
    unit_value = "CODE TABLE";
  else if (unit_value == "Flag table")
    unit_value = "CODE TABLE";
  else if (unit_value == "CCITT IA5")
    unit_value = "CCITTIA5";
  assocwidth_value = 0;
}

std::string DescriptorMeta::name() const { return name_value; }

std::string DescriptorMeta::unit() const { return unit_value; }

int DescriptorMeta::scale() const { return scale_value; }

int DescriptorMeta::reference() const { return reference_value; }

uint64_t DescriptorMeta::datawidth() const { return datawidth_value; }

uint8_t DescriptorMeta::assocwidth() const { return assocwidth_value; }

void DescriptorMeta::setName(std::string n) { name_value = n; }

void DescriptorMeta::setUnit(std::string u) { unit_value = u; }

void DescriptorMeta::setScale(int sc) { scale_value = sc; }

void DescriptorMeta::setReference(int r) { reference_value = r; }

void DescriptorMeta::setDatawidth(uint64_t dw) { datawidth_value = dw; }

void DescriptorMeta::setAssocwidth(uint8_t aw) { assocwidth_value = aw; }

/*
std::string DescriptorMeta::getAssocStrValue(const uint64_t startbit, const int
datawidth, const std::vector<bool> & bits) const
{
        return NorBufrIO::getBitStrValue(startbit, datawidth, bits);
}
*/

bool operator==(const DescriptorMeta &lhs, const DescriptorMeta &rhs) {
  return ((lhs.name_value == rhs.name_value) &&
          (lhs.unit_value == rhs.unit_value) &&
          (lhs.scale_value == rhs.scale_value) &&
          (lhs.reference_value == rhs.reference_value) &&
          (lhs.datawidth_value == rhs.datawidth_value) &&
          (lhs.assocwidth_value == rhs.assocwidth_value));
}

bool operator!=(const DescriptorMeta &lhs, const DescriptorMeta &rhs) {
  return (!(lhs == rhs));
}

Descriptor::Descriptor() { Dm = 0; }

Descriptor::Descriptor(DescriptorId did, ssize_t sb)
    : DescriptorId(did), startbit(sb) {
  Dm = 0;
}

DescriptorId Descriptor::getDescriptorId() const { return 0; }

void Descriptor::setMeta(DescriptorMeta *dm) { Dm = dm; }

DescriptorMeta *Descriptor::getMeta() { return Dm; }

const DescriptorMeta *Descriptor::getMeta() const { return Dm; }

ssize_t Descriptor::startBit() const { return startbit; }

void Descriptor::setStartBit(ssize_t sb) { startbit = sb; }

std::ostream &Descriptor::printDetail(std::ostream &os) const {
  os << DescriptorId(*this);

  if (Dm) {
    os << " [";
    os << " dw: " << Dm->datawidth();
    os << " sc: " << Dm->scale();
    os << " ref: " << Dm->reference();
    //  os << " " << d.Dm->name();
    os << "]";
  }

  return os;
}

std::ostream &operator<<(std::ostream &os, const Descriptor &d) {
  os << DescriptorId(d);

  return os;
}

std::ostream &operator<<(std::ostream &os, const DescriptorMeta &dm) {
  os << "datawidth=" << dm.datawidth_value << " scale=" << dm.scale_value
     << " reference=" << dm.reference_value
     << " assoc_value:" << dm.assocwidth_value;
  return os;
}
