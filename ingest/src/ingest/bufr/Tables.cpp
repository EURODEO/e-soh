/*
 * (C) Copyright 2023, met.no
 *
 * This file is part of the Norbufr BUFR en/decoder
 *
 * Author: istvans@met.no
 *
 */

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include "NorBufrIO.h"
#include "Tables.h"

TableA::TableA() {
  tableA.resize(256);

  tableA[0] = "Surface data - land";
  tableA[1] = "Surface data - sea";
  tableA[2] = "Vertical soundings (other than satellite)";
  tableA[3] = "Vertical soundings (satellite)";
  tableA[4] = "Single level upper-air data (other than satellite)";
  tableA[5] = "Single level upper-air data (satellite)";
  tableA[6] = "Radar data";
  tableA[7] = "Synoptic data";
  tableA[8] = "Physical/chemical constituents";
  tableA[9] = "Dispersal and transport";
  tableA[10] = "Radiological data";
  tableA[11] = "BUFR tables, complete replacement or update";
  tableA[12] = "Surface data (satellite)";
  tableA[13] = "Forecasts";
  tableA[14] = "Warnings";
  tableA[20] = "Status information";
  tableA[21] = "Radiances (satellite measured)";
  tableA[22] = "Radar (satellite) but not altimeter and scatterometer";
  tableA[23] = "Lidar (satellite)";
  tableA[24] = "Scatterometry (satellite)";
  tableA[25] = "Altimetry (satellite)";
  tableA[26] = "Spectrometry (satellite)";
  tableA[27] = "Gravity measurement (satellite)";
  tableA[28] = "Precision orbit (satellite)";
  tableA[29] = "Space environment (satellite)";
  tableA[30] = "Calibration datasets (satellite)";
  tableA[31] = "Oceanographic data";
  tableA[32] = "Lidar (ground-based)";
  for (int i = 33; i <= 100; ++i) {
    tableA[i] = "Reserved: " + std::to_string(i);
  }
  tableA[101] = "Image data (satellite)";
  for (int i = 102; i <= 239; ++i) {
    tableA[i] = "Reserved: " + std::to_string(i);
  }
  for (int i = 240; i <= 254; ++i)
    tableA[i] = "For Experimental use: " + std::to_string(i);
  // TODO: if( edition == 3 )
  // tableA[255]="Indicator for local use, with sub-category (for BUFR edition
  // 3)"
  tableA[255] = "Other category (for BUFR edition 4)";
}

TableB::TableB() { clear(); }

const DescriptorMeta &TableB::at(DescriptorId d, bool ignore_throw) const {
  if (tableB.find(d) == tableB.end()) {
    if (!ignore_throw) {
      std::stringstream ss;
      ss << "Table B " << d;
      std::out_of_range(ss.str());
    } else {
      return tableB.begin()->second;
    }
  }

  return tableB.find(d)->second;
}

TableB::TableB(std::string f) {
  if (std::filesystem::path(f).filename() == "element.table") {
    readECCodes(f);
  } else {
    if (std::filesystem::path(f).filename() == "BUFRCREX_TableB_en.txt") {
      readWMO(f);
    } else {
      readOPERA(f);
    }
  }
}

void TableB::clear() {
  tableB.clear();
  DescriptorId d;
  tableB[d] = dm0;
}

bool TableB::readECCodes(std::string filename) {
  std::ifstream is(filename.c_str());
  if (!is.good()) {
    std::cerr << "ERROR: Read ECCodes tableB problem: " << filename
              << std::endl;
    return false;
  }

  const int linesize = 4096;

  char *line = new char[linesize];
  char *tmp = new char[linesize];

  is.getline(tmp, linesize); // header

  while (is.getline(line, linesize)) {
    std::stringstream ss(line);

    ss.getline(tmp, linesize, '|'); // FXY
    std::string fxy(tmp);
    DescriptorId d(fxy);

    ss.getline(tmp, linesize, '|'); // abbrevation
    ss.getline(tmp, linesize, '|'); // type

    ss.getline(tmp, linesize, '|'); // name
    std::string namestr(tmp);

    ss.getline(tmp, linesize, '|'); // name
    std::string unitstr(tmp);

    int scale;
    ss.getline(tmp, linesize, '|'); // scale
    scale = strlen(tmp) ? std::stoi(tmp) : 0;

    int reference;
    ss.getline(tmp, linesize, '|'); // reference
    reference = strlen(tmp) ? std::stoi(tmp) : 0;

    ss.getline(tmp, linesize, '|'); // DataWidth_Bits
    uint64_t datawidth = strlen(tmp) ? std::stoi(tmp) : 0;

    DescriptorMeta dp(namestr, unitstr, scale, reference, datawidth);
    tableB[d] = dp;
  }

  return true;
}

bool TableB::readWMO(std::string filename) {
  clear();
  std::ifstream is(filename.c_str());
  if (!is.good()) {
    std::cerr << "ERROR: Read WMO TableB problem: " << filename << std::endl;
    return false;
  }
  const int linesize = 4096;
  // field separator
  char fs = ',';

  char *line = new char[linesize];
  char *tmp = new char[linesize];
  std::string tmpstr;

  is.getline(tmp, linesize); // header

  while (is.getline(line, linesize)) {
    std::stringstream ss(line);

    NorBufrIO::getElement(ss, tmp, linesize, fs); // ClassNo
    NorBufrIO::getElement(ss, tmp, linesize, fs); // ClassName_en
    NorBufrIO::getElement(ss, tmp, linesize, fs); // FXY
    DescriptorId d(tmp);

    NorBufrIO::getElement(ss, tmp, linesize, fs); // ElementName_en
    std::string namestr(tmp);

    NorBufrIO::getElement(ss, tmp, linesize, fs); // BUFR_Unit
    std::string unitstr(tmp);
    if (unitstr.substr(0, 10) == "Code table")
      unitstr = "CODE TABLE";
    if (unitstr.substr(0, 10) == "Flag table")
      unitstr = "FLAG TABLE";
    if (unitstr.substr(0, 9) == "CCITT IA5")
      unitstr = "CCITTIA5";

    NorBufrIO::getElement(ss, tmp, linesize, fs); // BUFR_Scale
    int scale;
    std::string scalestr(tmp);
    scale = scalestr.size() ? std::stoi(scalestr) : 0;

    NorBufrIO::getElement(ss, tmp, linesize, fs); // BUFR_ReferenceValue
    int reference;
    std::string referencestr(tmp);
    reference = referencestr.size() ? std::stoi(referencestr) : 0;

    NorBufrIO::getElement(ss, tmp, linesize, fs); // BUFR_DataWidth_Bits
    std::string dwstr(tmp);
    uint64_t datawidth = dwstr.size() ? std::stoi(dwstr) : 0;

    DescriptorMeta dp(namestr, unitstr, scale, reference, datawidth);
    tableB[d] = dp;
  }

  delete[] line;
  delete[] tmp;

  return 0;
}

bool TableB::readOPERA(std::string filename) {

  clear();
  std::ifstream is(filename.c_str());
  if (!is.good()) {
    std::cerr << "ERROR: Read WMO TableB problem: " << filename << std::endl;
    return false;
  }
  const int linesize = 4096;
  // field separator
  char fs = ';';

  char *line = new char[linesize];
  char *tmp = new char[linesize];
  std::string tmpstr;
  int f, x, y;

  while (is.getline(line, linesize)) {
    // Replace characters:
    unsigned char *uline = reinterpret_cast<unsigned char *>(line);
    for (size_t i = 0; i < strlen(line); ++i) {
      // En dash => Hyphen-minus
      if (uline[i] == 0x96) {
        uline[i] = 0x2d;
      }
    }

    if (isdigit(line[0])) {
      std::stringstream ss(line);
      ss >> f;
      ss.get(); // read fs
      ss >> x;
      ss.get(); // read fs
      if (isdigit(line[ss.tellg()])) {
        ss >> y;
        ss.get();
        DescriptorId d(f, x, y);
        NorBufrIO::getElement(ss, tmp, linesize, fs); // ElementName_en
        std::string namestr(tmp);

        NorBufrIO::getElement(ss, tmp, linesize, fs); // BUFR_Unit
        std::string unitstr(tmp);
        if (unitstr.substr(0, 10) == "Code table")
          unitstr = "CODE TABLE";
        if (unitstr.substr(0, 10) == "Flag table")
          unitstr = "FLAG TABLE";
        if (unitstr.substr(0, 9) == "CCITT IA5")
          unitstr = "CCITTIA5";

        NorBufrIO::getElement(ss, tmp, linesize, fs); // BUFR_Scale
        int scale;
        std::string scalestr(tmp);
        scale = scalestr.size() ? std::stoi(scalestr) : 0;

        NorBufrIO::getElement(ss, tmp, linesize, fs); // BUFR_ReferenceValue
        int reference;
        std::string referencestr(tmp);
        reference = referencestr.size() ? std::stoi(referencestr) : 0;

        NorBufrIO::getElement(ss, tmp, linesize, fs); // BUFR_DataWidth_Bits
        std::string dwstr(tmp);
        uint64_t datawidth = dwstr.size() ? std::stoi(dwstr) : 0;

        DescriptorMeta dp(namestr, unitstr, scale, reference, datawidth);
        tableB[d] = dp;
      }
    }
  }

  delete[] line;
  delete[] tmp;

  return 0;
}

TableB &TableB::operator+=(const TableB &rhs) {
  for (auto m : rhs.tableB) {
    this->tableB[m.first] = m.second;
  }
  return *this;
}

TableC::TableC() {}

TableC::TableC(std::string f) {

  if (std::filesystem::path(f).filename() == "codetables") {
    readECCodes(f);
  } else {
    if (std::filesystem::path(f).filename() == "BUFRCREX_CodeFlag_en.txt") {
      readWMO(f);
    } else {
      if (std::filesystem::path(f).filename() == "btc085.019") {
        readOPERA(f);
      } else {
        std::cerr << "Unknown Code/Flag table\n";
      }
    }
  }
}

void TableC::clear() { tableC.clear(); }

std::string TableC::codeStr(DescriptorId di, int c) {
  std::string ret;

  ret = tableC[di][c];

  return ret;
}

bool TableC::readECCodes(std::string path) {
  std::ifstream is(path.c_str());
  if (!is.good()) {
    std::cerr << "ERROR: Read ECCodes tableC problem: " << path << std::endl;
    return false;
  }

  const int linesize = 4096;
  char *tmp = new char[linesize];

  for (const auto &entry : std::filesystem::directory_iterator(path)) {
    auto i = entry.path().filename().string().find('.');
    if (i != std::string::npos) {
      DescriptorId d(std::string(entry.path().filename().string(), 0,
                                 static_cast<size_t>(i)));
      std::ifstream is(entry.path());
      int code;
      std::string value;
      while (is >> code) {
        is >> code;
        is.getline(tmp, linesize);
        value = std::string(tmp);

        tableC[d][code] = value;
      }
    } else {
      std::cerr << "Unknown TableC file: " << entry.path();
    }
  }

  delete[] tmp;

  return true;
}

bool TableC::readWMO(std::string filename) {
  std::ifstream is(filename.c_str());
  if (!is.good()) {
    std::cerr << "ERROR: Read WMO tableC problem: " << filename << std::endl;
    return false;
  }
  const int linesize = 4096;

  char *line = new char[linesize];
  char *tmp = new char[linesize];
  std::string tmpstr;
  // field separator
  char fs = ',';

  is.getline(tmp, linesize); // header

  while (is.getline(line, linesize)) {

    std::stringstream ss(line);
    NorBufrIO::getElement(ss, tmp, linesize, fs); // FXY

    DescriptorId d(tmp);

    std::string namestr;
    NorBufrIO::getElement(ss, tmp, linesize, fs); // ElementName_en
    NorBufrIO::getElement(ss, tmp, linesize, fs); // CodeFigure
    std::string codefigure(tmp);
    NorBufrIO::getElement(ss, tmp, linesize, fs); // EntryName_en
    std::string entrystr(tmp);

    int code = 0;
    if (!codefigure.substr(0, 3).compare("All")) {
      code = std::stoi(codefigure.substr(3)); // value
      tableC[d][code] = entrystr;
    } else {

      size_t range_indicator = codefigure.find("-");
      if (range_indicator != std::string::npos) {
        std::string from_str = codefigure.substr(0, range_indicator);
        int from = stoi(from_str);
        std::string to_str = codefigure.substr(range_indicator + 1);
        int to = stoi(to_str);

        for (code = from; code <= to; ++code) {
          tableC[d][code] = entrystr;
        }
      } else {
        if (codefigure.size()) {
          code = std::stoi(codefigure);
          tableC[d][code] = entrystr;
        }
      }
    }
  }

  delete[] line;
  delete[] tmp;
  return 0;
}

bool TableC::readOPERA(std::string filename) {
  std::cerr << "Read Code table: " << filename << "\n";
  std::ifstream is(filename.c_str());
  if (!is.good()) {
    std::cerr << "ERROR: Read WMO tableC problem: " << filename << std::endl;
    return false;
  }
  const int linesize = 4096;

  char *line = new char[linesize];
  char *tmp = new char[linesize];
  std::string tmpstr;
  // field separator
  char fs = ' ';

  // is.getline(tmp, linesize); // header

  while (is.getline(line, linesize)) {

    std::stringstream ss(line);
    int di;
    ss >> di;
    DescriptorId d(di, true);
    int elements_num = 0;
    ss >> elements_num;
    int zero;
    ss >> zero;
    NorBufrIO::getElement(ss, tmp, linesize, '\n');
    std::string namestr(tmp);

    int code = 0;
    int element_desc_lines;
    for (int i = 1; i < elements_num; ++i) {
      is >> code;
      is >> element_desc_lines;
      std::string entrystr;
      for (int j = 0; j < element_desc_lines; j++) {
        is.getline(tmp, linesize);
        std::string tmpstr(tmp);
        tmpstr.erase(
            tmpstr.begin(),
            std::find_if(tmpstr.begin(), tmpstr.end(),
                         std::bind1st(std::not_equal_to<char>(), ' ')));
        entrystr += " " + tmpstr;
      }
      tableC[d][code] = entrystr;
    }

    NorBufrIO::getElement(ss, tmp, linesize, fs); // ElementName_en
    NorBufrIO::getElement(ss, tmp, linesize, fs); // CodeFigure
    std::string codefigure(tmp);
  }

  return true;
}

TableD::TableD() {}

TableD::TableD(std::string f) {
  if (std::filesystem::path(f).filename() == "sequence.def") {
    readECCodes(f);
  } else {
    if (std::filesystem::path(f).filename() == "BUFR_TableD_en.txt") {
      readWMO(f);
    } else {
      readOPERA(f);
    }
  }
}

TableC &TableC::operator+=(const TableC &rhs) {
  for (auto m : rhs.tableC) {
    this->tableC[m.first] = m.second;
  }
  return *this;
}

ssize_t TableD::size() const { return tableD.size(); }

void TableD::clear() {
  tableD.clear();
  DescriptorId d(3, 0, 0);
  std::list<DescriptorId> l;
  tableD[d] = l;
}

bool TableD::readECCodes(std::string filename) {
  std::ifstream is(filename.c_str());
  if (!is.good()) {
    std::cerr << "ERROR: Read ECCodes tableD problem: " << filename
              << std::endl;
    return false;
  }

  const int linesize = 4096;

  char *line = new char[linesize];
  char *tmp = new char[linesize];

  while (is.getline(line, linesize, ']')) {
    std::string linestr(line);
    if (linestr.size() < 6)
      continue;

    std::stringstream ss(line);
    ss.getline(tmp, linesize, '"');

    int fxy;
    ss >> fxy;

    DescriptorId D(fxy, true);

    ss.read(tmp, 5);

    int dfxy;
    while (ss >> dfxy) {
      DescriptorId d(dfxy, true);
      ss.getline(tmp, linesize, ',');

      tableD[D].push_back(d);
    }
  }

  return true;
}

bool TableD::readWMO(std::string filename) {
  clear();
  std::ifstream is(filename.c_str());
  if (!is.good()) {
    std::cerr << "ERROR: Read WMO tableC problem: " << filename << std::endl;
    return false;
  }
  const int linesize = 4096;

  char *line = new char[linesize];
  char *tmp = new char[linesize];
  // field separator
  char fs = ',';

  std::string tmpstr;

  is.getline(tmp, linesize); // header

  while (is.getline(line, linesize)) {

    std::stringstream ss(line);

    NorBufrIO::getElement(ss, tmp, linesize, fs); // Category
    NorBufrIO::getElement(ss, tmp, linesize, fs); // CategoryOfSequences_en

    NorBufrIO::getElement(ss, tmp, linesize, fs); // FXY1
    std::string fxy1str(tmp);
    DescriptorId D(fxy1str);

    NorBufrIO::getElement(ss, tmp, linesize, fs); // Title_en
    NorBufrIO::getElement(ss, tmp, linesize, fs); // SubTitle_en
    NorBufrIO::getElement(ss, tmp, linesize, fs); // FXY2
    std::string fxy2str(tmp);

    DescriptorId d(fxy2str);
    tableD[D].push_back(d);
  }

  delete[] line;
  delete[] tmp;

  return 0;
}

bool TableD::readOPERA(std::string filename) {
  std::cerr << "Read OPERA D table: " << filename << "\n";

  clear();
  std::ifstream is(filename.c_str());
  if (!is.good()) {
    std::cerr << "ERROR: Read WMO TableB problem: " << filename << std::endl;
    return false;
  }
  const int linesize = 4096;
  // field separator
  char fs = ';';

  char *line = new char[linesize];
  char *tmp = new char[linesize];
  std::string tmpstr;
  int F, X, Y;
  int f, x, y;

  while (is.getline(line, linesize)) {

    if (line[0] == '3' || (line[0] == ' ' && line[1] == '3')) {
    next_descriptor:
      F = X = Y = 0;
      std::stringstream ss(line);
      ss >> F;
      ss.get(); // read fs
      ss >> X;
      ss.get(); // read fs
      if (ss.str().find(fs, ss.tellg()) < ss.str().size()) {
        ss >> Y;
        ss.get();
        DescriptorId D(F, X, Y);
        f = x = y = 0;
        ss >> f;
        ss.get();
        ss >> x;
        ss.get();
        ss >> y;
        bool elements = true;
        while (elements) {
          DescriptorId d(f, x, y);

          tableD[D].push_back(d);
          is.getline(line, linesize);
          if (is.eof())
            break;
          if (line[0] == '3') {
            goto next_descriptor;
          }
          f = x = y = 0;
          std::stringstream lss(line);
          lss.getline(tmp, linesize, fs);
          lss.getline(tmp, linesize, fs);
          lss.getline(tmp, linesize, fs);
          lss >> f;
          lss.getline(tmp, linesize, fs);
          lss >> x;
          lss.getline(tmp, linesize, fs);
          lss >> y;
          if (f == 0 && x == 0 && y == 0) {
            elements = false;
          }
        }
      }
    }
  }

  delete[] line;
  delete[] tmp;

  return 0;
}

std::list<DescriptorId> TableD::expandDescriptor(const DescriptorId d,
                                                 bool recursive) const {
  std::list<DescriptorId> dlist;
  switch (d.f()) {
  case 0:
  case 1:
  case 2:
    dlist.push_back(d);
    break;
  case 3: {
    if (tableD.find(d) != tableD.end()) // TODO: exception
    {
      for (auto it = at(d).begin(); it != at(d).end(); it++) {
        if (recursive) {
          auto dl = expandDescriptor(*it);
          dl.splice(dl.end(), dl);
        } else {
          dlist.push_back(*it);
        }
      }
    }
  }
  }

  return dlist;
}

const std::list<DescriptorId> &TableD::at(DescriptorId d,
                                          bool ignore_throw) const {
  if (tableD.find(d) == tableD.end()) {
    std::stringstream ss;
    ss << "Table D " << d;
    if (!ignore_throw)
      throw(std::out_of_range(ss.str()));
    else
      return tableD.begin()->second;
  }

  return tableD.find(d)->second;
}

TableD &TableD::operator+=(const TableD &rhs) {
  for (auto m : rhs.tableD) {
    this->tableD[m.first] = m.second;
  }
  return *this;
}
