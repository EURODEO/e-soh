 /*
 * (C) Copyright 2023, met.no
 *
 * This file is part of the Norbufr BUFR en/decoder
 *
 * Author: istvans@met.no
 *
 */

#include <fstream>
#include <string>
#include <sstream>
#include <iomanip>
#include <cstring>
#include <filesystem>
#include <algorithm>
#include <iostream>

#include "Tables.h"


TableA::TableA()
{
    tableA.resize(256);

    tableA[0]="Surface data - land";
    tableA[1]="Surface data - sea";
    tableA[2]="Vertical soundings (other than satellite)";
    tableA[3]="Vertical soundings (satellite)";
    tableA[4]="Single level upper-air data (other than satellite)";
    tableA[5]="Single level upper-air data (satellite)";
    tableA[6]="Radar data";
    tableA[7]="Synoptic data";
    tableA[8]="Physical/chemical constituents";
    tableA[9]="Dispersal and transport";
    tableA[10]="Radiological data";
    tableA[11]="BUFR tables, complete replacement or update";
    tableA[12]="Surface data (satellite)";
    tableA[13]="Forecasts";
    tableA[14]="Warnings";
    tableA[20]="Status information";
    tableA[21]="Radiances (satellite measured)";
    tableA[22]="Radar (satellite) but not altimeter and scatterometer";
    tableA[23]="Lidar (satellite)";
    tableA[24]="Scatterometry (satellite)";
    tableA[25]="Altimetry (satellite)";
    tableA[26]="Spectrometry (satellite)";
    tableA[27]="Gravity measurement (satellite)";
    tableA[28]="Precision orbit (satellite)";
    tableA[29]="Space environment (satellite)";
    tableA[30]="Calibration datasets (satellite)";
    tableA[31]="Oceanographic data";
    tableA[32]="Lidar (ground-based)";
    for(int i=33; i<=100; ++i)
    {
        tableA[i]="Reserved: " + std::to_string(i);
    }
    tableA[101]="Image data (satellite)";
    for(int i=102; i<=239; ++i)
    {
        tableA[i]="Reserved: " + std::to_string(i);
    }
    for(int i=240; i<=254; ++i) tableA[i]="For Experimental use: " + std::to_string(i);
    //TODO: if( edition == 3 )
    //tableA[255]="Indicator for local use, with sub-category (for BUFR edition 3)"
    tableA[255]="Other category (for BUFR edition 4)";

}

TableB::TableB()
{
    clear();
}

const DescriptorMeta & TableB::at(DescriptorId d, bool ignore_throw ) const
{
    if( tableB.find(d) == tableB.end() )
    {
        if( !ignore_throw )
        {
            std::stringstream ss;
            ss  << "Table B " << d ;
            std::out_of_range(ss.str());
        }
        else
        {
            return tableB.begin()->second;
        }
    }

    return tableB.find(d)->second;
}

TableB::TableB(std::string f)
{
    if( std::filesystem::path(f).filename() == "element.table" )
    {
        readECCodes(f);
    }
    else
    {
        readWMO(f);
    }
}

void TableB::clear()
{
    tableB.clear();
    DescriptorId d;
    tableB[d] = dm0;
}

bool TableB::readECCodes( std::string filename )
{
    std::ifstream is(filename.c_str());
    if( !is.good() )
    {
        std::cerr << "ERROR: Read ECCodes tableB problem: " << filename << std::endl;
        return false;
    }

    const int linesize = 4096;

    char * line = new char[linesize];
    char * tmp = new char[linesize];

    is.getline(tmp,linesize); // header


    while( is.getline(line,linesize) )
    {
        std::stringstream ss(line);

        ss.getline(tmp,linesize,'|'); // FXY
        std::string fxy(tmp);
        DescriptorId d(fxy);

        ss.getline(tmp,linesize,'|'); // abbrevation
        ss.getline(tmp,linesize,'|'); // type

        ss.getline(tmp,linesize,'|'); // name
        std::string namestr(tmp);

        ss.getline(tmp,linesize,'|'); // name
        std::string unitstr(tmp);

        int scale ;
        ss.getline(tmp,linesize,'|'); // scale
        scale = strlen(tmp) ? std::stoi(tmp): 0;

        int reference;
        ss.getline(tmp,linesize,'|'); // reference
        reference = strlen(tmp) ? std::stoi(tmp) : 0 ;

        ss.getline(tmp,linesize,'|'); // DataWidth_Bits
        uint64_t datawidth = strlen(tmp) ? std::stoi(tmp) : 0 ;

        DescriptorMeta dp(namestr,unitstr,scale,reference,datawidth);
        tableB[d] = dp;

    }

        return true;
}

bool TableB::readWMO( std::string filename )
{
    clear();
    std::ifstream is(filename.c_str());
    if( !is.good() )
    {
        std::cerr << "ERROR: Read WMO TableB problem: " << filename << std::endl;
        return false;
    }
    const int linesize = 4096;

    char * line_raw = new char[linesize];
    char * line = new char[linesize];
    char * tmp = new char[linesize];
    std::string tmpstr;

    is.getline(tmp,linesize); // header

    while( is.getline(line_raw,linesize) )
    {
        // TODO: to function
        // Convert: ,, => ,"",
        int line_shift = 0;
        for(int i=0; i < is.gcount(); i++)
        {
            line[i+line_shift]=line_raw[i];
            if( line_raw[i] == ',' && i < is.gcount()-1 && line_raw[i+1] == ',' )
            {
                line[i+line_shift+1] = '"';
                line[i+line_shift+2] = '"';
                line_shift += 2;
                if( i+line_shift > linesize )
                {
                    std::cerr << "TableB line size exceded!!!\n";
                    delete [] line_raw;
                    delete [] line;
                    delete [] tmp;
                    return false;
                }
            }
        }

        std::stringstream ss(line);

        ss.getline(tmp,linesize,','); // No
        ss >> std::quoted(tmpstr);
        ss.getline(tmp,linesize,','); // ClassNo
        ss >> std::quoted(tmpstr);
        ss.getline(tmp,linesize,','); // ClassName_en
        ss >> std::quoted(tmpstr);
        ss.getline(tmp,linesize,','); // FXY

        DescriptorId d(tmpstr);

        std::string namestr;
        ss >> std::quoted(namestr);
        ss.getline(tmp,linesize,','); // ElementName_en

        ss >> std::quoted(tmpstr);
        ss.getline(tmp,linesize,','); // Note_en
        std::string unitstr;
        ss >> std::quoted(unitstr);
        ss.getline(tmp,linesize,','); // BUFR_Unit

        if( unitstr.substr(0,10) == "Code table" ) unitstr="CODE TABLE";
        if( unitstr.substr(0,10) == "Flag table" ) unitstr="FLAG TABLE";
        if( unitstr.substr(0,9) == "CCITT IA5" ) unitstr="CCITTIA5";

        int scale ;
        std::string scalestr;
        ss >> std::quoted(scalestr);
        ss.getline(tmp,linesize,','); // BUFR_Scale

        scale =  scalestr.size() ? std::stoi(scalestr): 0;

        int reference;
        std::string referencestr;
        ss >> std::quoted(referencestr);
        ss.getline(tmp,linesize,','); // BUFR_ReferenceValue

        reference = referencestr.size() ? std::stoi(referencestr) : 0 ;

        std::string dwstr;
        ss >> std::quoted(dwstr);
        ss.getline(tmp,linesize,','); // BUFR_DataWidth_Bits

        uint64_t datawidth = dwstr.size() ? std::stoi(dwstr) : 0 ;

        DescriptorMeta dp(namestr,unitstr,scale,reference,datawidth);
        tableB[d] = dp;

    }

    delete[] line;
    delete[] line_raw;
    delete[] tmp;

    return 0;

}

TableC::TableC()
{
}

TableC::TableC(std::string f)
{

    if( std::filesystem::path(f).filename() == "codetables" )
    {
        readECCodes(f);
    }
    else
    {
        readWMO(f);
    }

}

void TableC::clear()
{
    tableC.clear();
}

std::string TableC::codeStr(DescriptorId di,int c)
{
    std::string ret;

    ret = tableC[di][c];

    return ret;

}

bool TableC::readECCodes( std::string path )
{
    std::ifstream is(path.c_str());
    if( !is.good() )
    {
        std::cerr << "ERROR: Read ECCodes tableC problem: " << path << std::endl;
        return false;
    }

    const int linesize = 4096;
    char * tmp = new char[linesize];

    for(const auto & entry : std::filesystem::directory_iterator(path))
    {
        auto i = entry.path().filename().string().find('.');
        if( i != std::string::npos )
        {
            DescriptorId d(std::string(entry.path().filename().string(),0,static_cast<size_t>(i)));
            std::ifstream is(entry.path());
            int code;
            std::string value;
            while ( is >> code )
            {
                is >> code ;
                is.getline(tmp,linesize);
                value = std::string(tmp);

                tableC[d][code] = value;
            }
        }
        else
        {
            std::cerr << "Unknown TableC file: " << entry.path();
        }
    }

    delete [] tmp;

    return true;
}

bool TableC::readWMO( std::string filename)
{
    std::ifstream is(filename.c_str());
    if( !is.good() )
    {
        std::cerr << "ERROR: Read WMO tableC problem: " << filename << std::endl;
        return false;
    }
    const int linesize = 4096;

    char * line_raw = new char[linesize];
    char * line = new char[linesize];
    char * tmp = new char[linesize];
    std::string tmpstr;

    is.getline(tmp,linesize); // header

    while( is.getline(line_raw,linesize) )
    {
        // TODO: to function!!
        // Convert: ,, => ,"",
        int line_shift = 0;
        for(int i=0; i < is.gcount(); i++)
        {
            line[i+line_shift]=line_raw[i];
            if( line_raw[i] == ',' && i < is.gcount()-1 && line_raw[i+1] == ',' )
            {
                line[i+line_shift+1] = '"';
                line[i+line_shift+2] = '"';
                line_shift += 2;
                if( i+line_shift > linesize )
                {
                    std::cerr << "TableC line size exceded!!!\n";
                    delete [] line_raw;
                    delete [] line;
                    delete [] tmp;
                    return false;
                }
            }
        }

        std::stringstream ss(line);
        ss.getline(tmp,linesize,','); // No

        ss >> std::quoted(tmpstr);  // FXY
        ss.getline(tmp,linesize,',');

        DescriptorId d(tmpstr);

        std::string namestr;
        ss >> std::quoted(namestr);  // ElementName_en
        ss.getline(tmp,linesize,',');

        std::string codefigure;
        ss >> std::quoted(codefigure);  // CodeFigure
        ss.getline(tmp,linesize,',');

        std::string entrystr;
        ss >> std::quoted(entrystr);  // EntryName_en
        ss.getline(tmp,linesize,',');

        int code = 0;
        if( !codefigure.substr(0,3).compare("All") )
        {
            code = std::stoi(codefigure.substr(3)); // value
            tableC[d][code] = entrystr;
        }
        else
        {

            size_t range_indicator = codefigure.find("-");
            if( range_indicator != std::string::npos )
            {
                std::string from_str = codefigure.substr(0,range_indicator);
                int from = stoi(from_str);
                std::string to_str = codefigure.substr(range_indicator+1);
                int to = stoi(to_str);

                for(code = from ; code <= to; ++code)
                {
                    tableC[d][code] = entrystr;
                }
            }
            else
            {
                tableC[d][code] = entrystr;
            }
        }
    }

    delete[] line_raw;
    delete[] line;
    delete[] tmp;

    return 0;
}

TableD::TableD()
{
}

TableD::TableD(std::string f)
{
    if( std::filesystem::path(f).filename() == "sequence.def" )
    {
        readECCodes(f);
    }
    else
    {
        readWMO(f);
    }
}

ssize_t TableD::size() const
{
    return tableD.size();
}

void TableD::clear()
{
    tableD.clear();
    DescriptorId d(3,0,0);
    std::list<DescriptorId> l;
    tableD[d] = l;
}

bool TableD::readECCodes( std::string filename )
{
    std::ifstream is(filename.c_str());
    if( !is.good() )
    {
        std::cerr << "ERROR: Read ECCodes tableD problem: " << filename << std::endl;
        return false;
    }

    const int linesize = 4096;

    char * line = new char[linesize];
    char * tmp = new char[linesize];

    while( is.getline(line,linesize,']') )
    {
        std::string linestr(line);
        if( linestr.size() < 6 ) continue;

        std::stringstream ss(line);
        ss.getline(tmp,linesize,'"');

        int fxy ;
        ss >> fxy;

        DescriptorId D(fxy,true);

        ss.read(tmp,5);

        int dfxy;
        while( ss >> dfxy )
        {
            DescriptorId d(dfxy,true);
            ss.getline(tmp,linesize,',');

            tableD[D].push_back(d);
        }
    }

    return true;
}

bool TableD::readWMO( std::string filename)
{
    clear();
    std::ifstream is(filename.c_str());
    if( !is.good() )
    {
        std::cerr << "ERROR: Read WMO tableC problem: " << filename << std::endl;
        return false;
    }
    const int linesize = 4096;

    char * line_raw = new char[linesize];
    char * line = new char[linesize];
    char * tmp = new char[linesize];

    std::string tmpstr;

    is.getline(tmp,linesize); // header

    while( is.getline(line_raw,linesize) )
    {

        // TODO: to function
        // Convert: ,, => ,"",
        int line_shift = 0;
        for(int i=0; i < is.gcount(); i++)
        {
            line[i+line_shift]=line_raw[i];
            if( line_raw[i] == ',' && i < is.gcount()-1 && line_raw[i+1] == ',' )
            {
                line[i+line_shift+1] = '"';
                line[i+line_shift+2] = '"';
                line_shift += 2;
                if( i+line_shift > linesize )
                {
                    std::cerr << "TableB line size exceded!!!\n";
                    delete [] line_raw;
                    delete [] line;
                    delete [] tmp;
                    return false;
                }
            }
        }

        std::stringstream ss(line);

        ss.getline(tmp,linesize,','); // No
        ss >> std::quoted(tmpstr);
        ss.getline(tmp,linesize,','); // Category
        ss >> std::quoted(tmpstr);
        ss.getline(tmp,linesize,','); // CategoryOfSequences_en

        std::string fxy1str;
        ss >> std::quoted(fxy1str);
        ss.getline(tmp,linesize,','); // FXY1

        DescriptorId D(fxy1str);

        ss >> std::quoted(tmpstr);
        ss.getline(tmp,linesize,','); // Title_en

        ss >> std::quoted(tmpstr);
        ss.getline(tmp,linesize,','); // SubTitle_en


        std::string fxy2str;
        ss >> std::quoted(fxy2str);
        ss.getline(tmp,linesize,','); // FXY2

        DescriptorId d(fxy2str);

        tableD[D].push_back(d);

    }

    delete [] line_raw;
    delete [] line;
    delete [] tmp;

    return 0;
}

std::list<DescriptorId> TableD::expandDescriptor(const DescriptorId d, bool recursive) const
{
    std::list<DescriptorId> dlist;
    switch (d.f())
    {
        case 0 :
        case 1 :
        case 2 : dlist.push_back(d); break;
        case 3 :
            {
                if( tableD.find(d) != tableD.end() ) // TODO: exception
                {
                    for( auto it = at(d).begin(); it != at(d).end(); it ++ )
                    {
                        if( recursive )
                        {
                            auto dl = expandDescriptor(*it);
                            dl.splice(dl.end(),dl);
                        }
                        else
                        {
                            dlist.push_back(*it);
                        }
                    }
                }
            }
    }

    return dlist;
}

const std::list<DescriptorId> & TableD::at(DescriptorId d, bool ignore_throw ) const
{
    if( tableD.find(d) == tableD.end() )
    {
        std::stringstream ss;
        ss << "Table D " << d;
        if( !ignore_throw ) throw( std::out_of_range(ss.str()));
        else return tableD.begin()->second;
    }

    return tableD.find(d)->second;
}


