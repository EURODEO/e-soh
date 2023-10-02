 /*
 * (C) Copyright 2023, met.no
 *
 * This file is part of the Norbufr BUFR en/decoder
 *
 * Author: istvans@met.no
 *
 */


#include <iostream>
#include <fstream>
#include <filesystem>
#include <algorithm>
#include <iterator>
#include <sstream>

#include "Descriptor.h"

#include "Tables.h"

#include "NorBufr.h"


int main(int argc, char *argv[])
{

    // Load All BUFR tables

    std::map<int,TableB *> tb;
    std::map<int,TableC *> tc;
    std::map<int,TableD *> td;

    std::string eccBtable_dir("/usr/share/eccodes/definitions/bufr/tables/0/wmo");
    std::string eccDtable_dir("/usr/share/eccodes/definitions/bufr/tables/0/wmo");

    for(const auto & entry : std::filesystem::directory_iterator(eccBtable_dir))
    {
        auto vers = stoi(entry.path().filename().string());
        TableB * tb_e = new TableB( entry.path().string() +"/element.table");
        tb[vers] = tb_e;
        TableC * tc_e = new TableC( entry.path().string() +"/codetables");
        tc[vers] = tc_e;
    }

    for(const auto & entry : std::filesystem::directory_iterator(eccDtable_dir))
    {
        auto vers = stoi(entry.path().filename().string());
        TableD * tb_d = new TableD( entry.path().string() +"/sequence.def");
        td[vers] = tb_d;
    }

    for(int i=1; i<argc; i++)
    {
        std::ifstream bufrFile(argv[i],std::ios_base::in | std::ios_base::binary);

        std::string fname = std::filesystem::path(argv[i]).filename();

        while( bufrFile.good() )
        {

            NorBufr *bufr = new NorBufr;

            if ( bufrFile >> *bufr )
            {

                bufr->setTableB(tb.at(bufr->getVersionMaster() && tb.find(bufr->getVersionMaster()) != tb.end() ? bufr->getVersionMaster() : tb.rbegin()->first));
                bufr->setTableC(tc.at(bufr->getVersionMaster() && tc.find(bufr->getVersionMaster()) != tc.end() ? bufr->getVersionMaster() : tc.rbegin()->first));
                bufr->setTableD(td.at(bufr->getVersionMaster() && td.find(bufr->getVersionMaster()) != td.end() ? bufr->getVersionMaster() : td.rbegin()->first));

                bufr->extractDescriptors();

                std::cout << *bufr;
                //bufr->printDetail(std::cout);

            }
        }
    }


    for( auto i : tb ) delete i.second;
    for( auto i : tc ) delete i.second;
    for( auto i : td ) delete i.second;

    return 0;
}
