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

#include "Descriptor.h"

#include "Tables.h"

std::list<Descriptor> descriptorTree(Descriptor d, bool recoursive, TableD * td, int l)
{
    std::list<Descriptor> tree;
    d.setStartBit(l);
    switch ( d.f() )
    {
        case 0 :
        case 1 :
        case 2 : tree.push_back(d); break;
        case 3 :
            tree.push_back(d);
            for( auto tid = td->at(d).begin(); tid != td->at(d).end(); ++tid )
            {
                if( recoursive )
                {
                    std::list<Descriptor> dt = descriptorTree(*tid,true,td,l+1);
                    tree.splice(tree.end(),dt);
                }
                else
                {
                    tree.push_back(*tid);
                }
            }

    }
    return tree;

}

int main(int argc, char *argv[])
{

    bool detail = false;

    // Default Tables Dir
    TableB * tb = new TableB("/usr/share/eccodes/definitions/bufr/tables/0/wmo/37/element.table");
    //TableC * tc = new TableC("./bufrtables/BUFRCREX_CodeFlag_en.txt");
    TableD * td = new TableD("/usr/share/eccodes/definitions/bufr/tables/0/wmo/37/sequence.def");

    if( argc <= 1 )
    {
        std::cout << "Print Descriptor information\n";
        std::cout << "Usage: " << std::string(argv[0]) << " [tableD_file] descriptor\n";
        std::cout << "Example: " << std::string(argv[0]) << " 30711\n";
        return 1;
    }

    std::string a1(argv[1]);
    int p_start=1;
    if( a1.size() > 6 )
    {
        if ( tb ) delete tb;
        tb = new TableB(a1+"/element.table");

        if ( td ) delete td;
        td = new TableD(a1+"/sequence.def");
        p_start++;
        std::cout << "Descriptor Dir: " << a1 << "\n";
    }

    int level = 0;
    std::list<Descriptor> desc;
    for(int i=p_start; i<argc; i++)
    {
        std::cerr << "Descriptor: " << argv[i] << "\n";
        try {
            DescriptorId DId(argv[i]);
            desc = descriptorTree(DId,true,td,level);
        }
        catch (std::out_of_range &)
            {
                std::cerr << "Descriptor not exists\n";
                if( td ) delete td;
                return 2;
            }
        for( auto de : desc)
        {
            DescriptorMeta dm;
            dm = tb->at(de);
            for(int j=0; j<de.startBit(); ++j ) std::cout << "\t";
            std::cout << de << " \t\t" ;
            if( de.f() == 0 )
            {
                std::cout << dm.name();
                if( detail )
                {
                    std::cout << "\t\tUnit:" << dm.unit() ;
                    std::cout << "\t" << dm ;
                }
            }
            std::cout << "\n";
        }
    }

    if( tb ) delete tb;
    //if( tc ) delete tc;
    if( td ) delete td;

    return 0;

}
