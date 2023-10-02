

#include <iostream>
#include <string>

#include "Oscar.h"
#include "WSI.h"

int main(int argc, char * argv[])
{

    if( argc <= 2 )
    {
        std::cerr << "Usage: " << std::string(argv[0]) << " OSCAR_Db_file WigosID\n";
        std::cerr << "Example: " << std::string(argv[0]) << " oscar_stations_all.json 0-22000-0-ZZEHWWT\n";
        return 1;
    }

    std::cerr << "Oscar: " << std::string(argv[1]) << "\n";
    Oscar oscar(argv[1]);

    for(int i=2; i<argc; i++)
    {

        std::string wid = oscar.findWigosId(argv[i]);
        if( wid.size() )
        {
            std::cout << "WigosID : " <<  wid << "\n";
            std::cout << "Details: " << oscar.to_string(wid) << "\n";
        }
        else
        {
            std::cout << "Station not found: " << std::string(argv[i]) << "\n";
        }

    }

    return 0;
}


