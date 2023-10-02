 /*
 * (C) Copyright 2023, met.no
 *
 * This file is part of the Norbufr BUFR en/decoder
 *
 * Author: istvans@met.no
 *
 */

#include <string.h>
#include <stack>
#include <iomanip>
#include <cmath>
#include <limits>
#include <algorithm>
#include <bitset>

#include "NorBufr.h"
#include "NorBufrIO.h"


NorBufr::NorBufr()
{
    buffer = 0;
    //tabA = 0;
    tabB = 0;
    tabC = 0;
    tabD = 0;

    len = 0;
    edition = 0;

}

NorBufr::~NorBufr()
{
    if ( buffer ) delete [] buffer;
    //clearTable();
    for( auto v : extraMeta) delete v;
    extraMeta.clear();
}

void NorBufr::setTableDir(std::string s)
{
    table_dir = s;

    // TODO: add ecctables
    //TableA * ta = new TableA(table_dir + "BUFRCREX_TableA_en.txt");
    //tabA = ta;
    TableB * tb = new TableB(table_dir + "/BUFRCREX_TableB_en.txt");
    tabB = tb;
    TableC * tc = new TableC(table_dir + "/BUFRCREX_CodeFlag_en.txt");
    tabC = tc;
    TableD * td = new TableD(table_dir + "/BUFR_TableD_en.txt");
    tabD = td;

}

uint64_t NorBufr::uncompressDescriptor(std::list<DescriptorId>::iterator & it, ssize_t & sb, ssize_t & subsetsb, uint16_t * repeatnum)
{
    uint64_t repeat0 = 0;
    DescriptorMeta dm = tabB->at(*it);
    ssize_t referenceNull = NorBufrIO::getBitValue(sb,tabB->at(*it).datawidth(),false, bits);
    sb += tabB->at(*it).datawidth();
    ssize_t NBINC = NorBufrIO::getBitValue(sb,6,false,bits);
    sb += 6;
    for(uint32_t s=0; s<subsets.size(); ++s)
    {
        ssize_t increment = NorBufrIO::getBitValue(sb,NBINC,true,bits);
        // if delayed, don't push_back
        if( s || !repeatnum ) desc[s].push_back(Descriptor(*it,subsetsb));
        Descriptor &current_desc = desc[s].back();

        if( tabB->at(*it).unit().find("CCITTIA5") != std::string::npos )
        {
            std::string bs;
            bs = NorBufrIO::getBitStr(sb,NBINC*8,bits);

            std::vector<bool> bl = NorBufrIO::getBitVec(sb,NBINC *8,bits);
            ucbits.insert(ucbits.end(),bl.begin(),bl.end());
            current_desc.setMeta(const_cast<DescriptorMeta *>(&(tabB->at(*it))));
            current_desc.setStartBit(subsetsb);
            /* TODO: switch back !!!!
            if( NBINC && dm.datawidth() != static_cast<uint64_t>(NBINC*8) )
            {
                std::cerr << "Datawidth Check: assoc field ? dw=" << dm.datawidth() << " NBINC:" << NBINC << "\n";
            }
            */

            subsetsb += NBINC *8;
            sb += NBINC *8;

        }
        else
        {
            // TODO:assoc_field !!!

            uint64_t val;

            if( increment == std::numeric_limits<ssize_t>::max() ) val = std::numeric_limits<uint64_t>::max();
            else val = referenceNull+increment;

            if( repeatnum )
            {
                if( !s ) repeat0 = val;
                else
                {
                    if( repeat0 != val )
                    {
                        throw( std::out_of_range("Compressed delayed descriptor error!"));
                    }
                }
                *repeatnum = val;
            }

            std::vector<bool> bl = NorBufrIO::valueToBitVec(val,dm.datawidth());

            ucbits.insert(ucbits.end(),bl.begin(),bl.end());

            current_desc.setStartBit(subsetsb);
            current_desc.setMeta(const_cast<DescriptorMeta *>(&(tabB->at(*it))));

            subsetsb += dm.datawidth();
            sb += NBINC;
        }
    }

    return 0;

}

ssize_t NorBufr::extractDescriptors(int ss, ssize_t subsb)
{

    if( !subsetNum() ) return 0;

    ssize_t sb = subsb; // startbit
    // subset startbit
    ssize_t subsetsb = 0;
    ssize_t mod_datawidth = 0;
    ssize_t mod_str_datawidth = 0;
    ssize_t local_datawidth = 0;
    int mod_scale = 0;
    int mod_refvalue = 0;

    std::stack<uint8_t> assoc_field;

    std::list<DescriptorId> DL = sec3_desc;

    for( auto it = DL.begin(); it != DL.end(); ++it )
    {
        if( isCompressed()  )
        {
            if( sb >= static_cast<ssize_t>(bits.size()) )
            {
                std::cerr << "COmpressed Section4 size error!!! " << sb  << "[" << static_cast<ssize_t>(bits.size()) << "]\n";
                std::cerr << "COmpressed: " << isCompressed() << "\n";
                std::cerr << "IT: " << std::dec << *it << "\n";
                std::cerr << "Subset: " << subsetNum() << "\n";
                return sb;
            }
        }

        switch (it->f())
        {
            case 0 :    // Element Descriptor
            {
                if( isCompressed() )
                {
                    uncompressDescriptor(it, sb, subsetsb);
                }
                else
                {
                    if( !assoc_field.empty() && ( it->x() != 31 && it->y() != 21 )) sb += assoc_field.top();

                    desc[ss].push_back(Descriptor(*it,sb));

                    Descriptor &current_desc = desc[ss].back();

                    // local Descriptor, datawidth definef by previous descriptor [ 2 06 YYY ]
                    if( local_datawidth )
                    {
                        DescriptorMeta *dm = new DescriptorMeta;
                        *dm = tabB->at(*it);
                        dm->setDatawidth(local_datawidth);
                        auto dm_ptr = addMeta(dm);
                        // Meta already exists
                        if( dm_ptr != dm ) delete dm;
                        current_desc.setMeta(dm_ptr);
                        sb += local_datawidth;
                        local_datawidth = 0;
                        break;
                    }

                    if( tabB->at(*it).unit().find("CCITTIA5") != std::string::npos )
                    {
                        if( mod_str_datawidth )
                        {
                            sb += mod_str_datawidth;
                            DescriptorMeta *dm = new DescriptorMeta;
                            *dm = tabB->at(*it);
                            if( mod_str_datawidth ) dm->setDatawidth(mod_str_datawidth);
                            auto dm_ptr = addMeta(dm);
                            // Meta already exists
                            if( dm_ptr != dm ) delete dm;
                            current_desc.setMeta(dm_ptr);
                        }
                        else
                        {
                            sb += tabB->at(*it).datawidth();
                            current_desc.setMeta(const_cast<DescriptorMeta *>(&(tabB->at(*it))));
                        }
                    }
                    else
                    {
                        if( tabB->at(*it).unit().find("CODE TABLE") != std::string::npos || tabB->at(*it).unit().find("FLAG TABLE") != std::string::npos )
                        {
                            sb += tabB->at(*it).datawidth();
                            current_desc.setMeta(const_cast<DescriptorMeta *>(&(tabB->at(*it))));
                        }
                        else
                        {
                            sb += tabB->at(*it).datawidth()+mod_datawidth;

                            if( sb > static_cast<ssize_t>(bits.size()) )
                            {
                                // TODO: set missing ???
                                std::cerr << "Section4 size error!!! " << sb  << "[" << static_cast<ssize_t>(bits.size()) << "]\n";
                                std::cerr << "COmpressed: " << isCompressed() << "\n";
                                std::cerr << "IT: " << std::dec << *it << "\n";
                                std::cerr << "Subset: " << subsetNum() << "\n";
                                return sb;
                            }

                            if( ( !mod_scale && !mod_refvalue && !mod_datawidth && assoc_field.empty() ) || ( it->x() == 31 && it->y() == 21 ))
                            {
                                // TODO: Too ugly !!
                                current_desc.setMeta(const_cast<DescriptorMeta *>(&(tabB->at(*it))));
                            }
                            else
                            {

                                DescriptorMeta *dm = new DescriptorMeta;
                                *dm = tabB->at(*it);
                                if( !assoc_field.empty() )
                                {
                                    dm->setAssocwidth(assoc_field.top());
                                }
                                if( mod_datawidth ) dm->setDatawidth(dm->datawidth()+mod_datawidth);
                                if( mod_scale ) dm->setScale(dm->scale()+mod_scale);
                                if( mod_refvalue ) dm->setReference(mod_refvalue);
                                auto dm_ptr = addMeta(dm);
                                // Meta already exists
                                if( dm_ptr != dm ) delete dm;
                                current_desc.setMeta(dm_ptr);
                            }
                        }
                    }
                }
                break;
            }
            case 1 :    // Replication Descriptors
            {
                desc[ss].push_back(Descriptor(*it,sb));
                uint64_t index = 0;
                int descnum = it->x();
                uint16_t repeatnum = 0;
                if( it->y() != 0 ) repeatnum = it->y();
                else
                {
                    // Delayed descriptor [ 0 31 YYY ]
                    ++it;
                    if( it == DL.end() )
                    {
                        std::cerr << "ERROR: Delayed descriptor missing!\n";
                    }
                    desc[ss].push_back(Descriptor(*it,sb));
                    if( it->f() == 0 && it->x() == 31 )
                    {
                        if( !isCompressed() )
                        {
                            if( sb <= static_cast<ssize_t>(bits.size()-tabB->at(*it).datawidth()) )
                            {
                                repeatnum = NorBufrIO::getBitValue(sb,tabB->at(*it).datawidth(),false, bits);
                                index += tabB->at(*it).datawidth();
                                Descriptor &cd = desc[ss].back();
                                cd.setMeta(const_cast<DescriptorMeta *>(&(tabB->at(*it))));
                            }
                            else
                            {
                                std::cerr << "REPEAT 0      2 ---->> \n";
                                repeatnum = 0;
                            }
                        }
                        else
                        {
                            uncompressDescriptor(it, sb, subsetsb, &repeatnum);
                        }
                    }
                    else
                    {
                        std::cerr << "ERROR: Delayed Descriprtor error: " << *it << "\n";
                    }
                }

                std::list<DescriptorId> repeat_descriptors;
                auto rep_it = it;
                for( int i=0; i<descnum ; ++i)
                {
                    if( ++rep_it != DL.end() )
                    {
                        DescriptorId d = *(rep_it);
                        if( repeatnum )
                        {
                            repeat_descriptors.push_back(d);
                        }
                        else
                        {
                            ++it;
                        }
                    }
                    else
                    {
                        if( repeatnum ) std::cerr << "ERROR: Missing descriptors: << " << descnum-1-i << "\n";
                        break;
                    }
                }
                if( repeatnum )
                {
                    auto insert_here = ++rep_it;
                    for( int i=1; i<repeatnum; ++i) DL.insert(insert_here,repeat_descriptors.begin(),repeat_descriptors.end());
                }

                sb += index;
                break;

            }
            case 2 :    // Operator Descriptors
            {
                desc[ss].push_back(Descriptor(*it,sb));
                if( isCompressed() ) break;

                switch (it->x())
                {
                    case  1 :   if( it->y() ) mod_datawidth = it->y()-128; else mod_datawidth = 0; break;
                    case  2 :   if( it->y() ) mod_scale = it->y()-128; else mod_scale = 0; break;
                    case  3 :
                    {
                        if( it->y() != 255 )
                        {
                            mod_refvalue = NorBufrIO::getBitValue(sb,it->y(),false, bits);
                            sb += it->y();
                        }
                        else
                        {
                            mod_refvalue = 0;
                        }
                        break;
                    }
                    case  4 :
                    {
                        if( it->y() != 000 )
                        {
                            if( assoc_field.empty() ) assoc_field.push( it->y());
                            else assoc_field.push( it->y() + assoc_field.top() );
                        }
                        else
                        {
                            assoc_field.pop();
                        }
                        break;
                    }
                    case  5 : sb += it->y()*8; break;
                    case  6 : local_datawidth = it->y(); break;
                    case  7 :
                    {
                        if( it->y() ==  0 )
                        {
                            mod_scale=0;
                            mod_refvalue=0;
                            mod_datawidth=0;
                        }
                        else
                        {
                           mod_scale=it->y();
                           mod_refvalue=pow(10,it->y());
                           mod_datawidth=(int) ((10*it->y()+2)/3);
                        }
                        break;
                    }
                    case  8 : mod_str_datawidth = it->y()*8; break;

                    default : std::cerr << "Error: not implemented yet: " << *it << "\n";

                }

                break;
            }
            case 3 :    // Sequence Descriptors
            {
                desc[ss].push_back(Descriptor(*it,sb));
                auto dl = tabD->expandDescriptor(*it);
                auto ih = it;
                dl.splice(++ih,dl);
                break;
            }

            default :
            {

                break;
            }
        }
    }

    // Create SUBSETS
    ssize_t endbit = sb;
    if( !subsb ) subsets.at(0) = 0;

    // [0 31 XXX] is different in each subset
    if( !isCompressed() )
    {
        if( !subsb )
        {
            if( subsets.size() > 1 )
            {
                for(uint32_t i=1; i<subsets.size(); ++i)
                {
                    subsets.at(i) = endbit;
                    endbit = extractDescriptors(i,endbit);
                }
            }
        }
    }

    return endbit;
}

void NorBufr::clearTable()
{
//    if ( tabA ) delete tabA;
    if ( tabB ) delete tabB;
    if ( tabC ) delete tabC;
    if ( tabD ) delete tabD;
}

void NorBufr::clear()
{
    Section2::clear();
    Section3::clear();
    Section4::clear();
    freeBuffer();
    desc.clear();
    subsets.clear();
    for( auto v : extraMeta) delete v;
    extraMeta.clear();
    ucbits.clear();
    edition = 0;

}

void NorBufr::freeBuffer()
{
    if( buffer ) delete [] buffer;
    buffer = 0;
    len = 0;
}

uint64_t NorBufr::length() const
{
    return len;
}

bool NorBufr::saveBuffer(std::string filename) const
{
    std::ofstream os(filename,std::ifstream::binary);
    os.write(reinterpret_cast<char *>(buffer),len);
    os.close();
    return os.good();
}

std::vector<DescriptorMeta *>::iterator NorBufr::findMeta(DescriptorMeta *dm)
{
    auto it = extraMeta.begin();
    for( ; it != extraMeta.end(); ++it )
    {
        if( **it == *dm )
        {
            return it;
        }
    }

    return extraMeta.end();
}

DescriptorMeta * NorBufr::addMeta(DescriptorMeta *dm)
{
    auto it = findMeta(dm);
    if( it == extraMeta.end() )
    {
        extraMeta.push_back(dm);
        return extraMeta.back();
    }
    else
    {
        return *it;
    }
}

double NorBufr::getValue(const Descriptor & d,double) const
{
    const DescriptorMeta * dm = d.getMeta();
    double dvalue = 0.0;

    if ( dm )
    {
        const std::vector<bool> & bitref = (isCompressed() ? ucbits : bits );
        uint64_t value = NorBufrIO::getBitValue(d.startBit(),dm->datawidth(),!(d.f() == 0 && d.x() == 31),bitref);

        if( value == std::numeric_limits<uint64_t>::max() ) return (value);
        dvalue = value;

        if( dm->reference() ) dvalue += dm->reference();
        if( dm->scale() )
        {
            dvalue = dvalue/pow(10.0,dm->scale());
        }
    }

    return dvalue;
}

uint64_t NorBufr::getBitValue(const Descriptor & d,uint64_t) const
{
    const DescriptorMeta * dm = d.getMeta();
    uint64_t value = 0;

    if ( dm )
    {
        const std::vector<bool> & bitref = (isCompressed() ? ucbits : bits );
        value = NorBufrIO::getBitValue(d.startBit(),dm->datawidth(),!(d.f() == 0 && d.x() == 31),bitref);

        if( value == std::numeric_limits<uint64_t>::max() ) return (value);
    }

    return value;
}

int NorBufr::getValue(const Descriptor & d,int) const
{
    const DescriptorMeta * dm = d.getMeta();
    int value = 0;

    if ( dm )
    {
        const std::vector<bool> & bitref = (isCompressed() ? ucbits : bits );
        value = NorBufrIO::getBitValue(d.startBit(),dm->datawidth(),!(d.f() == 0 && d.x() == 31),bitref);

        if( value == std::numeric_limits<int64_t>::max() ) return (value);

        if( dm->reference() ) value += dm->reference();
        if( dm->scale() )
        {
            value = value/pow(10.0,dm->scale());
        }
    }

    return value;
}

std::string NorBufr::getValue(const Descriptor & d,std::string , bool with_unit) const
{
    std::string ret;
    const DescriptorMeta * dm = d.getMeta();

    if ( dm )
    {
        const std::vector<bool> & bitref = (isCompressed() ? ucbits : bits );
        // String
        if( ( d.f() == 2 && d.x() == 5 ) || dm->unit().find("CCITTIA5") != std::string::npos )
        {
            bool missing = true;
            for(uint16_t i=0; i < dm->datawidth(); i+=8)
            {
                uint64_t c = NorBufrIO::getBitValue(d.startBit()+i,8,true,bitref);
                if( c ) ret += static_cast<char> (c);
                if ( std::numeric_limits<uint64_t>::max() != c ) missing = false;
            }
            if( missing ) ret = "MISSING";
            return ret;
        }

        uint64_t value = NorBufrIO::getBitValue(d.startBit(),dm->datawidth(),!(d.f() == 0 && d.x() == 31),bitref);

        if( value == std::numeric_limits<uint64_t>::max() ) return ("MISSING");

        if( ! dm->reference() && ( dm->unit().find("CODE TABLE" ) != std::string::npos || dm->unit().find("FLAG TABLE" ) != std::string::npos ) )
        {
            std::stringstream ss(tabB->at(d).unit());

            ret = tabC->codeStr(d,value);
        }
        else
        {
            double dvalue = value;
            if( dm->reference() ) dvalue += dm->reference();
            if( dm->scale() )
            {
                dvalue = dvalue/pow(10.0,dm->scale());
            }
            std::stringstream ss;
            if( d.x() == 1 )
            {
                ss << std::fixed << std::setprecision(0);
            }
            ss << dvalue;
            ret = ss.str();
            if ( with_unit ) ret += " " + dm->unit();
        }
    }
    return ret;
}

std::ifstream & operator>>( std::ifstream & is, NorBufr & bufr)
{
    bufr.clear();
    if ( bufr.buffer )
    {
        delete [] bufr.buffer;
        bufr.buffer = 0;
    }

    // Search "BUFR" string
    unsigned long n = NorBufrIO::findBytes( is, "BUFR", 4);
    if ( n == ULONG_MAX )
    {
        return is;
    }

    is.seekg(static_cast<std::streampos> (n),std::ios_base::beg);

    // Section0 length
    int slen = 8;
    uint8_t sec0[slen];
    is.read(reinterpret_cast<char *>(sec0),slen);

    bufr.len = NorBufrIO::getBytes(sec0+4,3);
    bufr.edition = sec0[7];

    bufr.buffer = new uint8_t[bufr.len];
    memcpy(bufr.buffer,sec0,slen);

    is.read(reinterpret_cast<char *>(bufr.buffer+slen),bufr.len-slen);
    std::streamsize rchar = is.gcount();

    if( rchar != bufr.len-slen )
    {
        std::cerr << "Reading Error\n";
        bufr.len = rchar + slen-1;
    }

    int offset = bufr.checkBuffer();

    // "rewind" filepos
    if( offset || is.good() )
    {
        is.seekg(offset,std::ios_base::cur);
    }

    // Section1 load
    bufr.Section1::fromBuffer(bufr.buffer+slen,bufr.len-slen,bufr.edition);
    slen += bufr.Section1::length();

    // Section2 load, if exists
    if( bufr.optSection() )
    {
        bufr.Section2::fromBuffer(bufr.buffer+slen,bufr.len-slen);
        slen  += bufr.Section2::length();
    }

    // Section3 load
    bufr.Section3::fromBuffer(bufr.buffer+slen,bufr.len-slen);

    slen += bufr.Section3::length();
    bufr.subsets.resize(bufr.subsetNum());
    bufr.desc.resize(bufr.subsetNum());

    // Section 4 load
    bufr.Section4::fromBuffer(bufr.buffer+slen,bufr.len-slen);

    return is;
}

long NorBufr::checkBuffer()
{
    const char *start =  "BUFR" ;
    int si = 0;
    int ei = 0;

    long offset = 0;

    if( len >= 8 )
    {
        for( int i=4; i<len; ++i)
        {
            if( buffer[i] == start[si] )
            {
                si++;
                if( si == 4 )
                {
                    std::cerr << "Found new BUFR sequence at:" << i-4 << "\n";
                    offset = i - len - 4;
                    len = i - 4;
                    break;
                }
            }
            else
            {
                if( buffer[i] == start[0] ) si = 1;
                else si = 0;
            }
            if( buffer[i] == '7' )
            {
                ei++;
                if( ei == 4 && i != len-1 )
                {
                    std::cerr << "Found end sequence at:" << i << " len:" << len << "\n";
                    offset = i - len ;
                }
            }
            else ei = 0;
        }
    }

    return offset;

}

void NorBufr::print(DescriptorId df, std::string filter, DescriptorId dv) const
{

    for(size_t i = 0; i < desc.size(); ++i)
    {
        //std::cerr << "Find subset: " << i << "\n";
        auto it = std::find(desc[i].begin(),desc[i].end(),df);
        if( it != desc[i].end() )
        {
            //std::cerr << " Desc match :" << getValue(*it,std::string(),false) << ": ";
            if( getValue(*it,std::string(),false) == filter )
            {
                //std::cerr << " Filter match " ;
                auto dvit = std::find(it,desc[i].end(),dv);
                if( dvit != desc[i].end() )
                {
                    std::string dt(asctime(&bufr_time));
                    dt.erase(std::remove(dt.begin(), dt.end(), '\n'), dt.end());
                    std::cout <<  dt << " " << df << "=" << filter << " => " << dv << "=" << getValue(*dvit,std::string()) << "\n";
                }
            }
        }
    }
}

void NorBufr::printValue(DescriptorId df) const
{
    for(size_t i = 0; i < desc.size(); ++i)
    {
        //std::cerr << "Find subset: " << i << "\n";
        auto it = std::find(desc[i].begin(),desc[i].end(),df);
        if( it != desc[i].end() )
        {
            std::string v = getValue(*it,std::string(),false);
            if( v.size() ) std::cout << v << " ";
        }
    }
}

std::ostream & NorBufr::printDetail( std::ostream & os)
{
    os << std::dec;
    os << "********************************** B U F R ********************************************\n";
    os << "=============== Section 0  ===============\n";
    os << "length: " << len << " Edition: " << static_cast<int>(edition) << "\n";

    os << static_cast<Section1 &> (*this);
    os << static_cast<Section2 &> (*this);
    os << static_cast<Section3 &> (*this);
    os << static_cast<Section4 &> (*this);
    os << "Subsetbits: ";
    for( auto v : subsets )
    {
        os << v << " ";
    }
    os << "\n";

    os << "********************************** EXPANDED DESCRIPTORS  ********************************************\n";

    int subsetnum = 0;
    for( auto s : desc)
    {
        os << "\n ===================================== S U B S E T " << subsetnum << " =====================================\n\n" ;
        for( auto v : s)
        {
            v.printDetail(os) ;
            DescriptorMeta * meta = v.getMeta();
            if( meta )
            {
                os << " [sb: " << v.startBit() << "] ";
                os << getValue(v,std::string() );

                if( meta->unit().find("CODE TABLE") != std::string::npos || meta->unit().find("FLAG TABLE") != std::string::npos )
                {
                    os << " [code:" ;
                    uint64_t cval = NorBufrIO::getBitValue(v.startBit(),meta->datawidth(),true,(isCompressed() ? ucbits : bits));
                    if ( std::numeric_limits<uint64_t>::max() != cval )
                    {
                        os << cval;
                    }
                    os << "]";

                }
                os << "\t\tbits:" << NorBufrIO::getBitStrValue(v.startBit(),meta->datawidth(),(isCompressed() ? ucbits : bits));
                if( meta->assocwidth() > 0 )
                {
                    os << "\tassocbits:" << NorBufrIO::getBitStrValue(v.startBit()-meta->assocwidth(),meta->assocwidth(),(isCompressed() ? ucbits : bits));
                }
                os << " \t\tMeta: " ;
                os << " " << meta->name() << " unit: " << meta->unit();

            }
        os << "\n";
        }
        subsetnum++;
    }

    os << "\n";
    os << "********************************** E N D B U F R ********************************************\n";

    return os;
}

std::ostream & operator<<( std::ostream & os, NorBufr & bufr)
{
    os << std::dec;
    os << "\n***************************************************************************************\n";
    os << "********************************** B U F R ********************************************\n";
    os << "***************************************************************************************\n\n";
    os << "=============== Section 0  ===============\n";
    os << "length: " << bufr.len << " Edition: " << static_cast<int>(bufr.edition) << "\n";

    os << static_cast<Section1 &> (bufr);
    os << static_cast<Section2 &> (bufr);
    os << static_cast<Section3 &> (bufr);
    os << static_cast<Section4 &> (bufr);

    os << "\n***************************************************************************************\n";
    os << "************************ EXPANDED DESCRIPTORS AND DATA  *******************************\n";
    os << "***************************************************************************************\n";

    int subsetnum = 0;
    for( auto s : bufr.desc)
    {
        os << "\n=============== S U B S E T " << subsetnum << " ===============\n\n" ;
        for( auto v : s)
        {
            bool skip_value = false;
            os << v ;
            if( v == DescriptorId(1128,true) )
            {
                // Workaroung USA wigos local identifier
                std::string value_str = bufr.getValue(v,std::string());
                skip_value = true;
                std::string missing_wigos = "01101010000110101000011010100001101010000110101000011010100001101010000110101000011010100001101010000110101000011010100001101010";
                for(int i = 0; i < 16; i++)
                {
                    std::bitset<8> bs(value_str[i]);
                    if( bs.to_string<char,std::string::traits_type,std::string::allocator_type>() != missing_wigos.substr(i*8,8) )
                    {
                        skip_value = false;
                        break;
                    }
                }
            }
            DescriptorMeta * meta = v.getMeta();
            if( meta )
            {
                if( !skip_value )
                {
                    os << "\t" << bufr.getValue(v,std::string() );
                }

                if( meta->unit().find("CODE TABLE") != std::string::npos || meta->unit().find("FLAG TABLE") != std::string::npos )
                {
                    os << " [code:" ;
                    uint64_t cval = NorBufrIO::getBitValue(v.startBit(),meta->datawidth(),true,(bufr.isCompressed() ? bufr.ucbits : bufr.bits));
                    if ( std::numeric_limits<uint64_t>::max() != cval )
                    {
                        os << cval;
                    }
                    os << "]";

                }
                os << "\t\t" << meta->name() ;
            }
        os << "\n";
        }
        subsetnum++;
    }

    os << "\n";
    os << "***************************************************************************************\n";
    os << "******************************* E N D B U F R *****************************************\n";
    os << "***************************************************************************************\n";

    return os;
}

