#
# (C) Copyright 2023, met.no
#
# This file is part of the Norbufr BUFR en/decoder
#
# Author: istvans@met.no
#
#


CXX := g++
#CXX := clang++
CXXFLAGS := -c -Wall -Wextra -O2 -std=c++17
LDFLAGS :=
PYFLAGS := -Wall -Wextra -O2 -shared -std=c++17 -fPIC

all: bufrprint descriptor_info esoh

esoh: bufresohmsg_py oscar_test

bufresohmsg_py: bufresohmsg_py.h bufresohmsg_py.cpp ESOHBufr.o Sections.o NorBufrIO.o Tables.o Descriptor.o NorBufr.o WSI.o Oscar.o
	$(CXX) $(PYFLAGS) $$(python3 -m pybind11 --includes) bufresohmsg_py.cpp ESOHBufr.cpp Sections.cpp NorBufrIO.cpp Tables.cpp Descriptor.cpp NorBufr.cpp WSI.cpp Oscar.cpp -o ./esoh/bufresohmsg_py$$(python3-config --extension-suffix)

bufresohmsg: bufresohmsg.o ESOHBufr.o Sections.o NorBufrIO.o Tables.o Descriptor.o NorBufr.o
	$(CXX) $(LDFLAGS) bufresohmsg.o ESOHBufr.o Sections.o NorBufrIO.o Tables.o Descriptor.o NorBufr.o -o bufresohmsg -lcrypto

bufresohmsg.o: ESOHBufr.h bufresohmsg.h bufresohmsg.cpp
	$(CXX) $(CXXFLAGS) bufresohmsg.cpp

ESOHBufr.o: ESOHBufr.h ESOHBufr.cpp
	$(CXX) $(CXXFLAGS) ESOHBufr.cpp

oscar_test: oscar_test.o Oscar.o WSI.o NorBufrIO.o
	$(CXX) $(LDFLAGS) oscar_test.o Oscar.o WSI.o NorBufrIO.o -o oscar_test

oscar_test.o: Oscar.h oscar_test.cpp
	$(CXX) $(CXXFLAGS) oscar_test.cpp

Oscar.o: Oscar.h Oscar.cpp WSI.h
	$(CXX) $(CXXFLAGS) Oscar.cpp

WSI.o: WSI.h WSI.cpp
	$(CXX) $(CXXFLAGS) WSI.cpp

descriptor_info: descriptor_info.o Descriptor.o Tables.o NorBufrIO.o
	$(CXX) $(LDFLAGS) descriptor_info.o Descriptor.o Tables.o NorBufrIO.o -o descriptor_info

descriptor_info.o: descriptor_info.cpp Descriptor.h
	$(CXX) $(CXXFLAGS) descriptor_info.cpp

Descriptor.o : Descriptor.h Descriptor.cpp
	$(CXX) $(CXXFLAGS) Descriptor.cpp

bufrprint: bufrprint.o NorBufr.o Sections.o NorBufrIO.o Descriptor.o Tables.o
	$(CXX) $(LDFLAGS) bufrprint.o NorBufr.o Sections.o NorBufrIO.o Descriptor.o Tables.o -o bufrprint

bufrprint.o: bufrprint.cpp NorBufr.h
	$(CXX) $(CXXFLAGS) bufrprint.cpp

NorBufr.o : NorBufr.h NorBufr.cpp Sections.h NorBufrIO.h Descriptor.h Tables.h
	$(CXX) $(CXXFLAGS) NorBufr.cpp

Sections.o : Sections.h Sections.cpp Descriptor.h
	$(CXX) $(CXXFLAGS) Sections.cpp

Tables.o : Tables.h Tables.cpp Descriptor.h
	$(CXX) $(CXXFLAGS) Tables.cpp

NorBufrIO.o : NorBufrIO.h NorBufrIO.cpp
	$(CXX) $(CXXFLAGS) NorBufrIO.cpp


clean:
	rm -f descriptor_info bufrprint oscar_test *.o *.so */*.so *.cpp~ *.h~

