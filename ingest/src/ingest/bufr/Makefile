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
CXXFLAGS := -c -Wall -Wextra -O2 -std=c++17 -fPIC
LDFLAGS := -shared

.PHONY: clean all bufresohmsg_py

objs := ESOHBufr.o Sections.o NorBufrIO.o Tables.o Descriptor.o NorBufr.o WSI.o Oscar.o LogBuffer.o LogEntry.o

all: bufresohmsg_py

bufresohmsg_py: bufresohmsg_py.so
	cp bufresohmsg_py.so ./bufresohmsg_py$$(python3-config --extension-suffix)

bufresohmsg_py.so: $(objs) bufresohmsg_py.o
	$(CXX) $(LDFLAGS) $(objs) bufresohmsg_py.o -o bufresohmsg_py.so

bufresohmsg_py.o: bufresohmsg_py.cpp bufresohmsg_py.h
	$(CXX) $(CXXFLAGS) $$(python3 -m pybind11 --includes) -o bufresohmsg_py.o bufresohmsg_py.cpp

$(objs): %.o: %.cpp %.h
	$(CXX) $(CXXFLAGS) -o $@ $<

clean:
	rm -f *.o *.so *.cpp~ *.h~
