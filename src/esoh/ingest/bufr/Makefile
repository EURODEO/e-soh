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
CXXFLAGS := -c -Wall -Wextra -O2 -std=c++17 -fPIC $$(python3 -m pybind11 --includes)
LDFLAGS := -shared

all: bufresohmsg_py


bufresohmsg_py: bufresohmsg_py.h bufresohmsg_py.o ESOHBufr.o Sections.o NorBufrIO.o Tables.o Descriptor.o NorBufr.o WSI.o Oscar.o LogBuffer.o LogEntry.o
	$(CXX) $(LDFLAGS) bufresohmsg_py.o ESOHBufr.o Sections.o NorBufrIO.o Tables.o Descriptor.o NorBufr.o WSI.o Oscar.o LogBuffer.o LogEntry.o -o ./bufresohmsg_py$$(python3-config --extension-suffix)

clean:
	rm -f *.o *.so *.cpp~ *.h~

