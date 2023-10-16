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

all: bufresohmsg_py


bufresohmsg_py: bufresohmsg_py.h bufresohmsg_py.cpp ESOHBufr.cpp Sections.cpp NorBufrIO.cpp Tables.cpp Descriptor.cpp NorBufr.cpp WSI.cpp Oscar.cpp
	$(CXX) $(PYFLAGS) $$(python3 -m pybind11 --includes) bufresohmsg_py.cpp ESOHBufr.cpp Sections.cpp NorBufrIO.cpp Tables.cpp Descriptor.cpp NorBufr.cpp WSI.cpp Oscar.cpp -o ./bufresohmsg_py$$(python3-config --extension-suffix)

clean:
	rm -f *.o *.so */*.so *.cpp~ *.h~

