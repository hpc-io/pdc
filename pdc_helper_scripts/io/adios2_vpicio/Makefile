# ADIOS2 analogs of the raw VPIC-IO write/read-back throughput
# benchmarks -- no dependency on the rest of the PDC build tree, just an
# MPI-enabled ADIOS2 install (see ../../adios2_build/README.md).
#
#   adios2_vpicio  - 8-field VPIC-style particle write, analog of
#                     src/tests/misc/vpicio.c
#   adios2_bdcats  - reads back what adios2_vpicio wrote, analog of
#                     src/tests/misc/bdcats.c -- run as its own
#                     process after the writer exits

# := (not ?=): Make's own built-in default for CXX is g++, which
# already counts as "set" and would silently win over ?=.
CXX := mpicxx
CXXFLAGS ?= -O2 -Wall -std=c++17
ADIOS2_PREFIX ?= /mnt/fast/nlewis/workspace/install/adios2

ADIOS2_INCLUDE := -I$(ADIOS2_PREFIX)/include
# Some installs (e.g. this project's own local dev machine) use plain
# lib/; others (RHEL/Cray-toolchain machines, e.g. Perlmutter) use
# lib64/ instead -- auto-detect rather than hardcoding either.
ADIOS2_LIBDIR := $(if $(wildcard $(ADIOS2_PREFIX)/lib64/libadios2_c.*),lib64,lib)
ADIOS2_LIB := -L$(ADIOS2_PREFIX)/$(ADIOS2_LIBDIR) -Wl,-rpath,$(ADIOS2_PREFIX)/$(ADIOS2_LIBDIR) \
              -ladios2_cxx_mpi -ladios2_cxx -ladios2_c_mpi -ladios2_c
ADIOS2_DEFINES := -DADIOS2_USE_MPI=1

TARGETS := adios2_vpicio adios2_bdcats

.PHONY: all clean

all: $(TARGETS)

adios2_vpicio: adios2_vpicio.cpp
	$(CXX) $(CXXFLAGS) $(ADIOS2_DEFINES) $(ADIOS2_INCLUDE) -o $@ $< $(ADIOS2_LIB)

adios2_bdcats: adios2_bdcats.cpp
	$(CXX) $(CXXFLAGS) $(ADIOS2_DEFINES) $(ADIOS2_INCLUDE) -o $@ $< $(ADIOS2_LIB)

clean:
	rm -f $(TARGETS) *.bp *.bp.key -r
