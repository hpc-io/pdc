# ADIOS2 analog of Data Flyway's curl+vorticity-magnitude benchmark --
# no dependency on the rest of the PDC build tree, just an MPI-enabled
# ADIOS2 install (see ../../../adios2_build/README.md) and curl_math.h, shared
# with ../../../../src/tests/analysis/ and
# ../hdf5/ (the HDF5-side curl benchmark) so all three sides compute
# the identical curl.
#
#   adios2_bench_curl           - u/v/w in, vorticity_magnitude out (posthoc --
#                                 see adios2_bench_curl.cpp), analog of
#                                 src/tests/analysis/bench_curl_eager.c
#   adios2_bench_curl_inflight  - u/v/w in, curl_x/y/z manual + vorticity_magnitude
#                                 as a registered derived variable (in-flight --
#                                 see adios2_bench_curl_inflight.cpp), analog of
#                                 src/tests/analysis/bench_curl_eager.c

# := (not ?=): Make's own built-in default for CXX is g++, which
# already counts as "set" and would silently win over ?=.
CXX := mpicxx
CXXFLAGS ?= -O2 -Wall -std=c++17
ADIOS2_PREFIX ?= /mnt/fast/nlewis/workspace/install/adios2
CURL_MATH_INCLUDE := -I../../../../src/tests/analysis

ADIOS2_INCLUDE := -I$(ADIOS2_PREFIX)/include
# Some installs (e.g. this project's own local dev machine) use plain
# lib/; others (RHEL/Cray-toolchain machines, e.g. Perlmutter) use
# lib64/ instead -- auto-detect rather than hardcoding either.
ADIOS2_LIBDIR := $(if $(wildcard $(ADIOS2_PREFIX)/lib64/libadios2_c.*),lib64,lib)
ADIOS2_LIB := -L$(ADIOS2_PREFIX)/$(ADIOS2_LIBDIR) -Wl,-rpath,$(ADIOS2_PREFIX)/$(ADIOS2_LIBDIR) \
              -ladios2_cxx_mpi -ladios2_cxx -ladios2_c_mpi -ladios2_c
ADIOS2_DEFINES := -DADIOS2_USE_MPI=1

TARGETS := adios2_bench_curl adios2_bench_curl_inflight

.PHONY: all clean

all: $(TARGETS)

adios2_bench_curl: adios2_bench_curl.cpp
	$(CXX) $(CXXFLAGS) $(ADIOS2_DEFINES) $(ADIOS2_INCLUDE) $(CURL_MATH_INCLUDE) -o $@ $< $(ADIOS2_LIB)

adios2_bench_curl_inflight: adios2_bench_curl_inflight.cpp
	$(CXX) $(CXXFLAGS) $(ADIOS2_DEFINES) $(ADIOS2_INCLUDE) $(CURL_MATH_INCLUDE) -o $@ $< $(ADIOS2_LIB)

clean:
	rm -f $(TARGETS) *.bp *.bp.key -r
