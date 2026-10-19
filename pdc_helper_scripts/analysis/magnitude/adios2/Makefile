# ADIOS2 analog of Data Flyway's eager magnitude benchmark -- no
# dependency on the rest of the PDC build tree, just an MPI-enabled
# ADIOS2 install (see ../../../adios2_build/README.md for how the one used
# during development was built).
#
#   adios2_bench_magnitude   - vx/vy/vz in, MAGNITUDE(vx,vy,vz) out,
#                               analog of src/tests/analysis/bench_magnitude.c
#   adios2_verify_magnitude  - standalone read-only verifier for
#                               adios2_bench_magnitude's output, run as a
#                               genuinely separate process after the writer
#                               exits, so read timing isn't conflated with
#                               the writer's own warm process/connection state

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

TARGETS := adios2_bench_magnitude adios2_verify_magnitude

.PHONY: all clean

all: $(TARGETS)

adios2_bench_magnitude: adios2_bench_magnitude.cpp
	$(CXX) $(CXXFLAGS) $(ADIOS2_DEFINES) $(ADIOS2_INCLUDE) -o $@ $< $(ADIOS2_LIB)

adios2_verify_magnitude: adios2_verify_magnitude.cpp
	$(CXX) $(CXXFLAGS) $(ADIOS2_DEFINES) $(ADIOS2_INCLUDE) -o $@ $< $(ADIOS2_LIB)

clean:
	rm -f $(TARGETS) *.bp *.bp.key -r
