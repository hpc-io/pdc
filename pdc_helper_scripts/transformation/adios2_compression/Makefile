# ADIOS2 Operators analog of Data Flyway's TF compression/encryption
# transformation benchmarks -- no dependency on the rest of the PDC
# build tree, just an MPI-enabled ADIOS2 install (see
# ../../adios2_build/README.md) and ZFP (./build_zfp.sh builds it locally into
# .zfp_build/ if you don't already have it installed elsewhere).
#
#   adios2_bench_compression             - ZFP operator, analog of
#                                          src/tests/transformation/compression_transform.c
#   adios2_bench_compression_encryption  - ZFP + EncryptionOperator (libsodium,
#                                          ADIOS2's own shipped plugin) chained, analog of
#                                          src/tests/transformation/compression_encryption_transform.c

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

TARGETS := adios2_bench_compression adios2_bench_compression_encryption

.PHONY: all clean

all: $(TARGETS)

adios2_bench_compression: adios2_bench_compression.cpp
	$(CXX) $(CXXFLAGS) $(ADIOS2_DEFINES) $(ADIOS2_INCLUDE) -o $@ $< $(ADIOS2_LIB)

# EncryptionOperator (ADIOS2's shipped libsodium plugin) is dlopen'd by
# name at runtime, not linked -- just needs to be on the loader's
# search path, hence the same -rpath as the rest of ADIOS2's own libs.
adios2_bench_compression_encryption: adios2_bench_compression_encryption.cpp
	$(CXX) $(CXXFLAGS) $(ADIOS2_DEFINES) $(ADIOS2_INCLUDE) -o $@ $< $(ADIOS2_LIB)

clean:
	rm -f $(TARGETS) *.bp *.bp.key -r
