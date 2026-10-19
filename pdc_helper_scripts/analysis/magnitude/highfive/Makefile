# magnitude_highfive is a single-session (not posthoc-split) C++
# benchmark using the HighFive header-only wrapper over the same
# libhdf5 ../hdf5/'s plain-C posthoc pair benchmarks -- see that file's
# own header comment for why it exists (answer "does the wrapper cost
# anything over the raw C API", not a new storage engine). Needs a
# C++14 compiler (mpicxx) and HighFive's headers, cloned on demand into
# .highfive_src/ (git, header-only, no build step of its own) since it
# isn't a system package here.

H5PCC := $(shell command -v h5pcc 2>/dev/null)
# CXX is one of Make's built-in variables (defaults to g++), so a plain
# ?= never takes effect -- only override it while it's still at that
# built-in default, so an explicit `make CXX=...` still wins.
ifeq ($(origin CXX),default)
CXX := mpicxx
endif
CXXFLAGS ?= -O2 -Wall -std=c++14

HIGHFIVE_SRC := .highfive_src
HIGHFIVE_INCLUDE := -I$(HIGHFIVE_SRC)/include
# h5pcc wraps a C compiler only, so magnitude_highfive.cpp needs the raw
# HDF5 include/lib paths directly -- overridable, e.g. on Perlmutter after
# `module load cray-hdf5-parallel` these usually aren't needed at all
# (the compiler wrappers already have them).
HDF5_CXX_INCLUDE ?= $(shell h5pcc -show 2>/dev/null | grep -oE -- '-I[^ ]+' | tr '\n' ' ')
HDF5_CXX_LIBDIR  ?= $(shell h5pcc -show 2>/dev/null | grep -oE -- '-L[^ ]+' | tr '\n' ' ')
# h5pcc bakes an rpath into the C binaries it produces, so they find
# libhdf5.so on the compute nodes regardless of LD_LIBRARY_PATH at
# runtime; a plain -L/-lhdf5 link does NOT do this (-L is link-time
# only), and LD_LIBRARY_PATH from `module load` does not reliably reach
# srun-launched compute-node tasks on Perlmutter -- confirmed the hard
# way (every rank of every magnitude_highfive job failed with "error
# while loading shared libraries: libhdf5.so.1000: cannot open shared
# object file"). Fix: bake the same -L directories in as -Wl,-rpath too.
comma := ,
HDF5_CXX_RPATH := $(foreach d,$(patsubst -L%,%,$(HDF5_CXX_LIBDIR)),-Wl$(comma)-rpath$(comma)$(d))

TARGETS := magnitude_highfive

.PHONY: all clean highfive-fetch

all: $(TARGETS)

highfive-fetch:
	@if [ ! -d $(HIGHFIVE_SRC)/include/highfive ]; then \
		echo "== Cloning HighFive (header-only) into $(HIGHFIVE_SRC)..."; \
		git clone --quiet --depth 1 https://github.com/BlueBrain/HighFive.git $(HIGHFIVE_SRC); \
	fi

magnitude_highfive: magnitude_highfive.cpp | highfive-fetch
	$(CXX) $(CXXFLAGS) $(HIGHFIVE_INCLUDE) $(HDF5_CXX_INCLUDE) $(HDF5_CXX_LIBDIR) $(HDF5_CXX_RPATH) -o $@ $< -lhdf5 -lm

clean:
	rm -f $(TARGETS) *.h5
