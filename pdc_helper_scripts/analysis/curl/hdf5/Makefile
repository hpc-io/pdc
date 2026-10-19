# Standalone build for the HDF5 posthoc curl benchmark pair -- no
# dependency on the rest of the PDC build tree except curl_math.h,
# shared with ../../../../src/tests/analysis/ and ../adios2/ (the
# ADIOS2-side curl benchmark) so all three sides compute the identical
# curl. `git clone` + `make` is enough on Perlmutter after
# `module load cray-hdf5-parallel` (or any parallel-HDF5-enabled
# environment providing h5pcc).
#
# Two binaries, run as separate job steps (see
# ../pdc/curl_hdf5_analysis.sbatch):
#   hdf5_bench_curl_write    - write u/v/w, exit
#   hdf5_bench_curl_analyze  - reopen the file, read u/v/w back,
#                               compute curl then magnitude, write
#                               curl_x/y/z and vorticity_magnitude back

H5PCC := $(shell command -v h5pcc 2>/dev/null)
CC    ?= mpicc
CFLAGS ?= -O2 -Wall
CURL_MATH_INCLUDE := -I../../../../src/tests/analysis

TARGETS := hdf5_bench_curl_write hdf5_bench_curl_analyze

.PHONY: all clean

all: $(TARGETS)

ifdef H5PCC
hdf5_bench_curl_write: hdf5_bench_curl_write.c
	h5pcc $(CFLAGS) -o $@ $< -lm
hdf5_bench_curl_analyze: hdf5_bench_curl_analyze.c
	h5pcc $(CFLAGS) $(CURL_MATH_INCLUDE) -o $@ $< -lm
else
hdf5_bench_curl_write: hdf5_bench_curl_write.c
	$(CC) $(CFLAGS) -o $@ $< $(HDF5_CFLAGS) -lhdf5 -lm $(HDF5_LDFLAGS)
hdf5_bench_curl_analyze: hdf5_bench_curl_analyze.c
	$(CC) $(CFLAGS) $(CURL_MATH_INCLUDE) -o $@ $< $(HDF5_CFLAGS) -lhdf5 -lm $(HDF5_LDFLAGS)
endif

clean:
	rm -f $(TARGETS) *.h5
