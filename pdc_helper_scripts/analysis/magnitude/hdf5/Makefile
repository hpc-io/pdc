# Standalone build for the HDF5 posthoc magnitude benchmark pair -- no
# dependency on the rest of the PDC build tree, so `git clone` + `make`
# is enough on Perlmutter after `module load cray-hdf5-parallel` (or any
# parallel-HDF5-enabled environment providing h5pcc).
#
# Two binaries, run as separate job steps (see
# ../pdc/posthoc_hdf5.sbatch), so posthoc's real-world relaunch cost is
# measured instead of hidden inside one continuous process:
#   hdf5_bench_write            - write vx/vy/vz, exit
#   hdf5_bench_posthoc_analyze  - reopen the file, read back, compute
#                                  magnitude, write it back

H5PCC := $(shell command -v h5pcc 2>/dev/null)
CC    ?= mpicc
CFLAGS ?= -O2 -Wall

TARGETS := hdf5_bench_write hdf5_bench_posthoc_analyze

.PHONY: all clean

all: $(TARGETS)

ifdef H5PCC
hdf5_bench_write: hdf5_bench_write.c
	h5pcc $(CFLAGS) -o $@ $< -lm
hdf5_bench_posthoc_analyze: hdf5_bench_posthoc_analyze.c
	h5pcc $(CFLAGS) -o $@ $< -lm
else
hdf5_bench_write: hdf5_bench_write.c
	$(CC) $(CFLAGS) -o $@ $< $(HDF5_CFLAGS) -lhdf5 -lm $(HDF5_LDFLAGS)
hdf5_bench_posthoc_analyze: hdf5_bench_posthoc_analyze.c
	$(CC) $(CFLAGS) -o $@ $< $(HDF5_CFLAGS) -lhdf5 -lm $(HDF5_LDFLAGS)
endif

clean:
	rm -f $(TARGETS) *.h5
