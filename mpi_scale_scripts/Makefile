# Standalone build -- no dependency on the rest of the PDC build tree. See
# mpi_object_setup_scale.c's header comment for what this isolates and why.
#
# On Perlmutter, build with `make CC=cc` -- the Cray compiler wrapper
# (loaded by the default PrgEnv) links against the Cray-tuned cray-mpich,
# whose OFI netmod picks the cxi provider automatically over Slingshot-11.
# A plain mpicc build may instead pick up a generic (non-Cray) MPICH with
# no cxi support at all. Off Perlmutter (e.g. this repo's own sandbox/CI),
# mpicc is the portable default.
#
# Deliberately := (not ?=): make's own built-in default already defines
# CC=cc, which would defeat a ?= here off Perlmutter, where plain cc is
# just the system C compiler, not an MPI wrapper.
CC     := mpicc
CFLAGS ?= -O2 -Wall

TARGETS := mpi_object_setup_scale

.PHONY: all clean

all: $(TARGETS)

mpi_object_setup_scale: mpi_object_setup_scale.c
	$(CC) $(CFLAGS) -o $@ $<

clean:
	rm -f $(TARGETS)
