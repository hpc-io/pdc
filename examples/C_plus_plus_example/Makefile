CC = mpicxx
CFLAGS = -O2 -Wall -Wextra

INCLUDES = -I. -I$(PDC_DIR)/include -I$(HDF5_DIR)/include
LDFLAGS = -Wl,-rpath,$(PDC_DIR)/lib -L$(PDC_DIR)/lib -L$(HDF5_DIR)/lib

LIBS = -lpdc -lhdf5

.cc.o:
	$(CC) $(CFLAGS) $(INCLUDES) -c $<

all: region_transfer_1D_append


region_transfer_1D_append: multidataset_plugin.o H5Timing.o region_transfer_1D_append.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)



clean:
	rm -f *.o region_transfer_1D_append
