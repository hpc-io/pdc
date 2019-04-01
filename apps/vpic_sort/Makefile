DTCMP_DIR=/global/homes/h/houhun/cori/dtcmp/install
INCLUDES = -I. -I$(DTCMP_DIR)/include -I$(HDF5_DIR)/include $(MPI_INCLUDE) 
LIB_FLAGS = -L$(DTCMP_DIR)/lib -ldtcmp -L$(HDF5_DIR)/lib -lhdf5 -lz -lm -ldl $(MPI_LIB) 
CFLAGS = -dynamic  -fPIC

all:
	cc vpic_sort.c -o vpic_sort $(INCLUDES) $(LIB_FLAGS) $(CFLAGS) -g
clean:
	rm -rf ./*.o ./vpic_sort
