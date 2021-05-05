CC = mpicc
CFLAGS = -O2 -Wall -Wextra

INCLUDES = -I. -I$(PDC_DIR)/include
LDFLAGS = -L$(PDC_DIR)/lib

LIBS = -lpdc

.c.o:
	$(CC) $(CFLAGS) $(INCLUDES) -c $<

all: region_obj_map_2D region_obj_map_3D pdc_init obj_get_data bdcats vpicio

region_obj_map_2D: region_obj_map_2D.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

region_obj_map_3D: region_obj_map_3D.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

obj_get_data: obj_get_data.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

pdc_init: pdc_init.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

bdcats: bdcats.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

vpicio: vpicio.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

clean:
	rm -f *.o region_obj_map_2D region_obj_map_3D pdc_init obj_get_data vpicio bdcats
