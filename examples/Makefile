CC = mpicc
CFLAGS = -O2 -Wall -Wextra

INCLUDES = -I. -I$(PDC_DIR)/include
LDFLAGS = -L$(PDC_DIR)/lib

LIBS = -lpdc

.c.o:
	$(CC) $(CFLAGS) $(INCLUDES) -c $<

all: region_transfer_2D region_transfer_3D pdc_init obj_get_data bdcats vpicio bdcats_old vpicio_old read_write_perf read_write_col_perf

region_transfer_2D: region_transfer_2D.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)
region_transfer_3D: region_transfer_3D.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)
obj_get_data: obj_get_data.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)
pdc_init: pdc_init.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)
bdcats: bdcats.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)
vpicio: vpicio.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)
bdcats_old: bdcats_old.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)
vpicio_old: vpicio_old.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)
haccio: haccio.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)
tileio: tileio.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)
read_write_perf: read_write_perf.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)
read_write_col_perf: read_write_col_perf.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

clean:
	rm -f *.o region_transfer_2D region_transfer_3D pdc_init obj_get_data vpicio bdcats vpicio_old bdcats_old
