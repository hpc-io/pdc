CC = mpicc
CFLAGS = -O2 -Wall -Wextra

INCLUDES = -I. -I$(PDC_DIR)/include
LDFLAGS = -L$(PDC_DIR)/lib

LIBS = -lpdc

.c.o:
	$(CC) $(CFLAGS) $(INCLUDES) -c $<

all: pdc_init

pdc_init: pdc_init.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

clean:
	rm -f *.o pdc_init pdc_server.exe
