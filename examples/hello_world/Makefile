CC = mpicc
CFLAGS = -O2 -Wall -Wextra

INCLUDES = -I. -I$(PDC_DIR)/include
LDFLAGS = -L$(PDC_DIR)/lib
LIBS = -lpdc

.c.o:
	$(CC) $(CFLAGS) $(INCLUDES) -c $<

all: hello_world pdc_server.exe

pdc_server.exe: pdc_server.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

pdc_init: pdc_init.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

clean:
	rm -f *.o hello_world pdc_server.exe
