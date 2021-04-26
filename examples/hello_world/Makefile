CC = mpicc
CFLAGS = -O2 -Wall -Wextra

INCLUDES = -I. -I$(PDC_DIR)/include -I$(PDC_DIR)/../api -I$(PDC_DIR)/../server -I$(MERCURY_LIB)/src -isystem /home/qkang/pdc_develop/test_install/mercury/src -isystem /home/qkang/pdc_develop/test_install/mercury/install/src -isystem /home/qkang/pdc_develop/test_install/mercury/src/proc_extra -isystem /home/qkang/pdc_develop/test_install/mercury/src/mchecksum/src -isystem /home/qkang/pdc_develop/test_install/mercury/install/src/mchecksum/src -isystem /home/qkang/pdc_develop/test_install/mercury/src/util -isystem /home/qkang/pdc_develop/test_install/mercury/install/src/util -isystem /home/qkang/pdc_develop/test_install/mercury/src/na -isystem /home/qkang/pdc_develop/test_install/mercury/install/src/na
LDFLAGS = -L$(PDC_DIR)/lib -L$(MERCURY_DIR)/bin
LIBS = -lpdc -lmercury

.c.o:
	$(CC) $(CFLAGS) $(INCLUDES) -c $<

all: pdc_init pdc_server.exe

pdc_server.exe: pdc_server.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

pdc_init: pdc_init.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

clean:
	rm -f *.o pdc_init pdc_server.exe
