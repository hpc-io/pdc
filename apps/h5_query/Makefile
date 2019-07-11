# /**
# *
# * *** Copyright Notice ***
# *
# * PIOK - Parallel I/O Kernels, Copyright (c) 2015-2016, 
# * The Regents of the University of California, through Lawrence
# * Berkeley National Laboratory (subject to receipt of any required approvals
# * from the U.S. Dept. of Energy).  All rights reserved.
# *  
# * If you have questions about your rights to use or distribute this software,
# * please contact Berkeley Lab's Innovation & Partnerships Office at
# * IPO@lbl.gov.
# *  
# * NOTICE.  This Software was developed under funding from the U.S. Department
# * of Energy and the U.S. Government consequently retains certain rights. As
# * such, the U.S. Government has been granted for itself and others acting on
# * its behalf a paid-up, nonexclusive, irrevocable, worldwide license in the
# * Software to reproduce, distribute copies to the public, prepare derivative
# * works, and perform publicly and display publicly, and to permit other to do
# * so.
# *
# */
# *
# * Email questions to SByna@LBL.GOV
# * Scientific Data Management Research Group
# * Lawrence Berkeley National Laboratory
# *
# * Fri Mar 25 09:59:39 PDT 2016
# *

SRC = ./
BIN_DIR	= ./bin
# HDF5_DIR	= /global/homes/h/houhun/hdf5_tmp/hdf5/develop_build/hdf5
# MPICH_DIR	= /usr/local/pkg/mpich/3.2
ifeq (X$(MPICH_DIR), X)
	MPI_INCLUDE =
	MPI_LIB =
else
	MPI_INCLUDE = -I$(MPICH_DIR)/include
	MPI_LIB = -L$(MPICH_DIR)/lib -lmpich
endif
INCLUDES = -I. -I$(HDF5_DIR)/include $(MPI_INCLUDE)
C_FLAGS	= -m64 -DUSE_V4_SSE -DOMPI_SKIP_MPICXX -DPARALLEL_IO
LIB_FLAGS   = -L$(HDF5_DIR)/lib $(MPI_LIB) -lhdf5 -lz -lm -ldl

DEBUG   =   -g
SHARED_FLAG = 

CC	= /usr/bin/cc
C_EXE	= h5_query

.SUFFIXES: .cpp .c
C_OBJS = $(C_SRC:.c=.o)
.c.o:
	cd ${<D}; $(CC) $(DEBUG) $(C_FLAGS) $(INCLUDES) -c ${<F}

INSTALL	= /usr/bin/install
###################################################################
# Source files
###################################################################
HEADER_SRC	= timer.h

C_SRC	= h5_query.c 

###################################################################
# For ALL 
###################################################################
all:	${C_EXE}

$(C_EXE): $(C_OBJS)
	$(CC) -o ${C_EXE} $(C_OBJS) $(SHARED_FLAG) $(C_FLAGS) $(LIB_FLAGS)

###################################################################
# For clean 
###################################################################
clean: 
	\rm -f $(C_OBJS) ${C_EXE}

cleanall: 
	\rm -f $(C_OBJS) ${C_EXE}

###################################################################
# for depend
###################################################################
depend:
#	@ $(C++) $(INCLUDES) -M $(C_SRC) >> Makefile

###################################################################
# For install
###################################################################
install:
	$(INSTALL) $(C_EXE) $(BIN_DIR)

###################################################################
# for forceful rebuild
# automatically generates dependencies of each source files
###################################################################
rebuild	:
