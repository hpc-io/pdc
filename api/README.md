This directory contains code for PDC API.

BUILD
======
Required libraries
------
1. CCI (has some issues, so it is optional for now)
    git clone https://github.com/CCI/cci
    ./configure
    make && make install

2. BMI 
    git clone git://git.mcs.anl.gov/bmi && cd bmi
    # If you are building BMI on an OSX platform, then apply the following patch:
    # patch -p1 < patches/bmi-osx.patch
    ./prepare && ./configure --enable-shared --enable-bmi-only
    make && make install

3. Mercury 
    git clone https://github.com/mercury-hpc/mercury
    git submodule update --init
    cd mercury-X
    mkdir build
    cd build
    ccmake .. (where ".." is the relative path to the mercury-X directory)

Type 'c' multiple times and choose suitable options. Recommended options are:

    BUILD_SHARED_LIBS                ON (or OFF if the library you link
                                     against requires static libraries)
    BUILD_TESTING                    ON
    Boost_INCLUDE_DIR                /path/to/include/directory
    CMAKE_INSTALL_PREFIX             /path/to/install/directory
    MERCURY_ENABLE_PARALLEL_TESTING  ON
    MERCURY_USE_BOOST_PP             ON/OFF (requires BOOST library)
    MERCURY_USE_SYSTEM_MCHECKSUM     OFF
    MERCURY_USE_XDR                  OFF
    NA_USE_BMI                       ON
    NA_USE_MPI                       OFF
    NA_USE_CCI                       OFF (OFF for now)

Setting include directory and library paths may require you to toggle to
the advanced mode by typing 't'. Once you are done and do not see any
errors, type 'g' to generate makefiles. Once you exit the CMake
configuration screen and are ready to build the targets, do:

    make

Building
====
    git clone git@bitbucket.org:berkeleylab/pdc.git
    cd pdc
    git fetch
    git checkout Metadata
    cd api
    mkdir build
    cd build
    ccmake .. (where ".." is the relative path to the PDC directory)

Type 'c' multiple times and choose suitable options. Recommended options are:

    BUILD_SHARED_LIBS                ON (or OFF if the library you link
                                     against requires static libraries)

Toggle to advanced mode by typing 't' and set the following:
    CFLAGS                           -dynamic (this is required on NERSC machines)

Once you are done and do not see any errors, type 'g' to generate makefiles. 
Once you exit the CMake configuration screen and are ready to build the targets, do:

    make

Testing
====
On NERSC machines (e.g. Edison, Cori), do the following:
1. Job allocation (e.g. use 4 nodes)
    salloc -N 4 -p debug -t 00:30:00 --gres=craynetwork:2

2. Run PDC create object test
    Run 4 server processes, each on one node in background:
    srun -N 4 -n 4 -c 2 --mem=2800 --gres=craynetwork:1 ./src/server/pdc_server.exe &

    Run 64 client processes that concurrently create 1000 objects each sequentially:
    srun -N 4 -n 64 -c 2 --mem=12840 --gres=craynetwork:1 ./tests/create_obj -r 1000

