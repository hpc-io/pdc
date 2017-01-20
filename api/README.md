
Required libraries
======
1 CCI (has some issues, so it is optional for now)
```sh
    git clone https://github.com/CCI/cci && cd cci
    ./autogen.pl
    ./configure (on Cori add --without-verbs for successful make)
    make && make install
```

2 BMI 
```sh
    git clone git://git.mcs.anl.gov/bmi && cd bmi
    # If you are building BMI on an OSX platform, then apply the following patch:
    # patch -p1 < patches/bmi-osx.patch
    ./prepare && ./configure --enable-shared --enable-bmi-only
    make && make install
```


3 OpenPA
```sh
    wget https://trac.mpich.org/projects/openpa/raw-attachment/wiki/Downloads/openpa-1.0.4.tar.gz
    tar xzvf openpa-1.0.4.tar.gz && cd openpa-1.0.4 
    ./configure --enable-shared
    make && make install

```

4 Mercury 
```sh
    git clone https://github.com/mercury-hpc/mercury && cd mercury
    git submodule update --init
    mkdir build
    cd build
    ccmake .. (where ".." is the relative path to the mercury-X directory)
```

Type 'c' multiple times and choose suitable options. Recommended options are:

    BUILD_SHARED_LIBS                ON (or OFF if the library you link
                                     against requires static libraries)
    BUILD_TESTING                    ON
    Boost_INCLUDE_DIR                /path/to/include/directory
    CMAKE_INSTALL_PREFIX             /path/to/install/directory
    MERCURY_ENABLE_PARALLEL_TESTING  ON
    MERCURY_USE_BOOST_PP             ON/OFF (ON requires BOOST library)
    MERCURY_USE_SYSTEM_MCHECKSUM     OFF
    MERCURY_USE_XDR                  OFF
    MERCURY_USE_OPA                  ON
    NA_USE_BMI                       ON
    NA_USE_MPI                       OFF
    NA_USE_CCI                       OFF (OFF for now)
    
    BMI_INCLUDE_DIR                  BMI_PATH/include
    BMI_LIBRARY                      BMI_PATH/libbmi.so  

    OPA_INCLUDE_DIR                  OPENPA_PATH/include
    OPA_LIBRARY                      OPENPA_PATH/libopa.so
    
    CMAKE_C_FLAGS                    add -dynamic on NERSC machines if 
    CMAKE_CXX_FLAGS                  there is error /usr/bin/ld: attempted 
                                     static link of dynamic object 
                                     `../bin/libmercury_hl.so.0.8.9' )


Setting include directory and library paths may require you to toggle to
the advanced mode by typing 't'. Once you are done and do not see any
errors, type 'g' to generate makefiles. Once you exit the CMake
configuration screen and are ready to build the targets, do:

    make
    
To test Mercury is successfuly built, run

    make test

Look for Test  #1: mercury_rpc_bmi_tcp, Test  #2: mercury_bulk_bmi_tcp, etc.

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

Similar to previous Mercury building process, type 'c' multiple times and choose 
suitable options, and toggle to advanced mode by typing 't'. Recommended options are:

    BUILD_SHARED_LIBS                ON (or OFF if the library you link
                                     against requires static libraries)
    CFLAGS                           -dynamic (this is required on NERSC machines)


Once you are done and do not see any errors, type 'g' to generate makefiles. 
Once you exit the CMake configuration screen and are ready to build the targets, do:

    make



Testing
====
On NERSC machines (e.g. Edison, Cori), do the following
----
* Job allocation (e.g. use 4 nodes)
```sh
    salloc -N 4 -p debug -t 00:30:00 --gres=craynetwork:2
```
Run PDC create object test
----
* Run 4 server processes, each on one node in background:
```sh
        srun -N 4 -n 4 -c 2 --mem=2800 --gres=craynetwork:1 ./src/server/pdc_server.exe &
```

* Run 64 client processes that concurrently create 1000 objects each sequentially:
```sh
        srun -N 4 -n 64 -c 2 --mem=12840 --gres=craynetwork:1 ./tests/create_obj -r 1000
```