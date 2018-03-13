
Required libraries
======

1 Mercury 
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
    CMAKE_INSTALL_PREFIX             /path/to/install/directory
    MERCURY_ENABLE_PARALLEL_TESTING  OFF (ON requires running on HPC compute nodes)
    MERCURY_USE_BOOST_PP             OFF
    MERCURY_USE_XDR                  OFF
    MERCURY_USE_OPA                  OFF
    NA_USE_BMI                       OFF
    NA_USE_OFI                       ON
    NA_USE_MPI                       OFF
    NA_USE_CCI                       OFF
    NA_USE_SM                        OFF
    
    OFI_INCLUDE_DIR                  /global/homes/j/jsoumagn/apps/libfabric/1.5.3/include
    OFI_LIBRARY                      /global/homes/j/jsoumagn/apps/libfabric/1.5.3/lib/libfabric.so

    CMAKE_C_FLAGS                    for both FLAGS, add -dynamic on NERSC machines 
    CMAKE_CXX_FLAGS                  if there you see errors: "/usr/bin/ld: attempted 
                                     static link of dynamic object `../bin/libmercury_hl.so' "


Setting include directory and library paths may require you to toggle to
the advanced mode by typing 't'. Once you are done and do not see any
errors, type 'g' to generate makefiles. Once you exit the CMake
configuration screen and are ready to build the targets, do:

    make

To test Mercury is successfully built, run

    make test

Look for Test  #1: mercury_rpc_ofi_tcp, Test  #2: mercury_bulk_ofi_tcp, etc.


2 OpenPA (Optional)
```sh
    wget https://trac.mpich.org/projects/openpa/raw-attachment/wiki/Downloads/openpa-1.0.4.tar.gz
    tar xzvf openpa-1.0.4.tar.gz && cd openpa-1.0.4 
    ./configure --enable-shared
    make && make install

```

3 CCI (Optional)
```sh
    git clone https://github.com/CCI/cci && cd cci
    ./autogen.pl
    ./configure (on Cori add --without-verbs for successful make)
    make && make install
```

4 BMI (optional)
```sh
    git clone git://git.mcs.anl.gov/bmi && cd bmi
    # If you are building BMI on an OSX platform, then apply the following patch:
    # patch -p1 < patches/bmi-osx.patch
    ./prepare && ./configure --enable-shared --enable-bmi-only
    make && make install
```

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
    ENABLE_CHECKPOINT                ON (or OFF if no metadata checkpoint is needed)
    ENABLE_LUSTRE                    ON
    ENABLE_MPI                       ON (or OFF if only used in serial mode)
    ENABLE_MULTITHREAD               OFF
    ENABLE_TIMING                    OFF (or ON to enable PDC timer)


Once you are done and do not see any errors, type 'g' to generate makefiles. 
Once you exit the CMake configuration screen and are ready to build the targets, do:

    make


Testing
====
On NERSC machines (e.g. Cori), do the following
----
* Job allocation (e.g. use 4 nodes)
```sh
    salloc -C haswell -N 4 -t 01:00:00 -q interactive --gres=craynetwork:2
```
Run PDC create object test
----
* Set pdc temporary directory for server config file and checkpoint file (optional, if not set, the server and client will create and write/read under ./pdc_tmp)
```sh
        export PDC_TMPDIR=/path/to/the/pdc/tmp/dir
```

* Run 4 server processes, each on one node in background:
```sh
        srun -N 4 -n  4 -c 2 --mem=25600 --cpu_bind=cores --gres=craynetwork:1 ./bin/pdc_server.exe &
```

* Run 64 client processes that concurrently create 1000 objects in total:
```sh
        srun -N 4 -n 64 -c 2 --mem=25600 --cpu_bind=cores --gres=craynetwork:1 ./bin/create_obj_scale -r 1000
```
