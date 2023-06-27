[![linux](https://github.com/hpc-io/pdc/actions/workflows/linux.yml/badge.svg?branch=stable)](https://github.com/hpc-io/pdc/actions/workflows/linux.yml)
# Proactive Data Containers (PDC)
Proactive Data Containers (PDC) software provides an object-centric API and a runtime system with a set of data object management services. These services allow placing data in the memory and storage hierarchy, performing data movement asynchronously, and providing scalable metadata operations to find data objects. PDC revolutionizes how data is stored and accessed by using object-centric abstractions to represent data that moves in the high-performance computing (HPC) memory and storage subsystems. PDC manages extensive metadata to describe data objects to find desired data efficiently as well as to store information in the data objects.

PDC API, data types, and developer notes are available in docs/readme.md. 

More information and publications of PDC is available at https://sdm.lbl.gov/pdc

## Installation with Spack
[Spack](https://spack.io) is a multi-platform package manager that builds and installs multiple versions and configurations of software. It works on Linux, macOS, and many supercomputers. 
PDC and its dependent libraries can be installed with spack:

```bash
git clone -c feature.manyFiles=true https://github.com/spack/spack.git
cd spack/bin
./spack install pdc
```

# Installation from source

The following instructions are for installing PDC on Linux and Cray machines. 
We recommend using GCC 7 or a later version. 

PDC requires MPI, libfabric, and Mercury libraries.
PDC can use either MPICH or OpenMPI as the MPI library, if your system doesn't have one installed, follow [MPICH Installers’ Guide](https://www.mpich.org/documentation/guides), or [Installing Open MPI](https://docs.open-mpi.org/en/v5.0.x/installing-open-mpi/quickstart.html).

We provide detailed instructions for installing libfabric, Mercury, and PDC below.

## Preparing for Installation

### Preparing Work Space

Before installing the dependencies and downloading the code repository, we assume there is a directory created for your installation already, e.g. `$WORK_SPACE` and now you are in `$WORK_SPACE`.

```bash
export WORK_SPACE=/path/to/your/work/space
mkdir -p $WORK_SPACE/source
mkdir -p $WORK_SPACE/install
```

### Download Necessary Source Repository

Now, let's download [`libfabric`](https://github.com/ofiwg/libfabric/), [`mercury`](https://github.com/mercury-hpc/mercury) and [`pdc`](https://github.com/hpc-io/pdc/tree/develop) into our `source` directory.

```bash
cd $WORK_SPACE/source
git clone git@github.com:ofiwg/libfabric.git
git clone git@github.com:mercury-hpc/mercury.git
git clone git@github.com:hpc-io/pdc.git
```

### Prepare Directories for Artifact Installation
```bash
export LIBFABRIC_SRC_DIR=$WORK_SPACE/source/libfabric
export MERCURY_SRC_DIR=$WORK_SPACE/source/mercury
export PDC_SRC_DIR=$WORK_SPACE/source/pdc

export LIBFABRIC_DIR=$WORK_SPACE/install/libfabric
export MERCURY_DIR=$WORK_SPACE/install/mercury
export PDC_DIR=$WORK_SPACE/install/pdc

mkdir -p $LIBFABRIC_SRC_DIR
mkdir -p $MERCURY_SRC_DIR
mkdir -p $PDC_SRC_DIR

mkdir -p $LIBFABRIC_DIR
mkdir -p $MERCURY_DIR
mkdir -p $PDC_DIR

echo "export LIBFABRIC_SRC_DIR=$LIBFABRIC_SRC_DIR" > $WORK_SPACE/pdc_env.sh
echo "export MERCURY_SRC_DIR=$MERCURY_SRC_DIR" >> $WORK_SPACE/pdc_env.sh
echo "export PDC_SRC_DIR=$PDC_SRC_DIR" >> $WORK_SPACE/pdc_env.sh

echo "export LIBFABRIC_DIR=$LIBFABRIC_DIR" >> $WORK_SPACE/pdc_env.sh
echo "export MERCURY_DIR=$MERCURY_DIR" >> $WORK_SPACE/pdc_env.sh
echo "export PDC_DIR=$PDC_DIR" >> $WORK_SPACE/pdc_env.sh
```

Remember, from now on, at any time, you can simply run the following to set the above environment variables so that you can run any of the following commands for your installation.

```bash
export WORK_SPACE=/path/to/your/work/space
source $WORK_SPACE/pdc_env.sh
```

### Compile and Install`libfabric`

Check out tag `v1.11.2` for `libfabric`:

```bash
cd $LIBFABRIC_SRC_DIR
git checkout tags/v1.11.2
```

Configure, compile, and install:

```bash
./autogen.sh
./configure --prefix=$LIBFABRIC_DIR CC=cc CFLAG="-O2"

make -j
make install

export LD_LIBRARY_PATH="$LIBFABRIC_DIR/lib:$LD_LIBRARY_PATH"
export PATH="$LIBFABRIC_DIR/include:$LIBFABRIC_DIR/lib:$PATH"

echo 'export LD_LIBRARY_PATH=$LIBFABRIC_DIR/lib:$LD_LIBRARY_PATH' >> $WORK_SPACE/pdc_env.sh
echo 'export PATH=$LIBFABRIC_DIR/include:$LIBFABRIC_DIR/lib:$PATH' >> $WORK_SPACE/pdc_env.sh
```

Note: On Perlmutter@NERSC, `--disable-efa --disable-sockets` should be added to the `./configure` command during the compilation on login nodes.

### Compile and Install `mercury`

Now, you may check out a specific tag version of `mercury`, for example, `v2.2.0`:

```bash
cd $MERCURY_SRC_DIR
mkdir build
git checkout tags/v2.2.0
git submodule update --init
```

Configure, compile, test, and install:

```bash
cd build
cmake -DCMAKE_INSTALL_PREFIX=$MERCURY_DIR -DCMAKE_C_COMPILER=cc -DBUILD_SHARED_LIBS=ON \
      -DBUILD_TESTING=ON -DNA_USE_OFI=ON -DNA_USE_SM=OFF -DNA_OFI_TESTING_PROTOCOL=tcp ../
make -j && make install

ctest

export LD_LIBRARY_PATH="$MERCURY_DIR/lib:$LD_LIBRARY_PATH"
export PATH="$MERCURY_DIR/include:$MERCURY_DIR/lib:$PATH"

echo 'export LD_LIBRARY_PATH=$MERCURY_DIR/lib:$LD_LIBRARY_PATH' >> $WORK_SPACE/pdc_env.sh
echo 'export PATH=$MERCURY_DIR/include:$MERCURY_DIR/lib:$PATH' >> $WORK_SPACE/pdc_env.sh
```

## Compile and Install PDC
Now, it's time to compile and install PDC.

* One can replace `mpicc` to other available MPI compilers. For example, on Perlmutter, `cc` can be used to replace `mpicc`. 

```bash
cd $PDC_SRC_DIR
git checkout develop
mkdir build
cd build
cmake -DBUILD_MPI_TESTING=ON -DBUILD_SHARED_LIBS=ON -DBUILD_TESTING=ON -DCMAKE_INSTALL_PREFIX=$PDC_DIR \
      -DPDC_ENABLE_MPI=ON -DMERCURY_DIR=$MERCURY_DIR -DCMAKE_C_COMPILER=cc -DMPI_RUN_CMD=srun ../
make -j && make install
```
Note: `-DCMAKE_C_COMPILER=cc -DMPI_RUN_CMD=srun` may need to be changed to `-DCMAKE_C_COMPILER=mpicc -DMPI_RUN_CMD=mpiexec` depending on your system environment.

## Test Your Installation
PDC's `ctest` contains both sequential and parallel/MPI tests, and can be run with the following in the `build` directory.

```bash
ctest
```

### On Perlmutter or Other HPC Systems
`ctest` should be run on a compute node, one can submit an interactive job with the following on Perlmutter:

```bash
salloc --nodes 1 --qos interactive --time 01:00:00 --constraint cpu --account=mxxxx
```

## Set Environment Variables 
If all the tests pass, you can now add the final set of environment variables for future PDC runs.

```bash
export LD_LIBRARY_PATH="$PDC_DIR/lib:$LD_LIBRARY_PATH"
export PATH="$PDC_DIR/include:$PDC_DIR/lib:$PATH"

echo 'export LD_LIBRARY_PATH=$PDC_DIR/lib:$LD_LIBRARY_PATH' >> $WORK_SPACE/pdc_env.sh
echo 'export PATH=$PDC_DIR/include:$PDC_DIR/lib:$PATH' >> $WORK_SPACE/pdc_env.sh
```

`$WORK_SPACE/pdc_env.sh` sets all the environment variables needed to run PDC, and you only need to do the following once in each terminal session before running PDC.

```
export WORK_SPACE=/path/to/your/work/space
$WORK_SPACE/pdc_env.sh
```

## Running PDC 
PDC is a typical client-server application.
To run `PDC`, one needs to start the server processes first, and then the clients can be started and connected to the PDC servers automatically. 

### On Linux
  * Run 2 server processes in the background:
    ```bash
    mpiexec -np 2 $PDC_DIR/bin/pdc_server.exe &
    ```

  * Run 4 client processes that concurrently create 1000 objects and then create and query 1000 tags:
    ```bash
    mpiexec -np 4 $PDC_DIR/share/test/bin/kvtag_add_get_scale 1000 1000 1000
    ```
    
### On Perlmutter
  * Run 4 server processes, each on one node in the background:
    ```bash
    srun -N 4 -n  4 -c 2 --mem=25600 --cpu_bind=cores $PDC_DIR/bin/pdc_server.exe &
    ```

  * Run 64 client processes that concurrently create 1000 objects and then create and query 100000 tags:
    ```bash
    srun -N 4 -n 64 -c 2 --mem=25600 --cpu_bind=cores $PDC_DIR/share/test/bin/kvtag_add_get_scale 100000 100000 100000
    ```



