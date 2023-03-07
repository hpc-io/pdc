[![linux](https://github.com/hpc-io/pdc/actions/workflows/linux.yml/badge.svg?branch=stable)](https://github.com/hpc-io/pdc/actions/workflows/linux.yml)
# Proactive Data Containers (PDC)
Proactive Data Containers (PDC) software provides an object-centric API and a runtime system with a set of data object management services. These services allow placing data in the memory and storage hierarchy, performing data movement asynchronously, and providing scalable metadata operations to find data objects. PDC revolutionizes how data is stored and accessed by using object-centric abstractions to represent data that moves in the high-performance computing (HPC) memory and storage subsystems. PDC manages extensive metadata to describe data objects to find desired data efficiently as well as to store information in the data objects.

PDC API, data types, and developer notes are available in docs/readme.md. 

More information and publications of PDC is available at https://sdm.lbl.gov/pdc

# Installation 

The following instructions are for installing PDC on Linux and Cray machines. 
GCC version 7 or newer and a version of MPI are needed to install PDC. 

Current PDC tests have been verified with MPICH. To install MPICH, follow the documentation in https://www.mpich.org/static/downloads/3.4.1/mpich-3.4.1-installguide.pdf

PDC also depends on libfabric and Mercury. We provide detailed instructions for installing libfabric, Mercury, and PDC below.
Make sure to record the environmental variables (lines that contains the "export" commands). They are needed for running PDC and to use the libraries again.

## Preparing for Installation

PDC relies on [`libfabric`](https://github.com/ofiwg/libfabric/) as well as [`mercury`](https://github.com/mercury-hpc/mercury). Therefore, let's **prepare the dependencies**.
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

Remember, from now on, at any time, you can simply run the following to set the above environment variables so that you can run any of the following command for your installation.

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

Configure, compile and install:

```bash
./autogen.sh
./configure --prefix=$LIBFABRIC_DIR --disable-efa --disable-sockets CC=cc CFLAG="-O2"

make -j 32
make install

export LD_LIBRARY_PATH="$LIBFABRIC_DIR/lib:$LD_LIBRARY_PATH"
export PATH="$LIBFABRIC_DIR/include:$LIBFABRIC_DIR/lib:$PATH"

echo 'export LD_LIBRARY_PATH=$LIBFABRIC_DIR/lib:$LD_LIBRARY_PATH' >> $WORK_SPACE/pdc_env.sh
echo 'export PATH=$LIBFABRIC_DIR/include:$LIBFABRIC_DIR/lib:$PATH' >> $WORK_SPACE/pdc_env.sh
```

### Compile and Install `mercury`

Now, you may check out a specific tag version of `mercury`, for example, `v2.2.0`:

```bash
cd $MERCURY_SRC_DIR
mkdir build
git checkout tags/v2.2.0
git submodule update --init
```

Configure, compile, test and install:

```bash
cd build
cmake ../ -DCMAKE_INSTALL_PREFIX=$MERCURY_DIR -DCMAKE_C_COMPILER=cc -DBUILD_SHARED_LIBS=ON -DBUILD_TESTING=ON -DNA_USE_OFI=ON -DNA_USE_SM=OFF -DNA_OFI_TESTING_PROTOCOL=tcp 
make -j 32 && make install

ctest

export LD_LIBRARY_PATH="$MERCURY_DIR/lib:$LD_LIBRARY_PATH"
export PATH="$MERCURY_DIR/include:$MERCURY_DIR/lib:$PATH"

echo 'export LD_LIBRARY_PATH=$MERCURY_DIR/lib:$LD_LIBRARY_PATH' >> $WORK_SPACE/pdc_env.sh
echo 'export PATH=$MERCURY_DIR/include:$MERCURY_DIR/lib:$PATH' >> $WORK_SPACE/pdc_env.sh
```

## Compile and Install PDC
Now, it's time to compile and install PDC.

* One can replace `mpicc` to other available MPI compilers. For example, on Cori, `cc` can be used to replace `mpicc`. 
* `-DCMAKE_C_FLAGS="-dynamic"` used to be required for Cori, but now it is not necessary any more.
* `ctest` contains both sequential and MPI tests for the PDC settings. These can be used to perform regression tests.

```bash
cd $PDC_SRC_DIR
git checkout develop
mkdir build
cd build
cmake ../ -DBUILD_MPI_TESTING=ON -DBUILD_SHARED_LIBS=ON -DBUILD_TESTING=ON -DCMAKE_INSTALL_PREFIX=$PDC_DIR -DPDC_ENABLE_MPI=ON -DMERCURY_DIR=$MERCURY_DIR -DCMAKE_C_COMPILER=cc -DMPI_RUN_CMD=srun 
make -j 32 && make install
```

Let's run `ctest` now on interactively on a compute node:

### On Cori
```bash
salloc --nodes 1 --qos interactive --time 01:00:00 --constraint haswell
```
### On Perlmutter

```bash
salloc --nodes 1 --qos interactive --time 01:00:00 --constraint cpu --account=mxxxx
```

Once you are on the compute node, you can run `ctest`.

```bash
ctest
```

If all the tests pass, you can now specify the environment variables.

```bash
export LD_LIBRARY_PATH="$PDC_DIR/lib:$LD_LIBRARY_PATH"
export PATH="$PDC_DIR/include:$PDC_DIR/lib:$PATH"

echo 'export LD_LIBRARY_PATH=$PDC_DIR/lib:$LD_LIBRARY_PATH' >> $WORK_SPACE/pdc_env.sh
echo 'export PATH=$PDC_DIR/include:$PDC_DIR/lib:$PATH' >> $WORK_SPACE/pdc_env.sh
```

<!-- 

# Install libfabric
```
wget https://github.com/ofiwg/libfabric/archive/v1.11.2.tar.gz
tar xvzf v1.11.2.tar.gz
cd libfabric-1.11.2
mkdir install
export LIBFABRIC_DIR=$(pwd)/install

./autogen.sh
./configure --prefix=$LIBFABRIC_DIR CC=gcc CFLAG="-O2"
make -j8
make install

export LD_LIBRARY_PATH="$LIBFABRIC_DIR/lib:$LD_LIBRARY_PATH"
export PATH="$LIBFABRIC_DIR/include:$LIBFABRIC_DIR/lib:$PATH"
```
# Install Mercury
Make sure the ctest passes. PDC may not work without passing all the tests of Mercury.

Step 2 in the following is not required. It is a stable commit that has been used to test when these these instructions were written (mercury-2.0.1 release commit). One may skip it to use the current master branch of Mercury.
```
git clone https://github.com/mercury-hpc/mercury.git
cd mercury
git checkout cabb83758f9e07842dc34b0443d0873301fbdf91
git submodule update --init
export MERCURY_DIR=$(pwd)/install
mkdir install
cd install

cmake ../ -DCMAKE_INSTALL_PREFIX=$MERCURY_DIR -DCMAKE_C_COMPILER=gcc -DBUILD_SHARED_LIBS=ON -DBUILD_TESTING=ON -DNA_USE_OFI=ON -DNA_USE_SM=OFF
make
make install

ctest

export LD_LIBRARY_PATH="$MERCURY_DIR/lib:$LD_LIBRARY_PATH"
export PATH="$MERCURY_DIR/include:$MERCURY_DIR/lib:$PATH"
```
# Install PDC
One can replace mpicc to other available MPI compilers. -DCMAKE_C_FLAGS="-dynamic" is sometimes required for Cori. For example, on Cori, cc can be used to replace mpicc.
ctest contains both sequential and MPI tests for the PDC settings. These can be used to perform regression tests.
```
git clone https://github.com/hpc-io/pdc.git
cd pdc
mkdir install
cd install
export PDC_DIR=$(pwd)

cmake ../ -DBUILD_MPI_TESTING=ON -DBUILD_SHARED_LIBS=ON -DBUILD_TESTING=ON -DCMAKE_INSTALL_PREFIX=$PDC_DIR -DPDC_ENABLE_MPI=ON -DMERCURY_DIR=$MERCURY_DIR -DCMAKE_C_COMPILER=mpicc
make -j8

ctest

export LD_LIBRARY_PATH="$PDC_DIR/lib:$LD_LIBRARY_PATH"
export PATH="$PDC_DIR/include:$PDC_DIR/lib:$PATH"
```

# Environmental variables
During installation, we have set some environmental variables. These variables may disappear after the close the current session ends.
We recommend adding the following lines to ~/.bashrc. (One may also execute them manually after logging in).
The MERCURY_DIR and LIBFABRIC_DIR variables should be identical to the values that were set during the installation of Mercury and libfabric.
The install path is the path containing bin and lib directory, instead of the one containing the source code.
```
export PDC_DIR="where/you/installed/your/pdc"
export MERCURY_DIR="where/you/installed/your/mercury"
export LIBFABRIC_DIR="where/you/installed/your/libfabric"
export LD_LIBRARY_PATH="$LIBFABRIC_DIR/lib:$MERCURY_DIR/lib:$LD_LIBRARY_PATH"
export PATH="$PDC_DIR/include:$PDC_DIR/lib:LIBFABRIC_DIR/include:$LIBFABRIC_DIR/lib:$MERCURY_DIR/include:$MERCURY_DIR/lib:$PATH"
```

-->

## About Spack

One can also manage the path with Spack, which is a lot more easier to load and unload these libraries.

## Running PDC
The ctest under PDC install folder runs PDC examples using PDC APIs.
PDC needs to run at least two applications. The PDC servers need to be started first. 
The client programs that send I/O request to servers as Mercury RPCs are started next.

We provide a convenient function (mpi_text.sh) to start MPI tests. 
One needs to change the MPI launching function (mpiexec) with the relevant launcher on a system. 
On Cori at NERSC, the mpiexec argument needs to be changed to srun. On Theta, it is aprun. On Summit, it is jsrun.
```
cd $PDC_DIR/bin
./mpi_test.sh ./pdc_init mpiexec 2 4
```
This is test will start 2 processes for PDC servers. The client program ./pdc_init will start 4 processes. Similarly, one can run any of the client examples in ctest.
These source code will provide some knowledge of how to use PDC. For more reference, one may check the documentation folder in this repository.
# PDC on Cori.
Installation on Cori is not very different from a regular linux machine. Simply replacing all gcc/mpicc with the default cc compiler on Cori would work. "-DMPI_RUN_CMD=srun" is needed for ctest command later. In some instances and on some systems, unload darshan before installation may be needed.

For job allocation on Cori it is recommended to add "--gres=craynetwork:2" to the command:
```sh
salloc -C haswell -N 4 -t 01:00:00 -q interactive --gres=craynetwork:2
```
And to launch the PDC server and the client, add "--gres=craynetwork:1" before the executables:

* Run 4 server processes, each on one node in background:
```sh
srun -N 4 -n  4 -c 2 --mem=25600 --cpu_bind=cores --gres=craynetwork:1 --overlap ./bin/pdc_server.exe &
```

* Run 64 client processes that concurrently create 1000 objects in total:
```sh
srun -N 4 -n 64 -c 2 --mem=25600 --cpu_bind=cores --gres=craynetwork:1 --overlap ./bin/create_obj_scale -r 1000
```



