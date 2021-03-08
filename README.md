## Installation for PDC
This instruction is for installing PDC on Linux and Cray machines. Make sure you have GCC version at least 7 and MPI installed before you proceed.
For MPI, I would recommend MPICH. Follow the procedures in https://www.mpich.org/downloads/.
PDC depends on libfabric and mercury. We are going to install libfabric, mercury, and PDC step by step.
Make sure you record the environmental variables (lines that contains the export commands). They are needed for running PDC and make the libraries again.
# Install libfabric
```
0. wget https://github.com/ofiwg/libfabric/archive/v1.11.2.tar.gz
1. tar xvzf v1.11.2.tar.gz
2. cd libfabric-1.11.2
3. mkdir install
4. export LIBFABRIC_DIR=$(pwd)/install
5. ./autogen.sh
6. ./configure --prefix=$LIBFABRIC_DIR CC=gcc CFLAG="-O2"
7. make -j8
8. make install
9. export LD_LIBRARY_PATH="$LIBFABRIC_DIR/lib:$LD_LIBRARY_PATH"
10. export PATH="$LIBFABRIC_DIR/include:$LIBFABRIC_DIR/lib:$PATH"
```
# Install mercury
Make sure the ctest passes. Otherwise PDC will not work.
Step 2 is not required. It is a stable commit I am using when I write this instruction. You may skip it if you believe the current master branch of mercury works.
```
0. git clone https://github.com/mercury-hpc/mercury.git
1. cd mercury
2. git checkout e741051fbe6347087171f33119d57c48cb438438
3. git submodule update --init
4. export MERCURY_DIR=$(pwd)/install
5. mkdir install
6. cd install
7. cmake ../ -DCMAKE_INSTALL_PREFIX=$MERCURY_DIR -DCMAKE_C_COMPILER=gcc -DBUILD_SHARED_LIBS=ON -DBUILD_TESTING=ON -DNA_USE_OFI=ON -DNA_USE_SM=OFF -DCMAKE_C_FLAGS="-dynamic" -DCMAKE_CXX_FLAGS="-dynamic"
8. make
9. make install
10. ctest
11. export LD_LIBRARY_PATH="$MERCURY_DIR/lib:$LD_LIBRARY_PATH"
12. export PATH="$MERCURY_DIR/include:$MERCURY_DIR/lib:$PATH"
```
# Install PDC
You can replace mpicc to whatever MPI compilers you are using. For example, on Cori, you may need to use cc.
The ctest contains both sequential and MPI tests for our settings. These regression tests should work.
```
0. git clone https://github.com/hpc-io/pdc.git
1. cd pdc
2. git checkout qiao_develop
3. cd src
4. mkdir install
5. cd install
6. export PDC_DIR=$(pwd)
7. cmake ../ -DBUILD_MPI_TESTING=ON -DBUILD_SHARED_LIBS=ON -DBUILD_TESTING=ON -DCMAKE_INSTALL_PREFIX=$PDC_DIR -DPDC_ENABLE_MPI=ON -DMERCURY_DIR=$MERCURY_DIR -DCMAKE_C_COMPILER=mpicc -DCMAKE_C_FLAGS="-dynamic"
8. make
9. make -j8
10. ctest
```

# Environmental variables
During installation, we have set some environmental variables. These variables will disappear when you close the current shell.
I recommend adding the following lines to ~/.bashrc. (you can also manually execute them when you login).
The MERCURY_DIR and LIBFABRIC_DIR should be identical to the values you set during your installations of Mercury and Libfabric.
Remember, the install path is the path containing bin and lib directory, instead of the one containing source code.
```
export PDC_DIR="where/you/installed/your/pdc"
export MERCURY_DIR="where/you/installed/your/mercury"
export LIBFABRIC_DIR="where/you/installed/your/libfabric"
export LD_LIBRARY_PATH="$LIBFABRIC_DIR/lib:$MERCURY_DIR/lib:$LD_LIBRARY_PATH"
export PATH="$LIBFABRIC_DIR/include:$LIBFABRIC_DIR/lib:$MERCURY_DIR/include:$MERCURY_DIR/lib:$PATH"
```
You can also manage the path with Spack, which is a lot more easier to load and unload these libraries.
## Running PDC
The ctest under PDC install folder runs PDC examples using PDC APIs.
PDC needs to run at least two applications. First, you need to start servers. Then, you can run client programs that send I/O request to servers as mercury RPCs.
For example, you can do the following. On Cori, you need to change the mpiexec argument to srun. On Theta, it is aprun. On Summit, it is jsrun.
```
cd $PDC_DIR/bin
./mpi_test ./pdc_init mpiexec 2 4
```
This is test will start 2 processes for PDC servers. The client program ./pdc_init will start 4 processes. Similarly, you can run any of the client examples in ctest.
These source code will provide you some knowledge of how to use PDC. For more reference, you may check the documentation folder in this repository.
# PDC on Cori.
Installation on Cori is not very different from a regular linux machine. Simply replace all gcc/mpicc with the default cc compiler on Cori.



