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
Make sure the ctest passes at step 7.Otherwise PDC will not work.
```
0. git clone https://github.com/mercury-hpc/mercury.git
1. cd mercury
2. export MERCURY_DIR=$(pwd)/install
3. mkdir install
4. cd install
5. cmake ../ -DCMAKE_INSTALL_PREFIX=$MERCURY_DIR -DCMAKE_C_COMPILER=gcc -DBUILD_SHARED_LIBS=ON -DBUILD_TESTING=ON -DNA_USE_OFI=ON -DNA_USE_SM=OFF
6. make
7. make install
8. ctest
9. export LD_LIBRARY_PATH="$MERCURY_DIR/lib:$LD_LIBRARY_PATH"
10. export PATH="$MERCURY_DIR/include:$MERCURY_DIR/lib:$PATH"
```
# Install PDC
```
0. git clone https://github.com/hpc-io/pdc.git
1. cd pdc
2. git checkout qiao_develop
3. cd src
4. mkdir install
5. cd install
6. export PDC_DIR=$(pwd)
7. cmake ../ -DBUILD_MPI_TESTING=ON -DBUILD_SHARED_LIBS=ON -DBUILD_TESTING=ON -DCMAKE_INSTALL_PREFIX=$PDC_DIR -DPDC_ENABLE_MPI=ON -DMERCURY_DIR=$MERCURY_DIR -DCMAKE_C_COMPILER=mpicc
8. make
9. make -j8
10. ctest
```
