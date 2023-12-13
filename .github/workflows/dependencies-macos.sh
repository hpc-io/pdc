#!/usr/bin/env bash

set -eu -o pipefail

brew install open-mpi autoconf automake libtool

# libfabric
wget https://github.com/ofiwg/libfabric/archive/refs/tags/v1.15.2.tar.gz
tar xf v1.15.2.tar.gz
cd libfabric-1.15.2
./autogen.sh
./configure --disable-usnic --disable-mrail --disable-rstream --disable-perf --disable-efa --disable-psm2 --disable-psm --disable-verbs --disable-shm --disable-static --disable-silent-rules
make -j2 && sudo make install
make check
cd ..

# Mercury
git clone --recursive https://github.com/mercury-hpc/mercury.git
cd mercury
git checkout v2.2.0
mkdir build && cd build
cmake ../  -DCMAKE_C_COMPILER=gcc -DBUILD_SHARED_LIBS=ON -DBUILD_TESTING=ON -DNA_USE_OFI=ON -DNA_USE_SM=OFF -DMERCURY_USE_CHECKSUMS=OFF -DNA_OFI_TESTING_PROTOCOL=sockets
make -j2 && sudo make install
ctest
