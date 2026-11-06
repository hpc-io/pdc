#!/usr/bin/env bash

set -eu -o pipefail

sudo apt-get update
sudo apt-get install libopenmpi-dev libhdf5-dev uuid-dev cmake libzstd-dev

# libfabric
wget https://github.com/ofiwg/libfabric/archive/refs/tags/v1.12.1.tar.gz
tar xf v1.12.1.tar.gz
cd libfabric-1.12.1
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
cmake ../  -DCMAKE_C_COMPILER=gcc -DBUILD_SHARED_LIBS=ON -DBUILD_TESTING=ON -DNA_USE_OFI=ON -DNA_USE_SM=OFF -DMERCURY_USE_CHECKSUMS=OFF -DCMAKE_POLICY_VERSION_MINIMUM=3.5
make -j2 && sudo make install
ctest
cd ../..

# In-flight transformation + analysis optional dependencies (ZFP, SZ3,
# TurboPFor, libsodium -- see the "Optional dependencies" table on PR #317).
# CUDA/cuSZ (GPU variants) are intentionally left out: this runner has no
# GPU, so those tests never register with ctest and don't need to build.
git clone --branch 1.0.1 --depth 1 https://github.com/LLNL/zfp.git zfp
cd zfp && mkdir build && cd build
cmake -DCMAKE_INSTALL_PREFIX="$HOME/opt/zfp" ../
make -j2 && make install
cd ../..

git clone --branch v3.4.0 --depth 1 https://github.com/szcompressor/SZ3.git sz3
cd sz3 && mkdir build && cd build
cmake -DCMAKE_INSTALL_PREFIX="$HOME/opt/sz3" ../
make -j2 && make install
cd ../..

git clone https://github.com/powturbo/TurboPFor-Integer-Compression.git turbopfor
cd turbopfor
git checkout 1f646d98d65ee91befd3066f351f2cf386593442
make libic.a
cd ..

# libsodium -- built from source into a plain (non-multiarch) prefix rather
# than apt's libsodium-dev. PDC's find_library() for this runs before
# project(PDC C), so CMake doesn't know this system's multiarch library
# triplet yet and can't see apt's /usr/lib/x86_64-linux-gnu/libsodium.so.
wget https://download.libsodium.org/libsodium/releases/libsodium-1.0.20.tar.gz
tar xf libsodium-1.0.20.tar.gz
cd libsodium-1.0.20
./configure --prefix="$HOME/opt/libsodium"
make -j2 && make install
cd ..

echo "ZFP_ROOT_DIR=$HOME/opt/zfp" >> "$GITHUB_ENV"
echo "SZ_CPU_DIR=$HOME/opt/sz3" >> "$GITHUB_ENV"
echo "TURBO_DIR=$GITHUB_WORKSPACE/turbopfor" >> "$GITHUB_ENV"
echo "LIBSODIUM_ROOT_DIR=$HOME/opt/libsodium" >> "$GITHUB_ENV"
echo "LD_LIBRARY_PATH=$HOME/opt/zfp/lib:$HOME/opt/sz3/lib:$HOME/opt/libsodium/lib:${LD_LIBRARY_PATH:-}" >> "$GITHUB_ENV"
