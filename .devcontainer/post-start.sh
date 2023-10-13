#!/bin/bash


ln -s /workspaces/pdc /home/codespace/source/pdc 
mkdir -p /workspaces/install
mkdir -p /workspaces/source
ln -s $PDC_SRC_DIR /workspaces/source/pdc
ln -s $PDC_DIR  /workspaces/install/pdc

export PDC_SRC_DIR=/workspaces/source/pdc

# Build and install PDC
export PDC_CMAKE_FLAGS="-DBUILD_MPI_TESTING=ON -DBUILD_SHARED_LIBS=ON -DBUILD_TESTING=ON -DCMAKE_INSTALL_PREFIX=$PDC_DIR -DPDC_ENABLE_MPI=ON -DMERCURY_DIR=$MERCURY_DIR -DCMAKE_C_COMPILER=mpicc -DMPI_RUN_CMD=mpiexec "

cd $PDC_SRC_DIR
rm -rf build && mkdir -p build


cd ${PDC_SRC_DIR}/build
cmake $PDC_CMAKE_FLAGS ../ 2>&1 > ./cmake_config.log || echo "ignoring cmake config error and proceed"
make -j && make install

# Set the environment variables
export LD_LIBRARY_PATH="$PDC_DIR/lib:$LD_LIBRARY_PATH"
export PATH="$PDC_DIR/include:$PDC_DIR/lib:$PATH"
echo 'export LD_LIBRARY_PATH=$PDC_DIR/lib:$LD_LIBRARY_PATH' >> $WORK_SPACE/pdc_env.sh
echo 'export PATH=$PDC_DIR/include:$PDC_DIR/lib:$PATH' >> $WORK_SPACE/pdc_env.sh


cd $PDC_SRC_DIR/build
# ctest