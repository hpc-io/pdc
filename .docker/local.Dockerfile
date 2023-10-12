# Note: Run `docker build -f .docker/Dockerfile -t pdc:latest .` from the root directory of the repository to build the docker image.

# Use Ubuntu Jammy (latest LTS) as the base image
FROM ubuntu:jammy

# Install necessary tools, MPICH, UUID library and developer files
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    mpich \
    libmpich-dev \
    uuid \
    uuid-dev \
    autoconf \
    libtool \
    cmake \
    cmake-curses-gui \
    wget \
    axel \
    curl \
    vim \
    nano \
    gdb \
    cgdb \
    curl \
    valgrind

# Set WORK_SPACE environment variable and create necessary directories
RUN mkdir -p /workspaces
ENV WORK_SPACE=/workspaces


# Clone the repositories
WORKDIR $WORK_SPACE/source
RUN git clone https://github.com/ofiwg/libfabric.git && \
    git clone https://github.com/mercury-hpc/mercury.git --recursive

COPY ./ ${WORK_SPACE}/source/pdc

ENV LIBFABRIC_SRC_DIR=$WORK_SPACE/source/libfabric
ENV MERCURY_SRC_DIR=$WORK_SPACE/source/mercury
ENV PDC_SRC_DIR=$WORK_SPACE/source/pdc
ENV LIBFABRIC_DIR=$WORK_SPACE/install/libfabric
ENV MERCURY_DIR=$WORK_SPACE/install/mercury
ENV PDC_DIR=$WORK_SPACE/install/pdc

RUN mkdir -p $LIBFABRIC_SRC_DIR \
    mkdir -p $MERCURY_SRC_DIR \
    mkdir -p $PDC_SRC_DIR \
    mkdir -p $LIBFABRIC_DIR \
    mkdir -p $MERCURY_DIR \
    mkdir -p $PDC_DIR


# Save the environment variables to a file
RUN echo "export LIBFABRIC_SRC_DIR=$WORK_SPACE/source/libfabric" > $WORK_SPACE/pdc_env.sh && \
    echo "export MERCURY_SRC_DIR=$WORK_SPACE/source/mercury" >> $WORK_SPACE/pdc_env.sh && \
    echo "export PDC_SRC_DIR=$WORK_SPACE/source/pdc" >> $WORK_SPACE/pdc_env.sh && \
    echo "export LIBFABRIC_DIR=$WORK_SPACE/install/libfabric" >> $WORK_SPACE/pdc_env.sh && \
    echo "export MERCURY_DIR=$WORK_SPACE/install/mercury" >> $WORK_SPACE/pdc_env.sh && \
    echo "export PDC_DIR=$WORK_SPACE/install/pdc" >> $WORK_SPACE/pdc_env.sh


# Build and install libfabric
WORKDIR $LIBFABRIC_SRC_DIR
RUN git checkout v1.18.0 && \
    ./autogen.sh && \
    ./configure --prefix=$LIBFABRIC_DIR CC=mpicc CFLAG="-O2" && \
    make clean && \
    make -j && make install && \
    make check

ENV LD_LIBRARY_PATH="$LIBFABRIC_DIR/lib:$LD_LIBRARY_PATH"
ENV PATH="$LIBFABRIC_DIR/include:$LIBFABRIC_DIR/lib:$PATH"
RUN echo 'export LD_LIBRARY_PATH=$LIBFABRIC_DIR/lib:$LD_LIBRARY_PATH' >> $WORK_SPACE/pdc_env.sh  && \
    echo 'export PATH=$LIBFABRIC_DIR/include:$LIBFABRIC_DIR/lib:$PATH' >> $WORK_SPACE/pdc_env.sh


# Build and install Mercury
WORKDIR $MERCURY_SRC_DIR
ENV MERCURY_CMAKE_FLAGS="-DCMAKE_INSTALL_PREFIX=$MERCURY_DIR -DCMAKE_C_COMPILER=mpicc -DBUILD_SHARED_LIBS=ON -DBUILD_TESTING=ON -DNA_USE_OFI=ON -DNA_USE_SM=OFF -DNA_OFI_TESTING_PROTOCOL=tcp "
RUN git checkout v2.2.0 \
    mkdir -p build 
WORKDIR ${MERCURY_SRC_DIR}/build
RUN cmake $MERCURY_CMAKE_FLAGS ../ && \
    make -j && make install && \
    ctest

# Set the environment variables
ENV LD_LIBRARY_PATH="$MERCURY_DIR/lib:$LD_LIBRARY_PATH"
ENV PATH="$MERCURY_DIR/include:$MERCURY_DIR/lib:$PATH"
RUN echo 'export LD_LIBRARY_PATH=$MERCURY_DIR/lib:$LD_LIBRARY_PATH' >> $WORK_SPACE/pdc_env.sh \
    echo 'export PATH=$MERCURY_DIR/include:$MERCURY_DIR/lib:$PATH' >> $WORK_SPACE/pdc_env.sh

# Build and install PDC
ENV PDC_CMAKE_FLAGS="-DBUILD_MPI_TESTING=ON -DBUILD_SHARED_LIBS=ON -DBUILD_TESTING=ON -DCMAKE_INSTALL_PREFIX=$PDC_DIR -DPDC_ENABLE_MPI=ON -DMERCURY_DIR=$MERCURY_DIR -DCMAKE_C_COMPILER=mpicc -DMPI_RUN_CMD=mpiexec "


WORKDIR $PDC_SRC_DIR
RUN rm -rf build && \
    mkdir -p build

# COPY ../  ${PDC_SRC_DIR}
# RUN ls -l $PDC_SRC_DIR

WORKDIR ${PDC_SRC_DIR}/build
RUN cmake $PDC_CMAKE_FLAGS ../ 2>&1 > ./cmake_config.log || echo "ignoring cmake config error and proceed" && \
    make -j && make install

# Set the environment variables
ENV LD_LIBRARY_PATH="$PDC_DIR/lib:$LD_LIBRARY_PATH"
ENV PATH="$PDC_DIR/include:$PDC_DIR/lib:$PATH"
RUN echo 'export LD_LIBRARY_PATH=$PDC_DIR/lib:$LD_LIBRARY_PATH' >> $WORK_SPACE/pdc_env.sh && \
    echo 'export PATH=$PDC_DIR/include:$PDC_DIR/lib:$PATH' >> $WORK_SPACE/pdc_env.sh


# WORKDIR $PDC_SRC_DIR/build
# RUN ctest