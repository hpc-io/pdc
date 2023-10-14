# Note: Run `docker build -f .devcontainer/Dockerfile -t pdc:latest .` from the root directory of the repository to build the docker image.

# Use Ubuntu Jammy (latest LTS) as the base image
ARG ARCH=
FROM ${ARCH}ubuntu:jammy

# Install necessary tools, MPICH, UUID library and developer files
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    mpich \
    libmpich-dev \
    libhdf5-mpi-dev \
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
    valgrind \
    python3 

# Install Oh My Bash
RUN bash -c "$(curl -fsSL https://raw.githubusercontent.com/ohmybash/oh-my-bash/master/tools/install.sh)" && \
    sed -i 's/OSH_THEME="font"/OSH_THEME="powerline-multiline"/g' ~/.bashrc

# Install Julia

# Set a default value for JULIA_URL, assuming x86_64 (amd64) as default
ARG JULIA_URL=https://julialang-s3.julialang.org/bin/linux/x64/1.6/julia-1.6.7-linux-x86_64.tar.gz

# If the architecture is arm64, set the JULIA_URL to the arm64 version
RUN if [ "$ARCH" = "arm64v8/" ]; then \
    JULIA_URL=https://julialang-s3.julialang.org/bin/linux/aarch64/1.6/julia-1.6.7-linux-aarch64.tar.gz; \
    elif [ "${ARCH}" = "amd64/" ]; then \
    JULIA_URL=https://julialang-s3.julialang.org/bin/linux/x64/1.6/julia-1.6.7-linux-x86_64.tar.gz; \
    fi

RUN mkdir -p /opt/julia && wget -O - $JULIA_URL | tar -xz -C /opt/julia --strip-components=1 && \
    ln -s /opt/julia/bin/julia /usr/local/bin/julia

ENV JULIA_HOME=/opt/julia

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
RUN echo 'source $HOME/.cargo/env' >> ~/.bashrc


# Set WORK_SPACE environment variable and create necessary directories
ENV WORK_SPACE=/home/project
RUN mkdir -p $WORK_SPACE

# Clone the repositories
WORKDIR $WORK_SPACE/source
RUN git clone https://github.com/ofiwg/libfabric.git && \
    git clone https://github.com/mercury-hpc/mercury.git --recursive

ENV LIBFABRIC_SRC_DIR=$WORK_SPACE/source/libfabric
ENV LIBFABRIC_DIR=$WORK_SPACE/install/libfabric
ENV MERCURY_SRC_DIR=$WORK_SPACE/source/mercury
ENV MERCURY_DIR=$WORK_SPACE/install/mercury

ENV PDC_SRC_DIR=$WORK_SPACE/source/pdc
ENV PDC_DIR=$WORK_SPACE/install/pdc

RUN mkdir -p $LIBFABRIC_SRC_DIR && \
    mkdir -p $MERCURY_SRC_DIR && \
    mkdir -p $PDC_SRC_DIR && \
    mkdir -p $LIBFABRIC_DIR && \
    mkdir -p $MERCURY_DIR && \
    mkdir -p $PDC_DIR


# Save the environment variables to a file
RUN echo "export LIBFABRIC_SRC_DIR=$WORK_SPACE/source/libfabric" > $WORK_SPACE/pdc_env.sh && \
    echo "export LIBFABRIC_DIR=$WORK_SPACE/install/libfabric" >> $WORK_SPACE/pdc_env.sh && \
    echo "export MERCURY_SRC_DIR=$WORK_SPACE/source/mercury" >> $WORK_SPACE/pdc_env.sh && \
    echo "export MERCURY_DIR=$WORK_SPACE/install/mercury" >> $WORK_SPACE/pdc_env.sh && \
    echo "export PDC_SRC_DIR=$WORK_SPACE/source/pdc" >> $WORK_SPACE/pdc_env.sh && \
    echo "export PDC_DIR=$WORK_SPACE/install/pdc" >> $WORK_SPACE/pdc_env.sh


# Build and install libfabric
WORKDIR $LIBFABRIC_SRC_DIR
RUN git checkout v1.18.0 && \
    ./autogen.sh && \
    ./configure --prefix=$LIBFABRIC_DIR CC=mpicc CFLAG="-O2" && \
    make clean && \
    make -j 8 && make install && \
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
    make -j 16 && make install && \
    ctest

# Set the environment variables
ENV LD_LIBRARY_PATH="$MERCURY_DIR/lib:$LD_LIBRARY_PATH"
ENV PATH="$MERCURY_DIR/include:$MERCURY_DIR/lib:$PATH"
RUN echo 'export LD_LIBRARY_PATH=$MERCURY_DIR/lib:$LD_LIBRARY_PATH' >> $WORK_SPACE/pdc_env.sh \
    echo 'export PATH=$MERCURY_DIR/include:$MERCURY_DIR/lib:$PATH' >> $WORK_SPACE/pdc_env.sh


ENV PDC_CMAKE_FLAGS="-DBUILD_MPI_TESTING=ON -DBUILD_SHARED_LIBS=ON -DUSE_SYSTEM_HDF5=OFF -DBUILD_TESTING=ON -DCMAKE_INSTALL_PREFIX=$PDC_DIR -DPDC_ENABLE_MPI=ON -DMERCURY_DIR=$MERCURY_DIR -DCMAKE_C_COMPILER=mpicc -DMPI_RUN_CMD=mpiexec "

ENTRYPOINT [ "/workspaces/pdc/.devcontainer/post-attach.sh" ]