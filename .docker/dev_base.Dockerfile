# Note: Run `docker build -f .devcontainer/Dockerfile -t pdc:latest .` from the root directory of the repository to build the docker image.

# Use Ubuntu Jammy (latest LTS) as the base image
ARG ARCH
FROM ${ARCH}ubuntu:jammy

ARG ARCH_CODE

ENV JULIA_HOME=/opt/julia
ENV WORK_SPACE=/home/project
ENV LIBFABRIC_SRC_DIR=$WORK_SPACE/source/libfabric
ENV LIBFABRIC_DIR=$WORK_SPACE/install/libfabric
ENV MERCURY_SRC_DIR=$WORK_SPACE/source/mercury
ENV MERCURY_DIR=$WORK_SPACE/install/mercury

ENV PDC_SRC_DIR=$WORK_SPACE/source/pdc
ENV PDC_DIR=$WORK_SPACE/install/pdc

ENV LD_LIBRARY_PATH="$LIBFABRIC_DIR/lib:$LD_LIBRARY_PATH"
ENV PATH="$LIBFABRIC_DIR/include:$LIBFABRIC_DIR/lib:$PATH"
ENV LD_LIBRARY_PATH="$MERCURY_DIR/lib:$LD_LIBRARY_PATH"
ENV PATH="$MERCURY_DIR/include:$MERCURY_DIR/lib:$PATH"


ENV MERCURY_CMAKE_FLAGS="-DCMAKE_INSTALL_PREFIX=$MERCURY_DIR -DCMAKE_C_COMPILER=mpicc -DBUILD_SHARED_LIBS=ON -DBUILD_TESTING=ON -DNA_USE_OFI=ON -DNA_USE_SM=OFF -DNA_OFI_TESTING_PROTOCOL=tcp "
ENV CLANG_FORMAT_PATH=$WORK_SPACE/software/clang-format-lint-action/clang-format/clang-format10

# Install necessary tools, MPICH, UUID library and developer files
RUN echo "ARCH=${ARCH}" && echo "ARCH_CODE=${ARCH_CODE}" && sleep 3 && mkdir -p $WORK_SPACE && \
    mkdir -p $WORK_SPACE/software && \
    mkdir -p $WORK_SPACE/source && \
    mkdir -p $WORK_SPACE/install && \
    mkdir -p $LIBFABRIC_SRC_DIR && \
    mkdir -p $MERCURY_SRC_DIR && \
    mkdir -p $PDC_SRC_DIR && \
    mkdir -p $LIBFABRIC_DIR && \
    mkdir -p $MERCURY_DIR && \
    mkdir -p $PDC_DIR && \
    rm -rf $LIBFABRIC_SRC_DIR/* && \
    rm -rf $MERCURY_SRC_DIR/* && \
    rm -rf $PDC_SRC_DIR/* && \
    rm -rf $LIBFABRIC_DIR/* && \
    rm -rf $MERCURY_DIR/* && \
    rm -rf $PDC_DIR/* && \
    echo "export LIBFABRIC_SRC_DIR=$WORK_SPACE/source/libfabric" > $WORK_SPACE/pdc_env.sh && \
    echo "export LIBFABRIC_DIR=$WORK_SPACE/install/libfabric" >> $WORK_SPACE/pdc_env.sh && \
    echo "export MERCURY_SRC_DIR=$WORK_SPACE/source/mercury" >> $WORK_SPACE/pdc_env.sh && \
    echo "export MERCURY_DIR=$WORK_SPACE/install/mercury" >> $WORK_SPACE/pdc_env.sh && \
    echo "export PDC_SRC_DIR=$WORK_SPACE/source/pdc" >> $WORK_SPACE/pdc_env.sh && \
    echo "export PDC_DIR=$WORK_SPACE/install/pdc" >> $WORK_SPACE/pdc_env.sh && \
    echo 'export LD_LIBRARY_PATH=$LIBFABRIC_DIR/lib:$LD_LIBRARY_PATH' >> $WORK_SPACE/pdc_env.sh  && \
    echo 'export PATH=$LIBFABRIC_DIR/include:$LIBFABRIC_DIR/lib:$PATH' >> $WORK_SPACE/pdc_env.sh && \
    echo 'export LD_LIBRARY_PATH=$MERCURY_DIR/lib:$LD_LIBRARY_PATH' >> $WORK_SPACE/pdc_env.sh \
    echo 'export PATH=$MERCURY_DIR/include:$MERCURY_DIR/lib:$PATH' >> $WORK_SPACE/pdc_env.sh && \
    apt-get update && apt-get install -y \
    build-essential \
    git \
    mpich \
    libmpich-dev \
    libhdf5-dev \
    libhdf5-mpich-dev \
    libtiff5 \
    libtiff5-dev \
    uuid \
    uuid-dev \
    autoconf \
    libtool \
    cmake \
    cmake-curses-gui \
    wget \
    axel \
    curl \
    bc \
    vim \
    nano \
    gdb \
    cgdb \
    curl \
    valgrind \
    python3 && \
    cd $WORK_SPACE/software && \
    bash -c "$(curl -fsSL https://raw.githubusercontent.com/ohmybash/oh-my-bash/master/tools/install.sh)" && \
    sed -i 's/OSH_THEME="font"/OSH_THEME="powerline-multiline"/g' ~/.bashrc && \
    echo "https://julialang-s3.julialang.org/bin/linux/aarch64/1.6/julia-1.6.7-linux-aarch64.tar.gz" > $WORK_SPACE/software/julia_url_arm64v8.txt && \
    echo "https://julialang-s3.julialang.org/bin/linux/x64/1.6/julia-1.6.7-linux-x86_64.tar.gz" > $WORK_SPACE/software/julia_url_amd64.txt && \
    echo $(cat $WORK_SPACE/software/julia_url_${ARCH_CODE}.txt) && sleep 3 && \
    mkdir -p /opt/julia && wget -O - $(cat $WORK_SPACE/software/julia_url_${ARCH_CODE}.txt) | tar -xz -C /opt/julia --strip-components=1 && \
    ln -s /opt/julia/bin/julia /usr/local/bin/julia && \
    rm -rf $WORK_SPACE/software/julia_url_*.txt && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && \
    echo 'source $HOME/.cargo/env' >> ~/.bashrc && \
    git clone https://github.com/DoozyX/clang-format-lint-action.git && \
    git clone https://github.com/ofiwg/libfabric.git ${LIBFABRIC_SRC_DIR} && \
    git clone https://github.com/mercury-hpc/mercury.git --recursive ${MERCURY_SRC_DIR} && \
    cd $LIBFABRIC_SRC_DIR && \
    git checkout v1.18.0 && \
    ./autogen.sh && \
    ./configure --prefix=$LIBFABRIC_DIR CC=mpicc CFLAG="-O2" && \
    make clean && \
    make -j 8 && make install && \
    make check && \
    cd $MERCURY_SRC_DIR && \
    git checkout v2.2.0 && \
    mkdir -p build && \
    cd ${MERCURY_SRC_DIR}/build && \
    cmake $MERCURY_CMAKE_FLAGS ../ && \
    make -j 16 && make install && \
    ctest && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /tmp/* && \
    rm -rf /var/tmp/* 

ENV PDC_CMAKE_FLAGS="-DBUILD_MPI_TESTING=ON -DBUILD_SHARED_LIBS=ON -DBUILD_TESTING=ON -DCMAKE_INSTALL_PREFIX=$PDC_DIR -DPDC_ENABLE_MPI=ON -DMERCURY_DIR=$MERCURY_DIR -DCMAKE_C_COMPILER=mpicc -DMPI_RUN_CMD=mpiexec "

ENTRYPOINT [ "/workspaces/pdc/.devcontainer/post-attach.sh" ]