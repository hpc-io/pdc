# Note: Run `docker build -f .docker/Dockerfile -t pdc:latest .` from the root directory of the repository to build the docker image.

# Use Ubuntu Jammy (latest LTS) as the base image
FROM zhangwei217245/pdc_dev_base:latest

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