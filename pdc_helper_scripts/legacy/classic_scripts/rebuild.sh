#!/bin/bash

pushd build
make -j$(nproc)
rm -rf /pscratch/sd/n/nlewi26/src/work_space/install/pdc/*
make install
popd
