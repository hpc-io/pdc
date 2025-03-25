#!/bin/bash

pushd build
make -j$(nproc)
popd
