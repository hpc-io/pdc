#!/bin/bash

set -eux

pushd ./build
ctest -V -L transform --output-on-failure
popd
