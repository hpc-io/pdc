#!/bin/bash

set -e

./rebuild.sh

echo "killing previous pdc_server.exe"
pkill -f pdc_server.exe || true

echo "deleting existing data"
rm -rf /home/nlewi26/src/work_space/source/pdc/build/bin/pdc_data

pushd ./build/bin
$1 ./pdc_server.exe
popd
