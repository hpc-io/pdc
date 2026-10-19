#!/bin/bash

set -e

echo "killing previous pdc_server"
pkill -f pdc_server || true
lsof -t -i :8000 | xargs -r kill -9

echo "deleting existing data"
rm -rf /home/nlewi26/src/work_space/source/pdc/build/bin/pdc_data

pushd ./build/bin
export PDC_DEBUG=1
./pdc_server restart
popd
