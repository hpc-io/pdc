#!/bin/bash

nohup /bin/bash /workspaces/pdc/.devcontainer/post-create.sh 2>&1 > /workspaces/pdc_install.out &

echo "Wait for 10 seconds for the building processes to start."
sleep 10s
