#!/bin/bash


WORK_SPACE_INITIALIZED_FILE=/workspaces/.workspace_initialized

if ! [ -f $WORK_SPACE_INITIALIZED_FILE ]; then
    /bin/bash /workspaces/pdc/.devcontainer/post-start.sh
    watch -t -n 5 'echo "Press Ctrl+C when there is no building processes."; echo "Number of initial PDC building processes:"; ps -ef | grep make | grep -v -c grep'
else
    echo "Welcome Back!"
fi

/bin/bash