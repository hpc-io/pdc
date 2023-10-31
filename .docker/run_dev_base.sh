#!/bin/bash

DEFAULT_WORKSPACE=/workspaces/pdc

LAST_PDC_DEV_CONTAINER=$(docker ps -a | grep pdc_dev_base | head -1 | awk '{print $NF}') 

# if this is empty, then we need to create a new container
if [ -z "$LAST_PDC_DEV_CONTAINER" ]; then
    echo "No existing pdc_dev_base container found, creating a new one"
    docker image rm -f hpcio/pdc-dev-base:latest
    docker create -it -v $(pwd):${DEFAULT_WORKSPACE} -w ${DEFAULT_WORKSPACE} --entrypoint /bin/bash hpcio/pdc-dev-base:latest
    sleep 1
    LAST_PDC_DEV_CONTAINER=$(docker ps -a | grep pdc_dev_base | head -1 | awk '{print $NF}') 
    echo "Created pdc_dev_base container: $LAST_PDC_DEV_CONTAINER. To stop it, run 'docker stop $LAST_PDC_DEV_CONTAINER'"
    docker start $LAST_PDC_DEV_CONTAINER
    echo "Wait for 5 seconds for the container to start."
    sleep 5
    docker exec -it $LAST_PDC_DEV_CONTAINER /bin/sh -c "/bin/bash ${DEFAULT_WORKSPACE}/.devcontainer/post-attach.sh"
else
    echo "Found existing pdc_dev_base container $LAST_PDC_DEV_CONTAINER, start it. To stop it, run 'docker stop $LAST_PDC_DEV_CONTAINER'"
    docker start $LAST_PDC_DEV_CONTAINER
    docker exec -it $LAST_PDC_DEV_CONTAINER /bin/bash -c "/bin/bash ${DEFAULT_WORKSPACE}/.devcontainer/post-attach.sh"
fi