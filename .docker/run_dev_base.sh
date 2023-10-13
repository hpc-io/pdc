#!/bin/bash

DEFAULT_WORKSPACE=/workspaces/pdc

docker run -it -v $(pwd):${DEFAULT_WORKSPACE} -w ${DEFAULT_WORKSPACE}  zhangwei217245/pdc_dev_base /bin/bash 