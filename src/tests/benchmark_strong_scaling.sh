#!/bin/bash

#export SUPERCOMPUTER=perlmutter
#module load PrgEnv-gnu
#module load libfabric
#
## note: might need for multi-node testing
##export FI_PROVIDER=tcp\;ofi_rxm
#
#export WORK_SPACE=/global/homes/r/raqib/pdc_home
#source $WORK_SPACE/pdc_env.sh
#
#export PDC_TMPDIR=$SCRATCH/pdc_tmp

## Strong scaling (fix the total amount of work and then change the number of workers)
#Fix work to 100K/1000 and then changing the number of parallel servers:
 #-n 2 -c 2
 #-n 4 -c 2
 #-n 8 -c 2
 #-n 16 -c 2

# parse log using:
  # less 100K-100__2-2.out | grep "Total checkpoint time\|total close time\|===== Run\|total restart time"
  # less 100K-100__4-2.out | grep "Total checkpoint time\|total close time\|===== Run\|total restart time"
  # less 100K-100__8-2.out | grep "Total checkpoint time\|total close time\|===== Run\|total restart time"
  # less 100K-100__16-2.out | grep "Total checkpoint time\|total close time\|===== Run\|total restart time"

for i in {1..3}; do
    echo "===== Run $i ===== 100K-100__2-2"
    echo "===== Run $i =====" >> 100K-100__2-2.out
    ./run_checkpoint_restart_mpi_test_with_prog_param.sh \
        srun 2 1 \
        "./kvtag_add_scale_mpi 100000 100" \
        "./kvtag_get_verify_scale_mpi 100000 100" \
        >> 100K-100__2-2.out 2>&1
done

for i in {1..3}; do
    echo "===== Run $i ===== 100K-100__4-2"
    echo "===== Run $i =====" >> 100K-100__4-2.out
    ./run_checkpoint_restart_mpi_test_with_prog_param.sh \
        srun 4 1 \
        "./kvtag_add_scale_mpi 100000 100" \
        "./kvtag_get_verify_scale_mpi 100000 100" \
        >> 100K-100__4-2.out 2>&1
done

for i in {1..3}; do
    echo "===== Run $i ===== 100K-100__8-2"
    echo "===== Run $i =====" >> 100K-100__8-2.out
    ./run_checkpoint_restart_mpi_test_with_prog_param.sh \
        srun 8 1 \
        "./kvtag_add_scale_mpi 100000 100" \
        "./kvtag_get_verify_scale_mpi 100000 100" \
        >> 100K-100__8-2.out 2>&1
done

for i in {1..3}; do
    echo "===== Run $i ===== 100K-100__16-2"
    echo "===== Run $i =====" >> 100K-100__16-2.out
    ./run_checkpoint_restart_mpi_test_with_prog_param.sh \
        srun 16 1 \
        "./kvtag_add_scale_mpi 100000 100" \
        "./kvtag_get_verify_scale_mpi 100000 100" \
        >> 100K-100__16-2.out 2>&1
done

