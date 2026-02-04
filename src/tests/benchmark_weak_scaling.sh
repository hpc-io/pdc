#!/bin/bash

#module load PrgEnv-gnu
#module load libfabric
#export SUPERCOMPUTER=perlmutter
#
## note: might need for multi-node testing
##export FI_PROVIDER=tcp\;ofi_rxm
#
#export WORK_SPACE=/global/homes/r/raqib/pdc_home
#source $WORK_SPACE/pdc_env.sh
#
#export PDC_TMPDIR=$SCRATCH/pdc_tmp

## Weak scaling (fix the amount of workload per-worker)
 #10K/100 -n 2 -c 2 will implies in respect to checkpointing (10k + 10k*100) = 1010000
 #20K/100 -n 4 -c 2 will implies in respect to checkpointing (20k + 20k*100) = 2020000
 #40K/100 -n 8 -c 2 will implies in respect to checkpointing (40k + 40k*100) = 4040000
 #80K/100 -n 16 -c 2 will implies in respect to checkpointing (80k + 80k*100) = 8080000

# parse log using:
  # less 10K-100__2-2.out | grep "Total checkpoint time\|total close time\|===== Run\|total restart time"
  # less 20K-100__4-2.out | grep "Total checkpoint time\|total close time\|===== Run\|total restart time"
  # less 40K-100__8-2.out | grep "Total checkpoint time\|total close time\|===== Run\|total restart time"
  # less 80K-100__16-2.out | grep "Total checkpoint time\|total close time\|===== Run\|total restart time"

for i in {1..3}; do
    echo "===== Run $i ===== 10K-100__2-2"
    echo "===== Run $i =====" >> 10K-100__2-2.out
    ./run_checkpoint_restart_mpi_test_with_prog_param.sh \
        srun 2 1 \
        "./kvtag_add_scale_mpi 10000 100" \
        "./kvtag_get_verify_scale_mpi 10000 100" \
        >> 10K-100__2-2.out 2>&1
done

for i in {1..3}; do
    echo "===== Run $i ===== 20K-100__4-2"
    echo "===== Run $i =====" >> 20K-100__4-2.out
    ./run_checkpoint_restart_mpi_test_with_prog_param.sh \
        srun 4 1 \
        "./kvtag_add_scale_mpi 20000 100" \
        "./kvtag_get_verify_scale_mpi 20000 100" \
        >> 20K-100__4-2.out 2>&1
done

for i in {1..3}; do
    echo "===== Run $i ===== 40K-100__8-2"
    echo "===== Run $i =====" >> 40K-100__8-2.out
    ./run_checkpoint_restart_mpi_test_with_prog_param.sh \
        srun 8 1 \
        "./kvtag_add_scale_mpi 40000 100" \
        "./kvtag_get_verify_scale_mpi 40000 100" \
        >> 40K-100__8-2.out 2>&1
done

for i in {1..3}; do
    echo "===== Run $i ===== 80K-100__16-2"
    echo "===== Run $i =====" >> 80K-100__16-2.out
    ./run_checkpoint_restart_mpi_test_with_prog_param.sh \
        srun 16 1 \
        "./kvtag_add_scale_mpi 80000 100" \
        "./kvtag_get_verify_scale_mpi 80000 100" \
        >> 80K-100__16-2.out 2>&1
done
