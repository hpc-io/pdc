#!/bin/bash -l

#REGSBATCH -q regular
#DBGSBATCH -q debug
#SBATCH -N NODENUM
#REGSBATCH -t 1:00:00
#DBGSBATCH -t 0:30:00
#SBATCH -C cpu
#SBATCH -J JOBNAME
#SBATCH -A m2621
#SBATCH -o o%j.JOBNAME.out
#SBATCH -e o%j.JOBNAME.out

# export PDC_DEBUG=0

export PDC_TMPDIR=$SCRATCH/data/pdc/conf

rm -rf $PDC_TMPDIR/*

REPEAT=1

N_NODE=NODENUM
NCLIENT=31
# NCLIENT=126

export PDC_TMPDIR=${PDC_TMPDIR}/$N_NODE
mkdir -p $PDC_TMPDIR

let TOTALPROC=$NCLIENT*$N_NODE

EXECPATH=/global/cfs/cdirs/m2621/wzhang5/perlmutter/install/pdc/share/test/bin
SERVER=$EXECPATH/pdc_server.exe
CLIENT=$EXECPATH/kvtag_add_get_scale
CLOSE=$EXECPATH/close_server

chmod +x $EXECPATH/*

NUM_OBJ=$((1024*1024*1024))
NUM_TAGS=$NUM_OBJ
NUM_QUERY=$((NUM_OBJ))

date


echo ""
echo "============="
echo "$i Init server"
echo "============="
stdbuf -i0 -o0 -e0 srun -N $N_NODE -n $((N_NODE*1)) -c 2 --cpu_bind=cores $SERVER &
sleep 5


echo "============================================"
echo "KVTAGS with $N_NODE nodes"
echo "============================================"
stdbuf -i0 -o0 -e0 srun -N $N_NODE -n $TOTALPROC -c 2 --cpu_bind=cores $CLIENT $NUM_OBJ $NUM_TAGS $NUM_QUERY

echo ""
echo "================="
echo "$i Closing server"
echo "================="
stdbuf -i0 -o0 -e0 srun -N 1 -n 1 -c 2 --mem=25600 --cpu_bind=cores $CLOSE

date
