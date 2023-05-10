# LLSM_Importer Tutorial

This is a tutorial for you to run llsm_importer on Perlmutter supercomputer at NERSC. 

## Prerequisite

Before building and installing LLSM_importer tool, you need to make sure you install PDC correctly. Check out the latest update on the `develop` branch of `PDC`. Please refer to [Proactive Data Containers (PDC) Installation Guide](../README.md) 

Once you finish all the steps in the installation guide above, you should have environment variable `$WORK_SPACE` defined.

## Installation

To build and install LLSM_importer, you need to download libtiff 4.4.0 first. 

```bash
cd $WORK_SPACE/source
wget https://download.osgeo.org/libtiff/tiff-4.4.0.tar.gz
tar zxvf tiff-4.4.0.tar.gz
cd tiff-4.4.0
./configure --prefix=$WORK_SPACE/install/tiff-4.4.0
make -j 32 install
```

Now you should have libtiff 4.4.0 installed and you need to include the path to the library to your environment variables:

```bash
echo "export TIFF_DIR=$WORK_SPACE/install/tiff-4.4.0"
echo 'export LD_LIBRARY_PATH=$TIFF_DIR/lib:$LD_LIBRARY_PATH'
echo 'export PATH=$TIFF_DIR/include:$TIFF_DIR/lib:$PATH'

echo "export TIFF_DIR=$WORK_SPACE/install/tiff-4.4.0" >> $WORK_SPACE/pdc_env.sh
echo 'export LD_LIBRARY_PATH=$TIFF_DIR/lib:$LD_LIBRARY_PATH' >> $WORK_SPACE/pdc_env.sh
echo 'export PATH=$TIFF_DIR/include:$TIFF_DIR/lib:$PATH' >> $WORK_SPACE/pdc_env.sh
```

Copy the 3 export commands on your screen and run them, and next time if you need to rebuild the llsm_importer program, you can run `$WORK_SPACE/pdc_env.sh` again in advance!

Now, time to build llsm_importer program.

```bash
mkdir -p $WORK_SPACE/source/pdc/tools/build
cd $WORK_SPACE/source/pdc/tools/build

cmake ../ -DCMAKE_BUILD_TYPE=RelWithDebInfo -DPDC_DIR=$PDC_DIR -DUSE_LIB_TIFF=ON -DUSE_SYSTEM_HDF5=ON -DUSE_SYSTEM_OPENMP=ON -DCMAKE_INSTALL_PREFIX=$PDC_DIR/tools/ -DCMAKE_C_COMPILER=cc

make -j 32
```

After this, you should be able to see `llsm_importer` artifact under your `$WORK_SPACE/source/pdc/tools/build` directory.

## Running LLSM_importer

First, locate the llsm_importer script

```bash
cd $WORK_SPACE/source/pdc/scripts/llsm_importer
```

Modify the template script `template.sh`. 

Change `EXECPATH` to where your `pdc_server.exe` is installed
Change `TOOLPATH` to where your `llsm_importer` artifact is.

Change `LLSM_DATA_PATH` to where your sample dataset is. For exmaple, 

```bash
LLSM_DATA_PATH=/pscratch/sd/w/wzhang5/data/llsm/20220115_Korra_LLCPK_LFOV_0p1PSAmpKan/run1
```

Note: you may download the sample dataset from the this [link](https://drive.google.com/file/d/19hH7v58iF_QBJ985ajwLD86MMseBH-YR/view?usp=sharing). It is provided with the courtesy of [Advanced BioImaging Center at UC Berkeley](https://mcb.berkeley.edu/faculty/cdb/upadhyayulas).

Now, run `gen_script.sh` to generate scripts for different settings with various number of servers. 

After this, enter into any directory named with a number, and submit the job with `sbatch` command.
