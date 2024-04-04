#!/bin/bash

#SBATCH -c 16
#SBATCH -t 00:15:00
#SBATCH -p rome,genoa
#SBATCH -o %x-%j
#SBATCH -e %x-%j

source ~/.bashrc
conda activate esm-lunch
which python

DATADIR=/home/jasperdj/nwo2021025/archive/mres_b.e10.B2000_CAM5.f05_t12.001/atm/hist
NAME=mres_b.e10.B2000_CAM5.f05_t12.001.cam2.h0.????-??.nc

files=$(echo $DATADIR/$NAME)
echo "found $(ls $files | wc -l) files in $DATADIR"

python tmean3.py $files $TMPDIR/tmean3.nc
rm $TMPDIR/tmean3.nc
