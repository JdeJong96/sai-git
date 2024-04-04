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
NAME=mres_b.e10.B2000_CAM5.f05_t12.001.cam2.h0.2050-??.nc

files=$(echo $DATADIR/$NAME)
echo "found $(ls $files | wc -l) files in $DATADIR"

mkdir $TMPDIR/tmpdata
cp $files $TMPDIR/tmpdata/

python tmean4.py $TMPDIR/tmpdata/*.nc $TMPDIR/tmean4.nc
rm -r $TMPDIR/tmpdata
rm $TMPDIR/tmean4.nc
