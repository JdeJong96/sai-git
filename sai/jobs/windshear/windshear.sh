#!/bin/bash

#$ -S /bin/bash
#$ -N windshear
#$ -pe mpich 8
#$ -l h_rt=00:10:00
#$ -cwd
#$ -q all.q

echo "running windshear.py from $(pwd)"

source ~/.bashrc
conda activate geoengineering

which python
python windshear.py -v /data2/imau/ihesp/HR/B.E.13.B1950TRC5.ne120_t12.cesm-ihesp-2051-2100.014/*.nc vshear.nc
