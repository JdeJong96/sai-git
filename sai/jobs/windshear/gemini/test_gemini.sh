#!/bin/bash

#$ -cwd                   # not allowed in /data2/imau/.., use -wd instead
#$ -j yes
#$ -l h_rt=00:00:59
#$ -M j.dejong3@uu.nl
#$ -N test
#$ -q all.q
#$ -S /bin/bash

echo $(pwd)
echo $0
echo "============================="
printenv
ls /data2/imau/ihesp/HR/B.E.13.B1950TRC5.ne120_t12.cesm-ihesp-2051-2100.014/
