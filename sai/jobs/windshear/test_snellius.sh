#! /bin/bash

# Some basic operations to perform using CDO 

#SBATCH -t 00:30:00          # wall clock time
#SBATCH -N 1                 # number of nodes
#SBATCH -p thin              # partition
#SBATCH -J windshear         # job name
#SBATCH -A uuce20038         # account
#SBATCH -n 1                 # number of tasks
#SBATCH --cpus-per-task=32   # cpus per task
#SBATCH --mem=56G            # memory per node limit

clear
echo "Running $0"

# set files to read
#TYPE=h0 	#[h0,h1,h2,h3,h4] 
#CASE=b.e13.B1950TRC5.ne30_g16.ihesp24_2051-2100.003  		# LR RCP8.5
#CASEDIR=/projects/0/nwo2021025/iHESP_data/HighResMIP/LR/${CASE}
#FILES=${CASEDIR}/atm/hist/${CASE}.cam.${TYPE}.*.nc		# LR RCP8.5
#echo "Found $(ls $FILES | wc -l) files of type ${TYPE} in ${CASEDIR}"

# HR RCP8.5
CASE=B.E.13.B1950TRC5.ne120_t12.cesm-ihesp-2051-2100.014
CASEDIR=/projects/0/nwo2021025/iHESP_data/HighResMIP/HR/${CASE}
FILES=$(ls ${CASEDIR}/atm/proc/tseries/hour_6A/${CASE}.cam.*.*.nc | grep -E "\.U\.|\.V\.|\.PS\." | grep "207001")
#echo $FILES
echo "Found $(ls $FILES | wc -l) files in ${CASEDIR}"

# use own version of cdo
source $HOME/.bashrc
conda activate geo
echo "CDO executable: $(which cdo)"
#echo `cdo -V`

echo "TMPDIR: $TMPDIR"

python windshear.py -v $FILES UV.nc

#OUTDIR = $PWD
#cp /home/jasperdj/analysis/B.E.13.B1950TRC5.ne30g16.ihesp24.sai2050.01/archive/atm/hist/B.E.13.B1950TRC5.ne30g16.ihesp24.sai2050.01.cam.h0.2060-??.nc $TMPDIR
#cp /home/jasperdj/create_tseries_dask.py $TMPDIR
#cd $TMPDIR

