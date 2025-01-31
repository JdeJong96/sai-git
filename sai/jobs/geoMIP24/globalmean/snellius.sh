#! /bin/bash

# Some basic operations to perform using CDO 

#SBATCH --time=05:15:00      	# wall clock time
#SBATCH --nodes=1            	# number of nodes
#SBATCH --partition=thin     	# partition
#SBATCH --job-name=globalmean 	# job name
#SBATCH --account=uuce20038    	# account
#SBATCH --ntasks=1           	# number of tasks
#SBATCH --cpus-per-task=16   	# cpus per task

clear
echo "$(date +"%D %T"): Running $0"

FILES=/projects/0/nwo2021025/archive/mres_b.e10.B2000_CAM5.f05_t12.001/atm/hist/mres_b.e10.B2000_CAM5.f05_t12.001.cam2.h0.2???-??.nc
#FILES=/projects/0/nwo2021025/archive/mres_b.e10.B2000_CAM5.f05_t12.001/atm/hist/mres_b.e10.B2000_CAM5.f05_t12.001.cam2.h0.2050-01.nc

source $HOME/.bashrc
conda activate geo

mkdir -p data
python globalmean.py -v $FILES data/mres.h0.gmean.nc
