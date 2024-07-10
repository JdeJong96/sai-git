#! /bin/bash

# Some basic operations to perform using CDO 

#SBATCH --time=00:15:00      	# wall clock time
#SBATCH --nodes=1            	# number of nodes
#SBATCH --partition=rome,genoa	# partition
#SBATCH --job-name=mres50 	# job name
#SBATCH --account=uuce20038    	# account
#SBATCH --ntasks=1           	# number of tasks
#SBATCH --cpus-per-task=16   	# cpus per task
#SBATCH --output=temperaturegradients.mres.sai2050.out
#SBATCH --error=temperaturegradients.mres.sai2050.err

echo "$(date +"%D %T"): Running $0"
FILES=/projects/0/nwo2021025/archive/mres_b.e10.B2000_CAM5.f05_t12.001/atm/hist/mres_b.e10.B2000_CAM5.f05_t12.001.cam2.h0.????-??.nc
source $HOME/.bashrc
conda activate geo
python temperaturegradients.py $FILES data/temperaturegradients.mres.sai2050.nc
