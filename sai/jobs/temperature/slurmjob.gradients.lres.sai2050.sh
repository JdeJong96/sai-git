#! /bin/bash

# Some basic operations to perform using CDO 

#SBATCH --time=00:15:00      	# wall clock time
#SBATCH --nodes=1            	# number of nodes
#SBATCH --partition=rome,genoa	# partition
#SBATCH --job-name=lres50 	# job name
#SBATCH --account=uuce20038    	# account
#SBATCH --ntasks=1           	# number of tasks
#SBATCH --cpus-per-task=16   	# cpus per task
#SBATCH --output=temperaturegradients.lres.sai2050.out
#SBATCH --error=temperaturegradients.lres.sai2050.err

echo "$(date +"%D %T"): Running $0"
FILES=nwo2021025/archive/lres_b.e10.B2000_CAM5.f09_g16.feedforward_2050.001/atm/hist/lres_b.e10.B2000_CAM5.f09_g16.feedforward_2050.001.cam2.h0.????-??.nc
source $HOME/.bashrc
conda activate geo
python temperaturegradients.py $FILES data/temperaturegradients.lres.sai2050.nc
