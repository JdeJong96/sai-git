#! /bin/bash

# Some basic operations to perform using CDO 

#SBATCH --time=00:45:00      	# wall clock time
#SBATCH --nodes=1            	# number of nodes
#SBATCH --partition=rome,genoa	# partition
#SBATCH --job-name=lres20 	# job name
#SBATCH --account=uuce20038    	# account
#SBATCH --ntasks=1           	# number of tasks
#SBATCH --cpus-per-task=16   	# cpus per task
#SBATCH --output=mdr.lres.sai2020.out
#SBATCH --error=mdr.lres.sai2020.err

echo "$(date +"%D %T"): Running $0"
FILES=/projects/0/nwo2021025/archive/lres_b.e10.B2000_CAM5.f09_g16.feedforward.001/atm/hist/lres_b.e10.B2000_CAM5.f09_g16.feedforward.001.cam2.h0.????-??.nc
source $HOME/.bashrc
conda activate geo
python mdr.py $FILES data/mdr.lres.sai2020.nc
