#! /bin/bash

# Some basic operations to perform using CDO 

#SBATCH --time=00:45:00      	# wall clock time
#SBATCH --nodes=1            	# number of nodes
#SBATCH --partition=rome,genoa	# partition
#SBATCH --job-name=mresrcp 	# job name
#SBATCH --account=uuce20038    	# account
#SBATCH --ntasks=1           	# number of tasks
#SBATCH --cpus-per-task=16   	# cpus per task
#SBATCH --output=windshear.mres.rcp85.out
#SBATCH --error=windshear.mres.rcp85.err

echo "$(date +"%D %T"): Running $0"
FILES=/projects/0/prace_imau/prace_2013081679/cesm1_0_4/f05_t12/rcp8.5_co2_f05_t12/atm/hist/yearly/rcp8.5_co2_f05_t12.cam2.h0.avg????.nc
source $HOME/.bashrc
conda activate geo
python windshear.py $FILES data/windshear.mres.rcp85.nc
