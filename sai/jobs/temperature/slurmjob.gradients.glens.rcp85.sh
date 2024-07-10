#! /bin/bash

# Some basic operations to perform using CDO 

#SBATCH --time=00:30:00      	# wall clock time
#SBATCH --nodes=1            	# number of nodes
#SBATCH --partition=rome,genoa	# partition
#SBATCH --job-name=glensrcp 	# job name
#SBATCH --account=uuce20038    	# account
#SBATCH --ntasks=1           	# number of tasks
#SBATCH --cpus-per-task=16   	# cpus per task
#SBATCH --output=temperaturegradients.glens.rcp85.out
#SBATCH --error=temperaturegradients.glens.rcp85.err

echo "$(date +"%D %T"): Running $0"
FILES1=/projects/0/nwo2021025/jasper/GLENS1/TREFHT/b.e15.B5505C5WCCML45BGCR.f09_g16.control.001.cam.h0.TREFHT.??????-??????.nc
FILES2=/projects/0/nwo2021025/jasper/GLENS1/TREFHT/b.e15.B5505C5WCCML45BGCR.f09_g16.control.001.cam.h0.TREFHT.??????-??????.nc
FILES3=/projects/0/nwo2021025/jasper/GLENS1/TREFHT/b.e15.B5505C5WCCML45BGCR.f09_g16.control.001.cam.h0.TREFHT.??????-??????.nc
source $HOME/.bashrc
conda activate geo
python temperaturegradients.py $FILES1 data/temperaturegradients.glens.rcp85.001.nc
python temperaturegradients.py $FILES2 data/temperaturegradients.glens.rcp85.002.nc
python temperaturegradients.py $FILES3 data/temperaturegradients.glens.rcp85.003.nc
