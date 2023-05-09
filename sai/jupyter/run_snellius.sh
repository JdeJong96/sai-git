#! /bin/bash

#SBATCH -t 00:05:00          # wall clock time
#SBATCH -N 1                 # number of nodes
#SBATCH -p thin              # partition
#SBATCH -J testrun           # job name
#SBATCH -A uuce20038         # account
#SBATCH -n 1                 # number of tasks
#SBATCH --cpus-per-task=32   # cpus per task
#SBATCH --mem=56G            # memory per node limit

echo "Running ${0} ..."
echo "Submitted from:         ${SLURM_SUBMIT_HOST}:${SLURM_SUBMIT_DIR}"
echo "Start time:             ${SLURM_JOB_START_TIME}"
echo "Projected end time:     ${SLURM_JOB_END_TIME}"
echo
echo "SLURM_JOB_NODELIST:     ${SLURM_JOB_NODELIST}"
echo "SLURM_TASKS_PER_NODE:   ${SLURM_TASKS_PER_NODE}"
echo "SLURM_CPUS_PER_TASK:    ${SLURM_CPUS_PER_TASK}"
echo "SLURM_NTASKS:           ${SLURM_NTASKS}"
echo "SLURM_LOCALID:          ${SLURM_LOCALID}"
echo "SLURM_JOB_ID:           ${SLURM_JOB_ID}"
echo "SLURM_SUBMIT_HOST:      ${SLURM_SUBMIT_HOST}"
echo "SLURM_JOB_ACCOUNT:      ${SLURM_JOB_ACCOUNT}"
echo "SLURM_JOB_PARTITION:    ${SLURM_JOB_PARTITION}"
echo "SLURM_MEM_PER_NODE:     ${SLURM_MEM_PER_NODE}"

source $HOME/.bashrc
conda activate geo
conda env list

# Make sure the jupyter command is available, either by loading the appropriate modules, sourcing your own virtual environment, etc.
#module load 2021
#module load IPython/7.25.0-GCCcore-10.3.0
 
# Choose random port and print instructions to connect
PORT=`shuf -i 5000-5999 -n 1`
LOGIN_HOST=int6-pub.snellius.surf.nl
BATCH_HOST=$(hostname)
 
echo "To connect to the notebook type the following command from your local terminal:"
echo "ssh -J ${USER}@${LOGIN_HOST} ${USER}@${BATCH_HOST} -L ${PORT}:localhost:${PORT}"
echo
echo "After connection is established in your local browser go to the address:"
echo "http://localhost:${PORT}"

jupyter lab --no-browser --port $PORT



#OUTDIR = $PWD
#cp /home/jasperdj/analysis/B.E.13.B1950TRC5.ne30g16.ihesp24.sai2050.01/archive/atm/hist/B.E.13.B1950TRC5.ne30g16.ihesp24.sai2050.01.cam.h0.2060-??.nc $TMPDIR
#cp /home/jasperdj/create_tseries_dask.py $TMPDIR
#cd $TMPDIR

#python create_tseries_dask.py 2>$&1 > log.txt

#mv log.txt $OUTDIR
#mv dask-report.html $OUTDIR
