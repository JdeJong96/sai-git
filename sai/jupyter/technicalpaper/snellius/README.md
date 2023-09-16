How to easily open Jupyter Lab on Snellius:

1. Open a browser and go to https://jupyter.snellius.surf.nl/2022
2. Log in using your credentials
3. Select CPU and click 'Start'


How to activate your own conda environment "my_env":

1. In Jupyter Lab, open a new terminal
2. Type in the following commands:
	module purge			# omit pre-loaded modules in subsequent installations
	conda activate my_env	# activate conda environment
	python -m ipykernel install --user --name=my_env	# create custom kernel, might need to install 'ipykernel' first
	sed -i '/"-m",/i \ \ "-E",' ~/.local/share/jupyter/kernels/my_env/kernel.json 	# adds -E to kernel launch command
3. Now, in a notebook, go to 'kernel' > 'change kernel' and select 'my_env'

Imported modules should now originate from "my_env". 
To add modules, just use 'conda install ...' in the terminal (with "my_env" activated).


The above is a slightly adapted version of the Snellius infopage:
https://servicedesk.surf.nl/wiki/display/WIKI/Jupyter+Notebooks+on+Snellius