#!/bin/bash
#SBATCH --ntasks=1
#SBATCH --time=0-00:04:00
#SBATCH --partition=cloud

# Load required modules
module load Python/3.5.2-goolf-2015a 

# Launch multiple process python code
echo "------ 1-node-1-core/big --------"
time mpiexec -n 1 python3 tweet_crawler.py -d bigTwitter.json
