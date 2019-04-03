#!/bin/bash
# 2 nodes, 4 tasks per node = 8 cores
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --time=0-00:02:00
#SBATCH --partition=physical

# Load required modules
module load Python/3.5.2-goolf-2015a 

# Launch multiple process python code
echo "------- 2-nodes-8-cores/small --------"
time mpiexec python3 tweet_crawler.py -d smallTwitter.json