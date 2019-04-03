#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --time=0-00:02:00
#SBATCH --partition=physical

# Load required modules
module load Python/3.5.2-goolf-2015a

# Launch multiple process python code
echo "---- 1-node-8-cores/small -------"
time mpirun -n 8 python3 tweet_crawler.py -d smallTwitter.json

