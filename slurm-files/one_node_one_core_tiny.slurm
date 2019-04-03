#!/bin/bash
#SBATCH --ntasks=1
#SBATCH --time=0-00:02:00
#SBATCH --partition=physical

# Load required modules
module load Python/3.5.2-goolf-2015a 

# Launch multiple process python code
echo "------ 1-node-1-core/tiny --------"
time mpiexec -n 1 python3 tweet_crawler.py -d tinyTwitter.json
