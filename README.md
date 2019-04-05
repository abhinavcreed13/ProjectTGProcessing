## Project TG Processing

### Installation

`pip install -r requirements.txt`

### Running

`time mpiexec -n 1 python tweet_crawler.py -d data/tinyTwitter.json`

Package Structure
--------------
    TwitterGeoProcessor                    
    |──── TwitterGeoProcessor                  # Main Python Package       
    |────── lib                                
    |──────── mpi_geo_manager.py               # MpiGeoManager class for MPI-driven processing
    |──────── utilities.py                     # Utilities class for common functions
    |────── twittergeoprocessor.py             # TwitterGeoProcessor class for processing & analysing results
    |──── out-files
    |────── slurm-7951179.out                  # 1-node-8-cores output file
    |────── slurm-7951199.out                  # 2-nodes-8-cores output file
    |────── slurm-7951206.out                  # 1-node-1-core output file
    |──── slurm-job               
    |────── one_node_eight_cores_big.slurm       
    |────── one_node_one_core_big.slurm 
    |────── two_nodes_eight_cores_big.slurm               
    |──── tweet_crawler.py                     # Script file to use TwitterGeoProcessor package
