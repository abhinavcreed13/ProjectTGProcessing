import json, sys, getopt
import matplotlib.pyplot as plt
from lib.dataplotmanager import DataPlotManager
from lib.datapreprocessing import DataPreProcessing

def parse_arguments(argv):
  # Initialise Variables
  data_file = ''
  output_type = 'total_posts'
  ## Try to read in arguments
  try:
    opts, args = getopt.getopt(argv,"hd:tpth:")
  except getopt.GetoptError as error:
    print(error)
    sys.exit(2)
  for opt, arg in opts:
    if opt == '-h':
      sys.exit()
    elif opt in ("-d"):
        data_file = arg
    elif opt in ("-tp"):
        output_type = 'total_posts'
    elif opt in ("-th"):
        output_type = 'total_hashtags'
  # Return all the arguments
  return data_file, output_type

def main(argv):
    # Get
    data_file, output_type = parse_arguments(argv)

    # read data file
    with open('data/mG.json', encoding="utf8") as mg_json_file:
        mg_data = json.loads(mg_json_file.read())

    with open(data_file, encoding="utf8") as ttw_json_file:
        ttw_data = json.loads(ttw_json_file.read())

    # process file
    plt.figure()
    dp_manager = DataPlotManager()
    pp_manager = DataPreProcessing()
    pp_manager.rem_missing_coordinates(ttw_data)
    dp_manager.plotMGgrid(plt, mg_data)
    dp_manager.plotTwitterDataPoints(plt, ttw_data)
    plt.show()


if __name__ == '__main__':
    main(sys.argv[1:])
