import json
from TwitterGeoProcessor.lib.utilities import Utilities
from TwitterGeoProcessor.lib.mpi_geo_manager import MpiGeoManager

class TwitterGeoProcessor:
    # variables
    melb_grid_obj = {}
    data_file_name = None
    IS_LOGGING_ENABLED = False

    def __init__(self, argv, melb_grid_file_name, IS_LOGGING_ENABLED = False):
        # process melb_grid file
        with open(melb_grid_file_name, encoding="utf8") as mg_json_file:
            mg_data = json.loads(mg_json_file.read())
            for mgobj in mg_data["features"]:
                id = mgobj["properties"]["id"]
                self.melb_grid_obj[id] = mgobj["properties"]

        # process argv arguments
        self.data_file_name = Utilities.parse_cmd_arguments(argv)

        self.IS_LOGGING_ENABLED = IS_LOGGING_ENABLED

    def start_processing(self):
        _manager = MpiGeoManager(self.melb_grid_obj, self.data_file_name, self.IS_LOGGING_ENABLED)
        output = _manager.start_processing()
        Utilities.generate_output(output)
