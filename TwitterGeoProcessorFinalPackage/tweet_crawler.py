import sys
from TwitterGeoProcessor.twittergeoprocessor import TwitterGeoProcessor

IS_LOGGING_ENABLED = True

def main(argv):
    geo_processor = TwitterGeoProcessor(argv, 'melbGrid.json', IS_LOGGING_ENABLED)
    geo_processor.start_processing()

if __name__ == '__main__':
    main(sys.argv[1:])
