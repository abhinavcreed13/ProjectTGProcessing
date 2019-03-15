import json
import matplotlib.pyplot as plt
from lib.dataplotmanager import DataPlotManager

if __name__ == '__main__':
    with open('data/mG.json') as mg_json_file:
        mg_data = json.loads(mg_json_file.read())

    with open('data/tTwp.json') as ttw_json_file:
        ttw_data = json.loads(ttw_json_file.read())

    plt.figure()
    dpmanager = DataPlotManager()
    dpmanager.plotMGgrid(plt, mg_data)
    dpmanager.plotTwitterDataPoints(plt, ttw_data)
    plt.show()
