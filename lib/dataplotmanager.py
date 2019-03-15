class DataPlotManager:

    def plotMGgrid(self, plt, mg_grid_data):
        for mgobj in mg_grid_data["features"]:
            id = mgobj["properties"]["id"]
            coordinates = mgobj["geometry"]["coordinates"][0]
            coordinates.append(coordinates[0])
            xs, ys = zip(*coordinates)
            xt = (coordinates[1][0] + coordinates[3][0]) / 2
            yt = (coordinates[1][1] + coordinates[3][1]) / 2
            plt.text(xt, yt, id)
            idx = 0
            for coor in coordinates:
                if idx != 0:
                    plt.text(coor[0], coor[1], '(' + str(coor[0]) + ',' + str(coor[1]) + ')')
                idx = idx + 1
            plt.plot(xs, ys)
        return plt

    def plotTwitterDataPoints(self, plt, tw_data):
        for tw_obj in tw_data:
            try:
                coor = tw_obj["coordinates"]["coordinates"]
                plt.text(coor[0], coor[1], "*")
            except:
                pass
        return plt
