import json
import matplotlib.pyplot as plt

def plot_mgGrid(mg_data):
    plt.figure()
    for mgobj in mg_data["features"]:
        #print(mgobj)
        id = mgobj["properties"]["id"]
        coordinates = mgobj["geometry"]["coordinates"][0]
        coordinates.append(coordinates[0])
        xs, ys = zip(*coordinates)
        xt = (coordinates[1][0]+coordinates[3][0])/2
        yt = (coordinates[1][1]+coordinates[3][1])/2
        plt.text(xt, yt,id)
        idx = 0
        for coor in coordinates:
            if idx != 0:
                plt.text(coor[0],coor[1],'('+str(coor[0])+','+str(coor[1])+')')
            idx = idx + 1
        plt.plot(xs, ys)
        #print(coordinates)
    plt.show()

with open('data/mG.json') as mg_json_file:
    mg_data = json.loads(mg_json_file.read())

with open('data/tTwp.json') as ttw_json_file:
    ttw_data = json.loads(ttw_json_file.read())

plot_mgGrid(mg_data)



