class DataPreProcessing:

    #1
    def rem_missing_coordinates(self, tw_data):

        tw_data_temp = []
        for item in tw_data:
            coor = item["coordinates"]["coordinates"]
            print(coor[0], coor[1])


            if "coordinates" in item.keys():
                pass
               # tw_data_temp.append(item)
            else:
                print(item["_id"])

        tw_data = tw_data_temp
        return tw_data

    #2,3
    def add_loc_factor(self, tw_data):
        #massaged twitter data - new structure
        return tw_data
