from mpi4py import MPI
from TwitterGeoProcessor.lib.utilities import Utilities
import json


class MpiGeoManager:
    # variables
    melb_grid_obj = {}
    data_file_name = None
    IS_LOGGING_ENABLED = False

    def __init__(self, melb_grid_obj, data_file_name, IS_LOGGING_ENABLED=False):
        self.melb_grid_obj = melb_grid_obj
        self.data_file_name = data_file_name
        self.IS_LOGGING_ENABLED = IS_LOGGING_ENABLED

    # processing
    def start_processing(self):
        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()

        if rank == 0:
            output = self.master_node_processor(comm)
        else:
            self.child_node_processor(comm)

        return output

    # MPI processing
    # Master node
    def master_node_processor(self, comm):
        rank = comm.Get_rank()
        size = comm.Get_size()
        # print('start rank:' + str(rank))
        # print('size:' + str(size))
        Utilities.write_log(rank, 'start rank:' + str(rank), True, self.IS_LOGGING_ENABLED)
        Utilities.write_log(rank, 'size:' + str(size), False)

        final_data = self.process_tweet_data(comm)

        ret_data = []
        if size > 1:
            child_data_arr = self.get_data_from_child_nodes(comm)

            Utilities.write_log(0, str(child_data_arr), False, self.IS_LOGGING_ENABLED)

            for i in range(size - 1):
                comm.send('kill', dest=(i + 1), tag=(i + 1))

            print('Merging Total Posts')
            for i, tup in enumerate(final_data[0]):
                inc_val = tup[1]
                for child_tup in child_data_arr:
                    _f = Utilities.check_tuple_exists(child_tup[0], tup[0])
                    if _f is not None:
                        inc_val = inc_val + _f[1][1]
                        final_data[0][i] = (tup[0], inc_val)
                    else:
                        # region found by master nodes which is not present in child nodes
                        # Assuming data is evenly distributed across nodes
                        pass

            # hash_tags
            # merger
            print('HashTags Merger')
            master_node_dict = final_data[1]
            # print(len(master_node_dict))
            # for data_key in master_node_dict:
            for child_tuple in child_data_arr:
                for data_key in child_tuple[1]:
                    if data_key in master_node_dict:
                        master_node_dict[data_key] = master_node_dict[data_key] + child_tuple[1][data_key]
                    else:
                        master_node_dict[data_key] = child_tuple[1][data_key]

            # print(len(master_node_dict))
            print('HashTag Reducer')
            reduced = {}
            reduced_hash = {}
            for data_key in master_node_dict:
                data_arr = data_key.split('||')
                master_key = data_arr[0]
                hash_key = data_arr[1]
                data_val = master_node_dict[data_key]
                if master_key not in reduced:
                    reduced[master_key] = []
                    reduced_hash[master_key] = []
                    _min = 0
                else:
                    _min = min(reduced_hash[master_key])

                if data_val >= _min:
                    if len(reduced_hash[master_key]) == 5:
                        _idx = reduced_hash[master_key].index(_min)
                        reduced_hash[master_key][_idx] = data_val
                        reduced[master_key][_idx] = (hash_key, data_val)

                if len(reduced_hash[master_key]) < 5:
                    reduced_hash[master_key].append(data_val)
                    reduced[master_key].append((hash_key, data_val))

            # print(reduced_hash)
            # print(reduced)
            for key in reduced:
                reduced[key] = Utilities.sort_tuple(reduced[key])

        elif size == 1:
            # only 1 thread is running - reducing required
            # hashtags - reducer
            print('HashTag Reducer')
            # reducer
            # hashtags - reducer
            new_reduced = {}
            reduced = {}
            reduced_hash = {}
            for data_key in final_data[1]:
                data_arr = data_key.split('||')
                master_key = data_arr[0]
                hash_key = data_arr[1]
                data_val = final_data[1][data_key]
                if master_key not in reduced:
                    reduced[master_key] = []
                    reduced_hash[master_key] = []
                    _min = 0
                else:
                    _min = min(reduced_hash[master_key])

                if data_val >= _min:
                    if len(reduced_hash[master_key]) == 5:
                        _idx = reduced_hash[master_key].index(_min)
                        reduced_hash[master_key][_idx] = data_val
                        reduced[master_key][_idx] = (hash_key, data_val)

                if len(reduced_hash[master_key]) < 5:
                    reduced_hash[master_key].append(data_val)
                    reduced[master_key].append((hash_key, data_val))

            for key in reduced:
                reduced[key] = Utilities.sort_tuple(reduced[key])

        final_data = (final_data[0], reduced)
        return final_data

    # child nodes
    def child_node_processor(self, comm):
        rank = comm.Get_rank()
        size = comm.Get_size()
        Utilities.write_log(rank, 'start rank:' + str(rank), True, self.IS_LOGGING_ENABLED)
        Utilities.write_log(rank, 'size:' + str(size), False, self.IS_LOGGING_ENABLED)

        final_child_data = self.process_tweet_data(comm)

        Utilities.write_log(rank, str(final_child_data), False, self.IS_LOGGING_ENABLED)
        while True:
            Utilities.write_log(rank, 'WAITING FOR COMMAND', False, self.IS_LOGGING_ENABLED)
            command = comm.recv(source=0, tag=rank)
            Utilities.write_log(rank, 'COMMAND RECEIVED ->' + str(command), False, self.IS_LOGGING_ENABLED)
            if command == 'send_data_back':
                comm.send(final_child_data, dest=0, tag=0)
            elif command == 'kill':
                exit(0)

    # Algorithms
    def process_tweet_data(self, comm):
        # variables
        rank = comm.Get_rank()
        size = comm.Get_size()
        melb_grid_data = self.melb_grid_obj
        json_filename = self.data_file_name
        region_post_count = []
        region_hashtags_count = {}

        # reading file iteratively
        with open(json_filename) as input_file:
            for line_num, line in enumerate(input_file):
                if line_num % size == rank:
                    try:
                        # pre-processing of data
                        if line[-2:] == ",\n":
                            line = line[:-2]

                        # data loading
                        line_obj = json.loads(line)
                        ret = {
                            'id': line_obj['doc']['_id'],
                            'text': line_obj['doc']['text'],
                            'coordinates': line_obj['doc']['coordinates']['coordinates'],
                            'hashtags': line_obj['doc']['entities']['hashtags']
                        }
                        # print(ret)

                        # get region from tweet object
                        region_key = self.get_region_from_tweet(ret, rank, None)
                        # print(region_key)
                        if region_key is not None:
                            _f = Utilities.check_tuple_exists(region_post_count, region_key)
                            if _f is not None:
                                new_tup = (_f[1][0], _f[1][1] + 1)
                                region_post_count[_f[0]] = new_tup
                            else:
                                region_post_count.append((region_key, 1))

                        # process hashtags
                        if region_key is not None:
                            region_hashtags_count = self.set_hashtags_for_region(ret, region_key, region_hashtags_count,
                                                                                 rank, None)
                    except Exception as e:
                        # print(str(e))
                        pass

        region_post_count = Utilities.sort_tuple(region_post_count)
        Utilities.write_log(rank, str(region_post_count), False, self.IS_LOGGING_ENABLED)
        Utilities.write_log(rank, str(region_hashtags_count), False, self.IS_LOGGING_ENABLED)
        return (region_post_count, region_hashtags_count)

    def get_region_from_tweet(self, tweet_obj, rank, file):
        melb_grid_data = self.melb_grid_obj
        d_keys = list(melb_grid_data.keys())
        d_keys.sort()
        for i, reg_key in enumerate(d_keys):
            reg_obj = melb_grid_data[reg_key]
            try:
                x_coor = tweet_obj['coordinates'][0]
                y_coor = tweet_obj['coordinates'][1]
                if (x_coor >= reg_obj['xmin']) and (x_coor <= reg_obj['xmax']):
                    if (y_coor >= reg_obj['ymin']) and (y_coor <= reg_obj['ymax']):
                        return reg_key
            except Exception as e:
                return None
        return None

    def set_hashtags_for_region(self, tweet_obj, region_key, hashtags_dict, rank, file):
        try:
            if region_key is not None:
                for tags in tweet_obj['hashtags']:
                    s_key = '#' + tags['text'].lower()
                    if region_key + '||' + s_key in hashtags_dict:
                        hashtags_dict[region_key + '||' + s_key] = hashtags_dict[region_key + '||' + s_key] + 1
                    else:
                        hashtags_dict[region_key + '||' + s_key] = 1
        except Exception as e:
            pass
        return hashtags_dict

    # MPI - data collector
    def get_data_from_child_nodes(self, comm):
        data = []
        size = comm.Get_size()
        for i in range(size - 1):
            Utilities.write_log(0, 'SENDING COMMAND TO GET DATA BACK -> ' + str(i + 1), False, self.IS_LOGGING_ENABLED)
            comm.send('send_data_back', dest=(i + 1), tag=(i + 1))
        for i in range(size - 1):
            Utilities.write_log(0, 'RECEIVING DATA FROM ->' + str(i + 1), False, self.IS_LOGGING_ENABLED)
            data.append(comm.recv(source=(i + 1), tag=0))
            Utilities.write_log(0, 'Data Status->' + str(data), False, self.IS_LOGGING_ENABLED)
        return data
