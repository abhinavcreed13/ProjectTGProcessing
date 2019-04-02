# import ijson
import json, getopt, sys
# import memory_profiler
from mpi4py import MPI

# CURRENT BENCHMARK
# tinyTwitter: real	0m0.278s
# smallTwitter: real 0m0.554s
# bigTwitter: real 0m56.216s

IS_LOGGING_ENABLED = False


def _w(rank, text, isNew):
    file = None
    if IS_LOGGING_ENABLED:
        if isNew:
            file = open('output_' + str(rank) + '.txt', 'w')
        else:
            file = open('output_' + str(rank) + '.txt', 'a')
        file.write(text + '\n')
        file.close()


# @profile
def parse_json_line_by_line(comm, melb_grid_data, json_filename, output_type):
    rank = comm.Get_rank()
    size = comm.Get_size()
    region_post_count = []
    region_hashtags_count = {}
    with open(json_filename) as input_file:
        # load json iteratively
        for line_num, line in enumerate(input_file):
            # print('processing->' + str(line_num))
            if line_num % size == rank:
                try:
                    # _w(rank, 'processing->' + str(line_num), False)
                    # file.write("Rank: " + str(rank) + " processing " + str(line_num) + '\n')
                    # file.write(str(line) + '\n')
                    if line[-2:] == ",\n":
                        line = line[:-2]
                    # file.write(line + '\n')

                    line_obj = json.loads(line)

                    # "(text|_id)":"((\\"|[^"])*)" - for text and _id
                    # "(coordinates|hashtags)":\[((\\"|[^"])*)\]

                    # file.write(str(ret))
                    # file.write(str(line_obj))
                    # file.write(line_obj['id'])
                    # file.write(line_obj['value']['properties']['text'])
                    # file.write(str(line_obj['value']['geometry']['coordinates']))
                    ret = {
                        'id': line_obj['doc']['_id'],
                        'text': line_obj['doc']['text'],
                        'coordinates': line_obj['doc']['coordinates']['coordinates'],
                        'hashtags': line_obj['doc']['entities']['hashtags']
                    }

                    # file.write(str(ret) + '\n')
                    # _w(rank, str(ret['coordinates']), False)
                    region_key = get_region_from_tweet(melb_grid_data, ret, rank, None)
                    # if output_type == 'total_posts':
                    if region_key is not None:
                        _f = check_tuple_exists(region_post_count, region_key)
                        # _w(rank, '_f:' + str(_f) + '\n', False)
                        if _f is not None:
                            new_tup = (_f[1][0], _f[1][1] + 1)
                            # _w(rank, 'new_tup:' + str(new_tup) + '\n', False)
                            # _w(rank, str(line_num) + ':' + str((new_tup, ret['coordinates'])), False)
                            region_post_count[_f[0]] = new_tup
                        else:
                            region_post_count.append((region_key, 1))
                    # elif output_type == 'total_hashtags':
                    if region_key is not None:
                        region_hashtags_count = set_hashtags_for_region(ret, region_key, region_hashtags_count,
                                                                        rank)
                except Exception as e:
                    # print(str(e))
                    pass

    region_post_count = sort_tuple(region_post_count)
    # _w(rank, str(region_post_count), False)
    # print("rank:" + str(rank) + "||lines_processed:" + str(lines_processed))
    # print("lines_handled:" + str(lines_handled))
    print("rank: " + str(rank) + ' = ' + str(region_post_count))
    list_hash_counts = []
    for key in region_hashtags_count:
        region_hashtags_count[key] = sort_tuple(region_hashtags_count[key])
        list_hash_counts.append(len(region_hashtags_count[key]))
    # _w(rank, str(region_hashtags_count), False)
    print("rank: " + str(rank) + ' = ' + str(list_hash_counts))
    return (region_post_count, region_hashtags_count)


def get_region_from_tweet(melb_grid_data, tweet_obj, rank, file):
    # print(melb_grid_data)
    # sorted keys required for logic
    d_keys = list(melb_grid_data.keys())
    d_keys.sort()
    # _w(rank, 'list->' + str(d_keys), False)
    for i, reg_key in enumerate(d_keys):
        # print('rank:', rank, reg_key)
        reg_obj = melb_grid_data[reg_key]
        # if LINE_NUM_HIT in [177, 178, 179, 180]:
        #     _w(rank, str(d_keys), False)
        #     _w(rank, 'reg_key:' + str(reg_key), False)
        #     _w(rank, 'reg_obj:' + str(reg_obj), False)
        try:
            x_coor = tweet_obj['coordinates'][0]
            y_coor = tweet_obj['coordinates'][1]
            # file.write('key: '+reg_key)
            # file.write("(x,y): "+str(x_coor)+','+str(y_coor)+'\n')
            # file.write("(xmin,xmax):"+str(reg_obj['xmin'])+","+str(reg_obj['xmax'])+'\n')
            # file.write("(ymin,ymax):" + str(reg_obj['ymin']) + "," + str(reg_obj['ymax']) + '\n')
            if (x_coor >= reg_obj['xmin']) and (x_coor <= reg_obj['xmax']):
                if (y_coor >= reg_obj['ymin']) and (y_coor <= reg_obj['ymax']):
                    # _w(rank, 'reg_key:' + str(reg_key), False)
                    # _w(rank, 'coordinate found inside region', False)
                    return reg_key

            # if reg_key == 'C2':
            #     min = reg_obj['xmin']
            #     max = reg_obj['xmax']
            #     strv = (str(reg_key) + '=((' + str(float(min)) + ',' + str(float(max)) + '),(' + str(
            #         float(x_coor)) + ',' + str(
            #         float(y_coor)) + '))=' + str((x_coor == reg_obj['xmin']) or (x_coor == reg_obj['xmax'])))
            #     # print(strv)
            #     _w(rank, strv, False)

            # coordinate found in fixed x and changing y
            # if (float(x_coor) == float(reg_obj['xmin'])) or (float(x_coor) == float(reg_obj['xmax'])):
            #     if (float(y_coor) >= float(reg_obj['ymin'])) and (float(y_coor) <= float(reg_obj['ymax'])):
            #         _w(rank, 'coordinate found in between (fixed x and changing y)', False)
            #         # _w(rank, 'coordinate found in top-right/left corner', False)
            #         # _w(rank, 'coordinate found in bottom-right/left corner', False)
            #         _w(rank, 'reg_key:' + str(reg_key), False)
            #         if i == 0:
            #             return reg_key
            #         elif i != 0:
            #             next_reg_key = d_keys[i + 1]
            #             next_reg_obj = melb_grid_data[next_reg_key]
            #             # in same row
            #             if float(next_reg_obj['ymin']) == float(reg_obj['ymin']) and float(
            #                     next_reg_obj['ymax']) == float(reg_obj['ymax']):
            #                 # choose left box
            #                 # row check
            #                 _w(rank, 'Match found->' + next_reg_key, False)
            #                 return reg_key
            #             else:
            #                 return reg_key
            #         # elif y_coor == reg_obj['ymin']:
            #         #     # coordinate found in top-right corner
            #         #     pass
            #         # elif y_coor == reg_obj['ymax']:
            #         #     # coordinate found in bottom-right corner
            #
            # # coordinate found in fixed y and changing x
            # if (float(y_coor) == float(reg_obj['ymin'])) or (float(y_coor) == float(reg_obj['ymax'])):
            #     if (float(x_coor) >= float(reg_obj['xmin'])) and (float(x_coor) <= float(reg_obj['xmax'])):
            #         _w(rank, 'coordinate found in between (fixed y and changing x)', False)
            #         # _w(rank, 'coordinate found in top-right/left corner', False)
            #         # _w(rank, 'coordinate found in bottom-right/left corner', False)
            #         if i == 0:
            #             return reg_key
            #         elif i != 0:
            #             _w(rank, str(i) + ':' + str(d_keys[i + 1:]), False)
            #             for next_reg_key in d_keys[i + 1:]:
            #                 next_reg_obj = melb_grid_data[next_reg_key]
            #                 # column check
            #                 if float(next_reg_obj['xmin']) == float(reg_obj['xmin']) and float(
            #                         next_reg_obj['xmax']) == float(reg_obj[
            #                                                            'xmax']):
            #                     # choosing higher one
            #                     _w(rank, 'Match found->' + next_reg_key, False)
            #                     return reg_key
            #                 else:
            #                     return reg_key
        except Exception as e:
            # print(str(e))
            return None
    return None


def set_hashtags_for_region(tweet_obj, region_key, hashtags_dict, rank):
    # file.write(str(hashtags_dict) + '\n')
    # _w(rank, region_key, False)
    try:
        if region_key is not None:
            # _fR = check_tuple_exists(hashtags_arr, region_key)
            # if _fR is None:
            #     hashtags_arr.append((region_key, None, None))
            if region_key not in hashtags_dict:
                hashtags_dict[region_key] = []
            # file.write(str(hashtags_dict) + '\n')
            # file.write('h:' + str(tweet_obj['hashtags']) + '\n')
            for tags in tweet_obj['hashtags']:
                s_key = '#' + tags['text'].lower()
                # file.write('s_key:' + s_key + '\n')
                _f = check_tuple_exists(hashtags_dict[region_key], s_key)
                # _w(rank, '_f:' + str(_f), False)
                if _f is not None:
                    new_tup = (_f[1][0], _f[1][1] + 1)
                    # _w(rank, 'new_tup:' + str(new_tup), False)
                    hashtags_dict[region_key][_f[0]] = new_tup
                else:
                    hashtags_dict[region_key].append((s_key, 1))
    except Exception as e:
        # print(str(e))
        pass

    return hashtags_dict


def check_tuple_exists(list_of_tuples, search_key, level=0):
    for i, tup in enumerate(list_of_tuples):
        if tup[level] == search_key:
            return (i, tup)


def get_tuples(list_of_tuples, search_key, level=0):
    tups = []
    for tup in list_of_tuples:
        if tup[level] == search_key:
            tups.append(tup)
    return tups


def merge_tuples(list_of_tuples, search_key, level=0):
    new_tup = (search_key, None)
    for i, tup in enumerate(list_of_tuples):
        if tup[level] == search_key:
            if new_tup[1] is not None:
                new_tup = (search_key, new_tup[1] + tup[1])
                del list_of_tuples[i]
            else:
                new_tup = (search_key, tup[1])
    return new_tup


def check_tuple_exists_pair(list_of_tuples, search_key_1, search_key_2):
    for i, tup in enumerate(list_of_tuples):
        if tup[0] == search_key_1 and tup[1] == search_key_2:
            return (i, tup)


def sort_tuple(list_of_tups, level=1):
    # reverse = None (Sorts in Ascending order)
    # key is set to sort using second element of
    # sublist lambda has been used
    list_of_tups.sort(key=lambda x: x[level], reverse=True)
    return list_of_tups


def parse_arguments(argv):
    # Initialise Variables
    data_file = ''
    output_type = 'total_posts'
    ## Try to read in arguments
    try:
        opts, args = getopt.getopt(argv, "xd:ph")
    except getopt.GetoptError as error:
        print(error)
        sys.exit(2)
    # print(opts)
    for opt, arg in opts:
        if opt == '-x':
            sys.exit()
        elif opt in ("-d"):
            data_file = arg
        elif opt in ("-p"):
            output_type = 'total_posts'
        elif opt in ("-h"):
            output_type = 'total_hashtags'
    # Return all the arguments
    return data_file, output_type


def get_data_from_child_nodes(comm):
    data = []
    size = comm.Get_size()
    for i in range(size - 1):
        _w(0, 'SENDING COMMAND TO GET DATA BACK -> ' + str(i + 1), False)
        comm.send('send_data_back', dest=(i + 1), tag=(i + 1))
    for i in range(size - 1):
        _w(0, 'RECEIVING DATA FROM ->' + str(i + 1), False)
        data.append(comm.recv(source=(i + 1), tag=0))
        _w(0, 'Data Status->' + str(data), False)
    return data


def master_node(comm, melbGridObj, data_file, output_type):
    rank = comm.Get_rank()
    size = comm.Get_size()
    # print('start rank:' + str(rank))
    # print('size:' + str(size))
    _w(rank, 'start rank:' + str(rank), True)
    _w(rank, 'size:' + str(size), False)

    final_data = parse_json_line_by_line(comm, melbGridObj, data_file, output_type)

    ret_data = []
    if size > 1:
        child_data_arr = get_data_from_child_nodes(comm)
        _w(0, str(child_data_arr), False)
        for i in range(size - 1):
            comm.send('kill', dest=(i + 1), tag=(i + 1))

        # total_posts
        print('Merging Total Posts')
        for i, tup in enumerate(final_data[0]):
            # _w(0, str(tup), False)
            inc_val = tup[1]
            for child_tup in child_data_arr:
                _f = check_tuple_exists(child_tup[0], tup[0])
                # _w(0, str(_f), False)
                if _f is not None:
                    inc_val = inc_val + _f[1][1]
                    final_data[0][i] = (tup[0], inc_val)
                else:
                    # region found by master nodes which is not present in child nodes
                    # Assuming data is evenly distributed across nodes
                    pass

        # hash_tags
        # merger
        print('Hash Tags Merger')
        reg_dict = final_data[1]
        for key in reg_dict:
            # _w(0, str(reg_dict[key]), False)
            master_list = reg_dict[key]
            for child_tup in child_data_arr:
                child_reg_dict = child_tup[1]
                if key in child_reg_dict:
                    child_list = child_reg_dict[key]
                    master_list = master_list + child_list
                else:
                    # region found by master nodes which is not present in child nodes
                    # Assuming data is evenly distributed across nodes
                    pass
            reg_dict[key] = master_list
        # _w(0, str(final_data[1]), False)

        print('Hash Tags Reducer')
        # reducer
        for key in reg_dict:
            array_of_tuples = reg_dict[key]
            new_arr_tuples = []
            for tup in array_of_tuples:
                new_arr_tuples.append(merge_tuples(array_of_tuples, tup[0], level=0))
            reg_dict[key] = sort_tuple(new_arr_tuples, level=1)[:5]
    elif size == 1:
        # only 1 thread is running - reducing required
        # hashtags - reducer
        for key in final_data[1]:
            final_data[1][key] = sort_tuple(final_data[1][key], level=1)[:5]

    print('Printing final output')
    final_output_printer(final_data)


def final_output_printer(final_data):
    print('')
    print('Total number of Twitter posts:')
    for data_tuple in final_data[0]:
        print(data_tuple[0] + ':', "{:,}".format(data_tuple[1]), 'posts')
    print('')
    print('Top 5 hashtags in each grid cells:')
    for data_key in final_data[1]:
        print(data_key + ':', '(' + str(final_data[1][data_key])[1:-1] + ')')


def child_node(comm, melbGridObj, data_file, output_type):
    rank = comm.Get_rank()
    size = comm.Get_size()
    # print('start rank:' + str(rank))
    # print('size:' + str(size))
    _w(rank, 'start rank:' + str(rank), True)
    _w(rank, 'size:' + str(size), False)

    final_child_data = parse_json_line_by_line(comm, melbGridObj, data_file, output_type)
    _w(rank, str(final_child_data), False)
    while True:
        _w(rank, 'WAITING FOR COMMAND', False)
        command = comm.recv(source=0, tag=rank)
        _w(rank, 'COMMAND RECEIVED ->' + str(command), False)
        if command == 'send_data_back':
            comm.send(final_child_data, dest=0, tag=0)
        elif command == 'kill':
            exit(0)


def main(argv):
    # Get
    data_file, output_type = parse_arguments(argv)
    # py_json_parse('data/tTwp.json')
    # r = parse_json('data/tTwp.json')
    # print(len(r.keys()))
    with open('melbGrid.json', encoding="utf8") as mg_json_file:
        mg_data = json.loads(mg_json_file.read())
        melbGridObj = {}
        for mgobj in mg_data["features"]:
            id = mgobj["properties"]["id"]
            melbGridObj[id] = mgobj["properties"]

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    if rank == 0:
        master_node(comm, melbGridObj, data_file, output_type)
    else:
        child_node(comm, melbGridObj, data_file, output_type)

    # parse_json_line_by_line(comm, melbGridObj, data_file, output_type)


if __name__ == '__main__':
    main(sys.argv[1:])
