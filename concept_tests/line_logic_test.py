import ijson
import json
# import memory_profiler
from mpi4py import MPI

# CURRENT BENCHMARK
# tinyTwitter: real	0m0.278s
# smallTwitter: real 0m0.554s
# bigTwitter: real 0m56.216s

# @profile
def parse_json_line_by_line(melb_grid_data, json_filename):
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    # file = open('output_' + str(rank) + '.txt', 'w')
    print('start rank:' + str(rank))
    print('size:' + str(size))
    # file.write('start rank:' + str(rank) + '\n')
    # file.write('size:' + str(size) + '\n')
    region_post_count = {}
    with open(json_filename) as input_file:
        # load json iteratively
        for line_num, line in enumerate(input_file):
            if line_num % size == rank:
                try:
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

                    # file.write(str(ret))
                    region_key = get_region_from_tweet(melb_grid_data, ret, rank, None)
                    if region_key is not None:
                        if region_key in region_post_count:
                            region_post_count[region_key] = region_post_count[region_key] + 1
                        else:
                            region_post_count[region_key] = 1
                except Exception as e:
                    # non parsable line
                    # file.write(str(e) + '\n')
                    # print('non parsable line')
                    pass
    # file.close()
    print(region_post_count)

def get_region_from_tweet(melb_grid_data, tweet_obj, rank, file):
    # print(melb_grid_data)
    for reg_key in melb_grid_data.keys():
        # print('rank:', rank, reg_key)
        reg_obj = melb_grid_data[reg_key]
        try:
            x_coor = tweet_obj['coordinates'][0]
            y_coor = tweet_obj['coordinates'][1]
            # file.write('key: '+reg_key)
            # file.write("(x,y): "+str(x_coor)+','+str(y_coor)+'\n')
            # file.write("(xmin,xmax):"+str(reg_obj['xmin'])+","+str(reg_obj['xmax'])+'\n')
            # file.write("(ymin,ymax):" + str(reg_obj['ymin']) + "," + str(reg_obj['ymax']) + '\n')
            if x_coor >= reg_obj['xmin'] and x_coor <= reg_obj['xmax']:
                if y_coor >= reg_obj['ymin'] and y_coor <= reg_obj['ymax']:
                    # file.write('found in: '+reg_key)
                    return reg_key
        except:
            return None
    return None

if __name__ == '__main__':
    # py_json_parse('data/tTwp.json')
    # r = parse_json('data/tTwp.json')
    # print(len(r.keys()))
    with open('data/melbGrid.json', encoding="utf8") as mg_json_file:
        mg_data = json.loads(mg_json_file.read())
        melbGridObj = {}
        for mgobj in mg_data["features"]:
            id = mgobj["properties"]["id"]
            melbGridObj[id] = mgobj["properties"]

    # parse_json_with_mpi(melbGridObj, 'data/tinyTwitter.json')
    # parse_json_load(melbGridObj, 'data/tinyTwitter.json')

    parse_json_line_by_line(melbGridObj, 'data/smallTwitter.json')
    # parse_json_at_line_awk(melbGridObj, 'data/tinyTwitter.json')
    # print('\n')

    # print(len(r.keys()))
    # extract_text('data/tTwp.json')

# def parse_json_load(melb_grid_data, json_filename):
#     comm = MPI.COMM_WORLD
#     rank = comm.Get_rank()
#     size = comm.Get_size()
#     print('start rank:' + str(rank))
#     print('size:' + str(size))
#     region_post_count = {}
#     # file = open('output.txt', 'w')
#     with open(json_filename) as input_file:
#         json_data = json.loads(input_file.read())
#         for idx, line_obj in enumerate(json_data['rows']):
#             if idx % size == rank:
#                 try:
#                     ret = {
#                         'id': line_obj['doc']['_id'],
#                         'text': line_obj['doc']['text'],
#                         'coordinates': line_obj['doc']['coordinates']['coordinates'],
#                         'hashtags': line_obj['doc']['entities']['hashtags']
#                     }
#
#                     # file.write(str(ret))
#                     region_key = get_region_from_tweet(melb_grid_data, ret, rank, None)
#                     if region_key is not None:
#                         if region_key in region_post_count:
#                             region_post_count[region_key] = region_post_count[region_key] + 1
#                         else:
#                             region_post_count[region_key] = 1
#                 except:
#                     pass
#     print(region_post_count)


# def enumerate2(xs, start=0, step=1):
#     for x in xs:
#         # print(x)
#         yield (start, x)
#         start += step


# def parse_json_line_by_line_mpi(melb_grid_data, json_filename):
#     comm = MPI.COMM_WORLD
#     rank = comm.Get_rank()
#     size = comm.Get_size()
#     # file = open('output_' + str(rank) + '.txt', 'w')
#     print('start rank:' + str(rank))
#     print('size:' + str(size))
#     # file.write('start rank:' + str(rank) + '\n')
#     # file.write('size:' + str(size) + '\n')
#     region_post_count = {}
#     if rank == 0:
#         with open(json_filename) as input_file:
#             # load json iteratively
#             for line_num, line in enumerate(input_file):
#                 if line_num % size == 0:
#                     try:
#                         # file.write("Rank: " + str(rank) + " processing " + str(line_num) + '\n')
#                         # file.write(str(line) + '\n')
#                         if line[-2:] == ",\n":
#                             line = line[:-2]
#                         # file.write(line + '\n')
#
#                         line_obj = json.loads(line)
#
#                         # "(text|_id)":"((\\"|[^"])*)" - for text and _id
#                         # "(coordinates|hashtags)":\[((\\"|[^"])*)\]
#
#                         # file.write(str(ret))
#                         # file.write(str(line_obj))
#                         # file.write(line_obj['id'])
#                         # file.write(line_obj['value']['properties']['text'])
#                         # file.write(str(line_obj['value']['geometry']['coordinates']))
#                         ret = {
#                             'id': line_obj['doc']['_id'],
#                             'text': line_obj['doc']['text'],
#                             'coordinates': line_obj['doc']['coordinates']['coordinates'],
#                             'hashtags': line_obj['doc']['entities']['hashtags']
#                         }
#
#                         # file.write(str(ret))
#                         region_key = get_region_from_tweet(melb_grid_data, ret, rank, None)
#                         if region_key is not None:
#                             if region_key in region_post_count:
#                                 region_post_count[region_key] = region_post_count[region_key] + 1
#                             else:
#                                 region_post_count[region_key] = 1
#                     except Exception as e:
#                         # non parsable line
#                         # file.write(str(e) + '\n')
#                         # print('non parsable line')
#                         pass
#                 else:
#                     try:
#                         if line[-2:] == ",\n":
#                             line = line[:-2]
#                         # file.write(line + '\n')
#
#                         line_obj = json.loads(line)
#
#                         # "(text|_id)":"((\\"|[^"])*)" - for text and _id
#                         # "(coordinates|hashtags)":\[((\\"|[^"])*)\]
#
#                         # file.write(str(ret))
#                         # file.write(str(line_obj))
#                         # file.write(line_obj['id'])
#                         # file.write(line_obj['value']['properties']['text'])
#                         # file.write(str(line_obj['value']['geometry']['coordinates']))
#                         ret = {
#                             'id': line_obj['doc']['_id'],
#                             'text': line_obj['doc']['text'],
#                             'coordinates': line_obj['doc']['coordinates']['coordinates'],
#                             'hashtags': line_obj['doc']['entities']['hashtags']
#                         }
#                         # file.write("SENDING LINE TO ->" + str(line_num % size) + '\n')
#                         comm.send({'ret': ret, 'line_num': line_num, 'exit_node': False}, dest=line_num % size)
#                     except:
#                         pass
#
#         for i in range(0, size):
#             # file.write("SENDING KILL TO " + str(i) + '\n')
#             comm.send({'exit_node': True}, dest=i)
#     elif rank != 0:
#         # file.write("RANK ->" + str(rank) + '\n')
#         while True:
#             try:
#                 # file.write("WAITING FOR SIGNAL FROM MASTER" + '\n')
#                 sent_obj = comm.recv(source=0)
#                 # file.write(str(sent_obj) + '\n')
#                 if sent_obj['exit_node']:
#                     # file.write("EXIT RECIEVED" + '\n')
#                     break
#                 ret = sent_obj['ret']
#                 # file.write("Rank: " + str(rank) + " processing " + str(sent_obj['line_num']) + '\n')
#                 # file.write(str(line) + '\n')
#                 # if line[-2:] == ",\n":
#                 #     line = line[:-2]
#                 # # file.write(line + '\n')
#                 #
#                 # line_obj = json.loads(line)
#
#                 # "(text|_id)":"((\\"|[^"])*)" - for text and _id
#                 # "(coordinates|hashtags)":\[((\\"|[^"])*)\]
#
#                 # file.write(str(ret))
#                 # file.write(str(line_obj))
#                 # file.write(line_obj['id'])
#                 # file.write(line_obj['value']['properties']['text'])
#                 # file.write(str(line_obj['value']['geometry']['coordinates']))
#                 # ret = {
#                 #     'id': line_obj['doc']['_id'],
#                 #     'text': line_obj['doc']['text'],
#                 #     'coordinates': line_obj['doc']['coordinates']['coordinates'],
#                 #     'hashtags': line_obj['doc']['entities']['hashtags']
#                 # }
#
#                 # file.write(str(ret))
#                 region_key = get_region_from_tweet(melb_grid_data, ret, rank, None)
#                 if region_key is not None:
#                     if region_key in region_post_count:
#                         region_post_count[region_key] = region_post_count[region_key] + 1
#                     else:
#                         region_post_count[region_key] = 1
#             except Exception as e:
#                 # file.write(str(e) + '\n')
#                 pass
#     # file.close()
#     print(region_post_count)
