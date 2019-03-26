import ijson
import json
import memory_profiler
from mpi4py import MPI
from json import JSONDecoder
from functools import partial
import re


# @profile
def py_json_parse(json_filename):
    with open(json_filename, 'r') as input_file:
        return json.load(input_file)


def parse_json(json_filename):
    with open(json_filename, 'rb') as input_file:
        # load json iteratively
        parser = ijson.parse(input_file)
        ret = {}
        r_id = None
        objcount = 0
        objs_processed = 0
        total_ids_processed = 0
        _flagItemFound = False
        for prefix, event, value in parser:
            # prefix=item||event=start_map||value=None
            # prefix=item||event=end_map||value=None
            if prefix == 'item' and event == 'start_map' and value == None:
                pass
            elif prefix == 'item' and event == 'end_map' and value == None:
                objs_processed = objs_processed + 1
            print('processing=', objs_processed)
            if prefix.endswith('._id'):
                ret[value] = {}
                r_id = value
            elif prefix == 'item.text':
                ret[r_id]['text'] = value
            elif prefix == 'item.coordinates.coordinates' and event == 'start_array':
                ret[r_id]['coordinates'] = []
            elif prefix == 'item.coordinates.coordinates.item' and event == 'number':
                ret[r_id]['coordinates'].append(value)
        return ret


# @profile
def parse_json_with_mpi(melb_grid_data, json_filename):
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    print('start rank:' + str(rank))
    print('size:' + str(size))
    region_post_count = {}
    with open(json_filename, 'rb') as input_file:
        # load json iteratively
        parser = ijson.parse(input_file)
        ret = {}
        r_id = None
        objcount = 0
        objs_processed = 0
        total_ids_processed = 0
        _flagItemFound = False
        # file = open('output.txt', 'w')
        for prefix, event, value in parser:
            # prefix=item||event=start_map||value=None
            # prefix=item||event=end_map||value=None
            # file.write('prefix='+str(prefix)+' event='+str(event)+' value='+str(value)+'\n')
            if prefix == 'rows.item' and event == 'start_map' and value == None:
                pass
            elif prefix == 'rows.item' and event == 'end_map' and value == None:
                # object is ready for processing
                # print(ret)
                region_key = get_region_from_tweet(melb_grid_data, ret, rank, None)
                if region_key is not None:
                    if region_key in region_post_count:
                        region_post_count[region_key] = region_post_count[region_key] + 1
                    else:
                        region_post_count[region_key] = 1
                ret = {}
                objs_processed = objs_processed + 1
            # print(objs_processed % size)
            # file.write(str(objs_processed) + '%' + str(size) + '=' + str(objs_processed % size) + '\n')
            if objs_processed % size == rank:
                # if rank == 1:
                # print('processing=', objs_processed, 'via rank=', rank)
                # file.write('processing=' + str(objs_processed) + ' via rank=' + str(rank) + '\n')
                if prefix.endswith('._id'):
                    ret['id'] = value
                    # ret[value] = {}
                    # r_id = value
                elif prefix == 'rows.item.doc.text':
                    ret['text'] = value
                elif prefix == 'rows.item.doc.coordinates.coordinates' and event == 'start_array':
                    ret['coordinates'] = []
                elif prefix == 'rows.item.doc.coordinates.coordinates.item' and event == 'number':
                    ret['coordinates'].append(value)
        print('rank:', rank, 'ret_count:', len(ret.keys()))
        print(region_post_count)
        # file.write('rank:' + str(rank) + ' ret_count:' + str(len(ret.keys())) + '\n')
        # file.close()
        return ret


# @profile
def parse_json_line_by_line(melb_grid_data, json_filename):
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    print('start rank:' + str(rank))
    print('size:' + str(size))
    region_post_count = {}
    #file = open('output.txt', 'w')
    with open(json_filename) as input_file:
        # load json iteratively
        for line_num, line in enumerate(input_file):
            if line_num % size == rank:
                try:
                    if line[-2:] == ",\n":
                        line = line[:-2]
                    # file.write(line)

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
                    #file.write(str(e) + '\n')
                    # print('non parsable line')
                    pass
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


def extract_text(json_filename):
    with open(json_filename, 'rb') as input_file:
        texts = ijson.items(input_file, 'item.text')
        for t in texts:
            print('Text:', t)


def parse_json_with_decoder(fileobj, decoder=JSONDecoder(), buffersize=2048):
    buffer = ''
    for chunk in iter(partial(fileobj.read, buffersize), ''):
        buffer += chunk
        while buffer:
            try:
                result, index = decoder.raw_decode(buffer)
                yield result
                buffer = buffer[index:]
            except ValueError:
                # Not enough data to decode, read more
                break


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

    parse_json_line_by_line(melbGridObj, 'data/tinyTwitter.json')
    # print('\n')

    # print(len(r.keys()))
    # extract_text('data/tTwp.json')
