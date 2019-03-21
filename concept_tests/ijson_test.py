import ijson
import json
import memory_profiler

#@profile
def py_json_parse(json_filename):
    with open(json_filename, 'r') as input_file:
        return json.load(input_file)

@profile
def parse_json(json_filename):
    with open(json_filename, 'rb') as input_file:
        # load json iteratively
        parser = ijson.parse(input_file)
        ret = {}
        r_id = None
        linecount = 0
        for prefix, event, value in parser:
            if prefix.endswith('._id'):
                ret[value] = {}
                r_id = value
            elif prefix == 'item.text':
                ret[r_id]['text'] = value
            elif prefix == 'item.coordinates.coordinates' and event == 'start_array':
                ret[r_id]['coordinates'] = []
            elif prefix == 'item.coordinates.coordinates.item' and event == 'number':
                ret[r_id]['coordinates'].append(value)
            linecount = linecount + 1
            print(str(linecount))
        return ret


def extract_text(json_filename):
    with open(json_filename, 'rb') as input_file:
        texts = ijson.items(input_file, 'item.text')
        for t in texts:
            print('Text:',t)


if __name__ == '__main__':
    #py_json_parse('data/tTwp.json')
    r = parse_json('data/tTwp.json')
    print(r)
    #extract_text('data/tTwp.json')
