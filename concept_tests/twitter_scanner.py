###  mpiexec /np 8 python concept_tests/twitter_scanner.py -i data/smallTwitter.json
###  mpiexec /np 8 python concept_tests/twitter_scanner.py -i data/tinyTwitter.json
# IMPORTANT: Use the above code to execute the program

import getopt
import json
import operator
import sys
import time
import heapq

from mpi4py import MPI
from collections import Counter

start = time.time()

# Constants
LEADER_RANK = 0
REGION_INFO = 1
HASHTAGS_INFO = 2

def print_regioncounts(regioncounts):
    sorted_counts = sorted(regioncounts.items(), key=operator.itemgetter(1))
    print("Regions with their no of tweets in Descending order:")
    for regioncounts in reversed(sorted_counts):
        key, times = regioncounts
        print(key, " : ", times)
    end = time.time()
    print(end - start)


def print_hashtagcounts(final_results, final_hashs):
    flag = 0
    sorted_results = sorted(final_results.items(), key=operator.itemgetter(1))
    print("Regions with top five tweets in Descending order:")
    for region in reversed(sorted_results):
        sort_hash = heapq.nlargest(5, final_hashs[region[0]].items(), key=lambda x: x[1])
        # sort_hash = list(Counter(final_hashs[region[0]]).most_common(5))
        print("\n", region[0],": (", end="")
        for hashes in sort_hash:
            key, times = hashes
            if flag == 0:
                flag = 1
            else:
                print(",",end="")

            print("('", key.decode('utf8').strip(), "', ", times,")", end="")
        print(")")
        flag = 0
    end = time.time()
    print("\n", end - start)

def display_final_output(final_results, search_type):
    print_regioncounts(final_results)

def display_all_output(final_results, final_hashs, search_type):
    print_hashtagcounts(final_results, final_hashs)


def twitter_json_parser(rank, file_name, size, melboure_grid_data, search_type):
    region_post_count = {}
    hashtags_count = {}
    # Open the main Twitter files
    with open(file_name, encoding="utf8") as input_file:
        # Load data one line a time
        # Enumerate helps in iterating the file as weel as keep track of line number
        for line_num, line in enumerate(input_file):
            # This is where the magic happens.
            # We only process the code for the lines that are supposed to be by the current node
            if line_num % size == rank:
                try:
                    # Check if the JSON file is ending with new line feed
                    # This helps in keeping track of the line number
                    if line[-2:] == ",\n":
                        line = line[:-2]
                        line_obj = json.loads(line)
                        ret = {
                            'id': line_obj['doc']['_id'],
                            'screen_name': line_obj['doc']['user']['screen_name'],
                            'text': line_obj['doc']['text'],
                            'coordinates': line_obj['doc']['coordinates']['coordinates'],
                            'hashtags': line_obj['doc']['entities']['hashtags']
                        }
                        region_key = get_region_from_tweet(melboure_grid_data, ret)
                        if region_key is not None:
                            if region_key in region_post_count:
                                region_post_count[region_key] = region_post_count[region_key] + 1
                            else:
                                region_post_count[region_key] = 1
                        if search_type == "hashtags":
                            if region_key in hashtags_count:
                                hashtags_data = hashtags_count[region_key]
                            else:
                                hashtags_data = {}

                            for hashtags in ret["hashtags"]:
                                hashtags_data[hashtags['text'].encode('utf8').lower()] = hashtags_data.setdefault(hashtags['text'].encode('utf8').lower(), 0) + 1

                            hashtags_count[region_key] = hashtags_data

                            # if hashtags['text'].encode('utf8').lower() in hashtags_data:
                            #     hashtags_data[hashtags['text'].encode('utf8').lower()] = hashtags_data.setdefault(
                            #         hashtags['text'].encode('utf8').lower(), 0) + 1
                            # else:
                            #     hashtags_data[hashtags['text'].encode('utf8').lower()] = 1

                            # for hashtags in ret["hashtags"]:
                            #     hashtags_count[hashtags['text'].encode('utf8').strip()] = hashtags_count.setdefault(
                            #         hashtags['text'].encode('utf8').strip(), 0) + 1
                except Exception as e:
                    # non parsable line
                    # print('non parsable line')
                    pass
    if search_type == "regions":
        # print(rank, ":", end="")
        # print(region_post_count)
        return region_post_count
    elif search_type == "hashtags":
        # print(rank, ":", end="")
        # print(hashtags_count)
        # if rank == 0:
        #     print("HashTag File Data", hashtags_count)
        # print('hashtags_count len:', rank, hashtags_count.values())
        return region_post_count, hashtags_count


def get_region_from_tweet(melboure_grid_data, tweet_obj):
    # Loop on all the Gird Data and find the region tweet belongs to
    for reg_key in melboure_grid_data.keys():
        reg_obj = melboure_grid_data[reg_key]
        try:
            x_coordinate = tweet_obj['coordinates'][0]
            y_coordinate = tweet_obj['coordinates'][1]
            if reg_obj['xmin'] <= x_coordinate <= reg_obj['xmax'] and reg_obj['ymin'] <= y_coordinate <= reg_obj['ymax']:
                return reg_key
        except:
            return None
    return None


def leaders_helper_communicator(comm):
    processes = comm.Get_size()
    mid_results = []
    mid_hashs = {}
    mid_hashs_all = {}
    # Now ask all processes except oursevles to return counts
    for i in range(processes - 1):
        # Send request to slaves to send back the data
        comm.send('Please give me the analyzed data.', dest=(i + 1), tag=(i + 1))
    for i in range(processes - 1):
        # Receive data the data sent from helpers and append it in the leaders file
        # and then return the final count
        mid_results.append(comm.recv(source=(i + 1), tag=REGION_INFO))
        if search_type == "hashtags":
            mid_hashs = comm.recv(source=(i + 1), tag=HASHTAGS_INFO)
            newcounter = Counter()
            for hashs in mid_hashs:
                if hashs in mid_hashs_all:
                    newcounter = Counter(mid_hashs[hashs]) + Counter(mid_hashs_all[hashs])
                    mid_hashs_all[hashs] = dict(newcounter)
                else:
                    mid_hashs_all[hashs] = mid_hashs[hashs]

            # final_hashs[dkey] = hashtags_data
            # print("mid_hashs type: ", type(mid_hashs))
            # if i+1 == 1:
            #     print('mid_hashs len:', i + 1, mid_hashs)
    if search_type == "hashtags":
        return mid_results, mid_hashs_all
    else:
        return mid_results


def leader_node_handler(comm, file_name, melboure_grid_data, search_type):
    # Find the process info
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Parse the JSON file line by line and perform the analysis and store in final result
    # We will add the data from Helper process to this once they have finished the analysis
    # final_results = twitter_json_parser(rank, file_name, size, melboure_grid_data, search_type)
    if search_type == "hashtags":
        final_results, final_hashs = twitter_json_parser(rank, file_name, size, melboure_grid_data, search_type)
    else:
        final_results = twitter_json_parser(rank, file_name, size, melboure_grid_data, search_type)

    # if multicore then Leader needs to communicate with processes and collect their analyzed data
    if size > 1:
        # Use the communicator to give instruction to Helpers to provide the data back
        # helper_results = leaders_helper_communicator(comm)
        if search_type == "hashtags":
            helper_results, helper_hashs = leaders_helper_communicator(comm)
        else:
            helper_results = leaders_helper_communicator(comm)


        # Collect all the data from Helpers in the final_results
        for helper_data in helper_results:
            # loop on key value pair and add the entries
            for dkey, dval in helper_data.items():
                final_results[dkey] = final_results.setdefault(dkey, 0) + dval
                if search_type == "hashtags":
                    # print("hashtags_data count: ", type(final_hashs))
                    if dkey in helper_hashs:
                        hashtags_data = final_hashs[dkey]
                    else:
                        hashtags_data = {}
                    for hashs_keys, hashs_val in hashtags_data.items():
                        hashtags_data[hashs_keys] = hashtags_data.setdefault(hashs_keys, 0) + hashs_val

                    final_hashs[dkey] = hashtags_data
            # print('final_results len:', rank, final_results)
        # Turn everything off since we have got the data from all the processes
        # Except for the master. It is but obvious, still need to code as such.
        for i in range(size - 1):
            # Appreciate them and wave the process dasvidaniya
            comm.send('Thanks a ton for all the hard work. Now exit and rest.', dest=(i + 1), tag=(i + 1))
    # print("Final Data :", end="")
    # print(final_results)
    # Print the final result using display_final_output
    if search_type == "hashtags":
        display_all_output(final_results, final_hashs, search_type)
    else:
        display_final_output(final_results, search_type)


def helper_node_handler(comm, file_name, melboure_grid_data, search_type):
    # Find the process info
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Parse the JSON file line by line and perform the analysis and return the results
    # Helper nodes will store data and send it back to Leader when asked for
    if search_type == "hashtags":
        helper_results, helper_hashs = twitter_json_parser(rank, file_name, size, melboure_grid_data, search_type)
    else:
        helper_results = twitter_json_parser(rank, file_name, size, melboure_grid_data, search_type)

    # Now that we have our results/counts. We wait for for Leader's further instructions.
    while True:
        # in_comm = comm.recv(source=LEADER_RANK, tag=rank)
        in_comm = comm.recv(source=LEADER_RANK, tag=rank)
        # Check if instruction has been sent from Leader.
        # It must be "Please give me the analyzed data."
        # or 'Thanks a ton for all the hard work. Now exit and rest.' .. in string format
        if isinstance(in_comm, str):
            if in_comm in ('Please give me the analyzed data.'):
                # Send data back to the Leader. He said pleases
                # print("Process: ", rank, " sending back ", len(counts), " items") #testing
                comm.send(helper_results, dest=LEADER_RANK, tag=REGION_INFO)
                if search_type == "hashtags":
                    comm.send(helper_hashs, dest=LEADER_RANK, tag=HASHTAGS_INFO)
            elif in_comm in ('Thanks a ton for all the hard work. Now exit and rest.'):
                exit(0)


# Find the grid data from the JSON
def melbourne_grid_json_parser(gird_data_path):
    # Use the encoding since Windows is not actually that friendly. It acts weirds without encoding.
    with open(gird_data_path, encoding="utf8") as mg_json_file:
        # read the files and then store in the transformed format. Keep only the relevant info.
        mg_data = json.loads(mg_json_file.read())
        melb_grid_data = {}

        for mgobj in mg_data["features"]:
            id = mgobj["properties"]["id"]
            melb_grid_data[id] = mgobj["properties"]
    return melb_grid_data


# In case the user is not able to figure out the input. Give them hints. Also known as 'HELP'.
def search_help():
    print('How to use:')
    print('For region counts: mpiexec /np <no of cores> python <program name> -i <JSON data file path>')
    print("example: mpiexec /np 8 python concept_tests/twitter_scanner.py -i data/smallTwitter.json")
    print('For hashtag counts: mpiexec /np <no of cores> python <program name> -i <JSON data file path> -t')
    print("example: mpiexec /np 8 python concept_tests/twitter_scanner.py -i data/smallTwitter.json -t")
    print("if 'mpiexec /np' does not work use mpirun -np")

def read_arguments(arguments):
    # Initialise variables with default values
    file_name = ''
    search_type = 'regions'

    # Try to read in arguments
    try:
        opts, args = getopt.getopt(arguments, "hi:tms:")
    except getopt.GetoptError as error:
        print(error)
        search_help()
        sys.exit(2)
    # If arguments were read then check if it makes sense.
    for opt, arg in opts:
        if opt == '-h':
            # if user asked for help
            search_help()
            sys.exit()
        elif opt in "-i":
            # User provides the path of the JSON file it wants to be processed
            file_name = arg
        elif opt in "-t":
            # User provides the path of the JSON file it wants to be processed
            search_type = "hashtags"
     # Return all the arguments
    return file_name, search_type


# This is where everything start
if __name__ == '__main__':
    # User provided inputs
    arguments = sys.argv[1:]

    # Pick up the inputs(arguments) from user and check what she/he wants
    file_name, search_type = read_arguments(arguments)

    # Initiate MPI programming and get rank details of each process.
    # This will help us in deciding if the process is leader or helper
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    # Read the Melbourne Grid file for finding the details of coordinates for each region
    # It is being run for every process since running it in parallel does not makes sense.
    # Due to its small size, parallel processing will be more time consuming.
    melbourne_grid_data = melbourne_grid_json_parser('melbGrid.json')

    if rank == 0:
        # Leader Node - I Assign work and do some work too simultaneously :)
        leader_node_handler(comm, file_name, melbourne_grid_data, search_type)
    else:
        # Helper Node - I can help with processing and get the processing get done quickly :D
        helper_node_handler(comm, file_name, melbourne_grid_data, search_type)

