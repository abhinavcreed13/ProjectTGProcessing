###  mpiexec /np 8 python concept_tests/twitter_scan_logic.py -i data/smallTwitter.json
# IMPORTANT: Use the above code to execute the program

# Import mpi so we can run on more than one node and processor. That is what our project requires.
from mpi4py import MPI

import sys, getopt

# Import regular expressions to look for topics and mentions, json to parse tweet data
import re, json, operator

# Constants
TOPIC_SEARCH_REGEX = '#\w+'
MENTION_SEARCH_REGEX = '@\w+'
LEADER_RANK = 0

def print_matches(matches, squery):
  print(squery, " was found ", matches.setdefault(squery,0), " times.")

def print_mentions(mentions):
  sorted_counts = sorted(mentions.items(), key=operator.itemgetter(1))
  print("The top ten users mentioned are:")
  for mention in reversed(sorted_counts[-10:]):
    user, times = mention
    print(user, ":", times)

def print_topics(topics):
  sorted_counts = sorted(topics.items(), key=operator.itemgetter(1))
  print("The top ten trending topics are:")
  for topic in reversed(sorted_counts[-10:]):
    topic, times = topic
    print(topic, ":", times)

def display_final_output(final_results, search_type, search_query):
    if search_type in ("mentions"):
        print_mentions(final_results)
    elif search_type in ("topic"):
        print_topics(final_results)
    elif stype in ("string_search"):
        print_matches(final_results, search_query)

def count_regex(tweet, regex):
  # We want to find the most mentioned @user in the dataset
  counts = {}
  occurences = re.findall(regex, tweet['text'])
  for occurence in occurences:
    counts[occurence] = counts.setdefault(occurence,0) + 1
  return counts

def trending_topics(tweet):
  # We want to find the most mentioned #word in the dataset
  return count_regex(tweet, TOPIC_REGEX)

def user_mentions(tweet):
  # We want to find the most mentioned @user in the dataset
  return count_regex(tweet, MENTION_REGEX)

def tweet_to_json(tweet):
  # Remove poorly formatted urls and new line / carriage returns
  tweet = re.sub('"source":"<a.*?>.*?</a>"','"source":""',tweet)
  tweet = re.sub("(\r|\n)+", '', tweet)
  tweet = json.loads(tweet)
  return tweet

def process_tweet(counts, stype, squery, tweet):
    results = []
    if stype in ("mentions"):
        results = user_mentions(tweet)
    elif stype in ("topic"):
        results = trending_topics(tweet)
    elif stype in ("string_search"):
        regex = '\\b' + squery + '\\b'
        results = count_regex(tweet, regex)
    for k, v in results.items():
        counts[k] = counts.setdefault(k, 0) + v
    return counts


def process_tweets(rank, input_file, processes, stype, squery):
  with open(input_file) as f:
    rows = csv.DictReader(f)
    occurences = {}
    # Send tweets to slave processes
    try:
      for i, line in enumerate(rows):
        if i%processes == rank:
          tweet = line['value']
          try:
            tweet = tweet_to_json(tweet)
            occurences = process_tweet(occurences, stype, squery, tweet)
          except ValueError:
            print("Malformed JSON in tweet ", i)
    except csv.Error:
      print("Could not read line in csv.")
  return occurences

def twitter_json_parser(rank, file_name, size, search_type, search_query, melboure_grid_data):
    region_post_count = {}

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
                            'screen_name':line_obj['doc']['user']['screen_name'],
                            'text': line_obj['doc']['text'],
                            'coordinates': line_obj['doc']['coordinates']['coordinates'],
                            'hashtags': line_obj['doc']['entities']['hashtags']
                        }

                        region_key = get_region_from_tweet(melboure_grid_data, ret, rank)
                        if region_key is not None:
                            if region_key in region_post_count:
                                region_post_count[region_key] = region_post_count[region_key] + 1
                            else:
                                region_post_count[region_key] = 1
                except Exception as e:
                    # non parsable line
                    # print('non parsable line')
                    pass
    print(rank, ":", end="")
    print(region_post_count)
    return region_post_count


def get_region_from_tweet(melboure_grid_data, tweet_obj, rank):
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
    results = []
    # Now ask all processes except oursevles to return counts
    for i in range(processes - 1):
        # Send request to slaves to send back the data
        comm.send('Please give me the analyzed data.', dest=(i + 1), tag=(i + 1))
    for i in range(processes - 1):
        # Receive data the data sent from helpers and append it in the leaders file
        # and then return the final count
        results.append(comm.recv(source=(i + 1), tag=LEADER_RANK))
    return results


def leader_node_handler(comm, file_name, search_type, search_query, melboure_grid_data):
    # Find the process info
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Parse the JSON file line by line and perform the analysis and store in final result
    # We will add the data from Helper process to this once they have finished the analysis
    final_results = twitter_json_parser(rank, file_name, size, search_type, search_query, melboure_grid_data)

    # if multicore then Leader needs to communicate with processes and collect their analyzed data
    if size > 1:
        # Use the communicator to give instruction to Helpers to provide the data back
        helper_results = leaders_helper_communicator(comm)

        # Collect all the data from Helpers in the final_results
        for helper_data in helper_results:

            # loop on key value pair and add the entries
            for dkey, dval in helper_data.items():
                final_results[dkey] = final_results.setdefault(dkey, 0) + dval

        # Turn everything off since we have got the data from all the processes
        # Except for the master. It is but obvious, still need to code as such.
        for i in range(size - 1):
            # Appreciate them and wave the process dasvidaniya
            comm.send('Thanks a ton for all the hard work. Now exit and rest.', dest=(i + 1), tag=(i + 1))
    print("Final Data :", end="")
    print(final_results)
    # Print the final result using display_final_output
    display_final_output(final_results, search_type, search_query)


def helper_node_handler(comm, file_name, search_type, search_query, melboure_grid_data):
    # Find the process info
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Parse the JSON file line by line and perform the analysis and return the results
    # Helper nodes will store data and send it back to Leader when asked for
    helper_results = twitter_json_parser(rank, file_name, size, search_type, search_query, melboure_grid_data)

    # Now that we have our results/counts. We wait for for Leader's further instructions.
    while True:
        in_comm = comm.recv(source=LEADER_RANK, tag=rank)

        # Check if instruction has been sent from Leader.
        # It must be "Please give me the analyzed data."
        # or 'Thanks a ton for all the hard work. Now exit and rest.' .. in string format
        if isinstance(in_comm, str):
            if in_comm in ('Please give me the analyzed data.'):
                # Send data back to the Leader. He said pleases
                # print("Process: ", rank, " sending back ", len(counts), " items") #testing
                comm.send(helper_results, dest=LEADER_RANK, tag=LEADER_RANK)
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
    print('How to use: python <program name> -i <JSON data file path> [opts] ')
    print('Opts are:')
    print('  -[tms] <query>       flag to search topics, mentions or search')
    print('                       by a query string respectively, -s requires')
    print('                       a search string. (optional, default is mentions).')


def read_arguments(arguments):
    # Initialise variables with default values
    file_name = 'data/smallTwitter.json'
    search_type = 'mentions'
    search_query = ''

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
        elif opt in "-m":
            # User wants to know who was mentioned most times in tweets in descending order
            search_type = 'mentions'
        elif opt in "-t":
            # User wants to know which #hashtag was mentioned most times in tweets in descending order
            search_type = 'topic'
        elif opt in "-s":
            # Search for specific string that user is looking for and has provided in arguments
            search_type = 'string_search'
            search_query = arg

    # Return all the arguments
    return file_name, search_type, search_query


# This is where everything start
if __name__ == '__main__':
    # User provided inputs
    arguments = sys.argv[1:]

    # Pick up the inputs(arguments) from user and check what she/he wants
    file_name, search_type, search_query = read_arguments(arguments)

    # Initiate MPI programming and get rank details of each process.
    # This will help us in deciding if the process is leader or helper
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    # Read the Melbourne Grid file for finding the details of coordinates for each region
    # It is being run for every process since running it in parallel does not makes sense.
    # Due to its small size, parallel processing will be more time consuming.
    melbourne_grid_data = melbourne_grid_json_parser('data/melbGrid.json')

    if rank == 0:
        # Leader Node - I Assign work and do some work too simultaneously :)
        leader_node_handler(comm, file_name, search_type, search_query, melbourne_grid_data)
    else:
        # Helper Node - I can help with processing and get the processing get done quickly :D
        helper_node_handler(comm, file_name, search_type, search_query, melbourne_grid_data)

# # Hello World program in Python
# import operator
#
# region_data = {
#     'E1': 50,
#     'C3': 10,
#     'D2': 20,
#     'A3': 40,
#     'B2': 30}
# hashtag_data = {
#     'E1': {'counts': 50, 'a': 2, 'b': 4, 'c': 3, 'd': 1, 'e': 10, 'f': 7, 'g': 6},
#     'C3': {'counts': 10, 'a': 3, 'b': 6, 'c': 4, 'd': 3, 'e': 16, 'f': 7, 'g': 6},
#     'D2': {'counts': 20, 'a': 4, 'b': 6, 'c': 5, 'd': 5, 'e': 23, 'f': 7, 'g': 6},
#     'A3': {'counts': 40, 'a': 3, 'b': 5, 'c': 8, 'd': 3, 'e': 2, 'f': 7, 'g': 6},
#     'B2': {'counts': 30, 'a': 1, 'b': 8, 'c': 1, 'd': 7, 'e': 20, 'f': 7, 'g': 6}}
#
# print
# region_data
# print
# hashtag_data
#
# sorted_counts = sorted(region_data.items(), key=operator.itemgetter(1))
# print
# sorted_counts
#
# for hashs in hashtag_data.items():
#     # sort_hash = sorted(hashs.items(), key=operator.itemgetter(1))
#     print
#     hashs
#
# for regionkey in region_data.keys():
#     # sort_hash = sorted(hashs.items(), key=operator.itemgetter(1))
#     # print (hashtag_data[regionkey].values())
#     sort_hash = sorted(hashtag_data[regionkey].items(), key=operator.itemgetter(1))
#     print(hashtag_data[regionkey])
#     print(sort_hash)