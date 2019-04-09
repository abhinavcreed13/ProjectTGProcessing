import getopt, sys
import re


class Utilities:

    def parse_cmd_arguments(argv):
        # Initialise Variables
        data_file = ''

        # Read arguments
        try:
            opts, args = getopt.getopt(argv, "xd:")
        except getopt.GetoptError as error:
            print(error)
            sys.exit(2)
        for opt, arg in opts:
            if opt == '-x':
                sys.exit()
            elif opt in ("-d"):
                data_file = arg

        # Return all the arguments
        return data_file

    def generate_output(final_data):
        print('')
        print('Total number of Twitter posts:')
        for data_tuple in final_data[0]:
            print(data_tuple[0] + ':', "{:,}".format(data_tuple[1]), 'posts')
        print('')
        print('Top 5 hashtags in each grid cells:')
        for data_tuple in final_data[0]:
            print(data_tuple[0] + ':', '(' + str(final_data[1][data_tuple[0]])[1:-1] + ')')

    def write_log(rank, text, isNew, IS_LOGGING_ENABLED=False):
        file = None
        if IS_LOGGING_ENABLED:
            if isNew:
                file = open('output_' + str(rank) + '.txt', 'w')
            else:
                file = open('output_' + str(rank) + '.txt', 'a')
            file.write(text + '\n')
            file.close()

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

    def extract_hashtags_from_text(text):
        hashtags = []
        # create regex pattern
        regex_p = re.compile('#\w+')
        txt_len = len(text)
        for m in regex_p.finditer(text):
            idx_span = m.span()
            try:
                # validate found hashtag
                if idx_span[0] - 1 >= 0 and idx_span[1] < txt_len:
                    if text[idx_span[0] - 1] == ' ' and text[idx_span[1]] == ' ':
                        hashtags.append(m.group())
            except Exception as e:
                pass
        return hashtags
