import os
import sys
import pandas
import json
import datetime
import time
import graphiteudp
import logging

# TODO inode support
# {
#     filename: {
#         last_size :: int pointer offset
#         last_ts :: timestamp
#     }
# }


def nginx_time(x):
    return datetime.datetime.strptime(x, '%d/%b/%Y:%H:%M:%S +0000')


def load_data(state, filename):
    '''
    return: (state, lines :: [str], time_slice :: float seconds)
    '''

    cur_size = os.path.getsize(filename)
    cur_ts = time.time()

    if filename not in state:
        state[filename] = {
            'last_size': cur_size,
            'last_ts': cur_ts
        }

        time_slice = 0
        lines = []

        return state, lines, time_slice

    else:
        file_state = state[filename]

        if file_state['last_size'] > cur_size:
            file_state['last_size'] = 0

        f = file(filename)

        f.seek(file_state['last_size'])

        lines = f.readlines()
        time_slice = cur_ts - file_state['last_ts']

        file_state['last_size'] = f.tell()
        file_state['last_ts'] = cur_ts

        return state, lines, time_slice


def parse_json_data(lines, log_fields):
    parsed_data = []

    for line in lines:
        try:
            line_data = json.loads(line)
            line_parsed_data = {}

            for key, parser in log_fields.iteritems():
                if key in line_data:
                    try:
                        line_parsed_data[key] = parser(line_data[key])
                    except:
                        pass

            parsed_data.append(line_parsed_data)
        except:
            pass

    return pandas.DataFrame(parsed_data)


class Plugin(object):
    def __init__(
            self,
            state_filename,
            log_filename,
            log_fields,
            graphite_host,
            graphite_prefix):

        self.state_filename = state_filename
        self.log_filename = log_filename
        self.log_fields = log_fields
        self.graphite_host = graphite_host
        self.graphite_prefix = graphite_prefix

        if len(sys.argv) > 1 and sys.argv[1] == '-':
            self.test_mode = True
            logging.basicConfig(level=logging.DEBUG)
        else:
            self.test_mode = False

        graphiteudp.init(host=self.graphite_host, prefix=self.graphite_prefix, debug=self.test_mode)


    def read_data(self):
        if not self.test_mode:
            if os.path.exists(self.state_filename):
                state = json.load(file(self.state_filename))
            else:
                state = {}

            state, lines, self.time_slice = load_data(state, self.log_filename)

            json.dump(state, file(self.state_filename, 'w+'))
        else:
            lines = sys.stdin.readlines()
            self.time_slice = 60.

        return parse_json_data(lines, self.log_fields)


    def send(self, name, val):
        graphiteudp.send(name, val)


    def send_qps(self, name, val):
        self.send(name, val/self.time_slice)
