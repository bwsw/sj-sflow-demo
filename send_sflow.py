#!/usr/bin/python

import sys
import getopt
import time
import socket


class Params:
    interval = None
    delay = None
    files = None
    field_index = None
    host = None
    port = None
    svsocket = None

    def __init__(self,
                 interval=interval,
                 delay=delay,
                 files=files,
                 field_index=field_index,
                 host=host,
                 port=port):
        self.interval = interval
        self.delay = delay
        self.files = files
        self.field_index = field_index
        self.host = host
        self.port = port
        self.svsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.svsocket.connect((self.host, self.port))


def do_after_interval(*args):
    time.sleep(args[0])


def serialize(data):
    return ','.join(data)


def send(data, connection):
    connection.sendall(serialize(data))


def line_parse(line):
    return line.split(',')


def check_changes(line, last_field_value, changes_count):
    if line[params.field_index] != last_field_value:
        changes_count += 1
    last_field_value = line[params.field_index]
    if changes_count > 0 and changes_count == params.interval:
        do_after_interval(delay)
        changes_count = 0
    return last_field_value, changes_count


def read_line(number, line, last_field_value, changes_count):
    line = line_parse(line)
    print changes_count
    print line

    if number == 0:
        last_field_value = line[params.field_index]

    last_field_value, changes_count = check_changes(line, last_field_value, changes_count)

    send(line, params.svsocket)

    return last_field_value, changes_count


def read_file(file):
    last_value = None
    changes_count = 0
    for number, line in enumerate(file):
        last_value, changes_count = read_line(number, line, last_value, changes_count)


def main(params):
    for filename in params.files:
        # openfile = open(filename, 'r')
        with open(filename, 'rt') as openfile:
            read_file(openfile)
            openfile.close()
    params.svsocket.close()


def usage():
    """
    Help text.
    """
    usg = """
    Usage: python script.py [options] --port=PORT FILES
    Options:

    --help                  Print usage
    -i, --interval=0        Set interval between delay
    -d, --delay=1           Set delay seconds
    -f, --field=0           Set field number for tracking changes
    -h, --host=127.0.0.1    Set connection host
    -p, --port              Set connection port
    """
    print(usg)


try:
    optlist, args = getopt.getopt(sys.argv[1:], "h:i:d:p:", ["help", "interval=", "delay=", "host=", "port="])
except getopt.GetoptError as error:
    print str(error)
    usage()
    sys.exit(2)

# Default base params
interval = 0
delay = 1
field_index = 0
host = "127.0.0.1"
port = None

# Parse option list
for opt, arg in optlist:
    if opt == "--help":
        usage()
        sys.exit()
    elif opt in ("-i", "--interval"):
        interval = int(arg)
    elif opt in ("-d", "--delay"):
        delay = float(arg)
    elif opt in ("-h", "--host"):
        host = arg
    elif opt in ("-p", "--port"):
        port = int(arg)
    elif opt in ("-f", "--field"):
        field_index = arg-1

if not port:
    print("Port is required argument.")
    sys.exit(2)

# main program
if __name__ == "__main__":
    params = Params(interval=interval,
                    delay=delay,
                    files=args,
                    field_index=field_index,
                    host=host,
                    port=port)
    main(params)


