import os,logging,sys,errno
from itertools import islice,zip_longest
from collections import defaultdict as dd
from functools import partial
import mmap

def mapcount(filename):

    if not os.path.isfile(filename):
        return 0
    try:
        return count_lines(filename)
    except:
        return 0
    
def count_lines(filename):
    '''
    Counts line in file
    '''
    f = open(filename, "r+")
    buf = mmap.mmap(f.fileno(), 0)
    lines = 0
    readline = buf.readline
    while readline():
        lines += 1
    return lines


# you need to definte it like this or the defaultdict is not pickable and multiprocessing can't use it
def thl_default_(value):return value
def read_thl_map(map_path,default_value):
    default_ = partial(thl_default_,default_value)
    thl_lab_map = dd(default_)
    with open(map_path) as i:
        for elem in i:
            elem = elem.strip().split()
            thl_lab_map[elem[0]] = elem[1]
    return thl_lab_map


def batched(iterable, n):
    "Batch data into lists of length n. The last batch may be shorter."
    # batched('ABCDEFG', 3) --> ABC DEF G
    it = iter(iterable)
    while True:
        batch = list(islice(it, n))
        if not batch:
            return
        yield batch
        
def progressBar(value, bar_length=20):
    '''
    Writes progress bar, given value (eg.current row) and endvalue(eg. total number of rows)
    '''
    spaces = ' ' * (bar_length - len(value))
    sys.stdout.write("\rLines Processed: {0}".format(value + spaces))
    sys.stdout.flush()


def file_exists(fname):
    '''
    Function to pass to type in argparse
    '''
    if os.path.isfile(fname):
        return str(fname)
    else:
        print(fname + ' does not exist')
        sys.exit(1)
 
log_levels = {
    'critical': logging.CRITICAL,
    'error': logging.ERROR,
    'warn': logging.WARNING,
    'warning': logging.WARNING,
    'info': logging.INFO,
    'debug': logging.DEBUG
}

def make_sure_path_exists(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

def configure_logging(logger,log_level,log_file):
    # Format for our loglines
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(levelname)-8s - %(message)s","%Y-%m-%d %H:%M:%S")

    # Setup console logging
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    # Setup file logging as well
    fh = logging.FileHandler(log_file,mode='w')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
