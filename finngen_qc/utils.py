import os,logging,sys,errno,gzip,mmap,math
from itertools import islice,zip_longest
from collections import defaultdict as dd
from functools import partial
import pandas as pd
import urllib.request

dir_path = os.path.dirname(os.path.realpath(__file__))

def init_harmonization(args):
    repo = args.config['harmonization_repo']
    for key,value in args.config['harmonization_files'].items():
        fname = value[1]
        url = repo + fname
        out_file = os.path.join(dir_path,'data',fname)
        urllib.request.urlretrieve(url,out_file)


    args.config['usagi_units'] = pd.read_csv(os.path.join(dir_path,'data',args.config['harmonization_files']['usagi_units'][1]),usecols=args.config['harmonization_files']['usagi_units'][0])
    args.config['usagi_mapping'] = pd.read_csv(os.path.join(dir_path,'data',args.config['harmonization_files']['usagi_mapping'][1]),usecols=args.config['harmonization_files']['usagi_mapping'][0])
    args.config['unit_abbreviation_fix'] = pd.read_csv(os.path.join(dir_path,'data',args.config['harmonization_files']['unit_abbreviation_fix'][1]),sep='\t',usecols=args.config['harmonization_files']['unit_abbreviation_fix'][0])


    #fix mapping
    args.config['usagi_mapping'][['TEST_NAME_ABBREVIATION','MEASUREMENT_UNIT']] = args.config['usagi_mapping']['sourceCode'].str.replace(']','').str.split('[',expand=True)

    args.config['usagi_mapping']['conceptId'] =args.config['usagi_mapping']['conceptId'].astype(int)
    
    approved_mask = args.config['usagi_mapping']['mappingStatus'] != "APPROVED"
    
    args.config['usagi_mapping'].loc[approved_mask,'conceptId'] = 0
    
    return args

    
def init_log_files(args):
    # setup error file
    args.err_file = os.path.join(args.out,f"{args.prefix}_err.txt")
    with open(args.err_file,'wt') as err:err.write('\t'.join(args.config['err_cols']) + '\n')
    args.unit_file = os.path.join(args.out,f"{args.prefix}_unit.txt")
    with open(args.unit_file,'wt') as unit:unit.write('\t'.join(['FINREGISTRYID','TEST_DATE_TIME','TEST_NAME_ABBREVIATION','old_unit','MEASUREMENT_UNIT','SOURCE']) + '\n')

    args.abbr_file = os.path.join(args.out,f"{args.prefix}_abbr.txt")
    with open(args.abbr_file,'wt') as abbr:abbr.write('\t'.join(['FINREGISTRYID','TEST_DATE_TIME','old_abbr','MEASUREMENT_UNIT','TEST_NAME_ABBREVIATION']) + '\n')
    

    

def mapcount(filename):

    if not os.path.isfile(filename):
        return 0
    try:
        return count_lines(filename)
    except:
        return 0

def estimate_lines(f):
    """ Estimate the number of lines in the given f(s) """


    # Get total size of all fs
    LEARN_SIZE = int(math.pow(2,18))                                     
    size = os.path.getsize(f)
    open_func = gzip.open if f.endswith('.gz') else open
    if f.endswith('.gz'):
        with open_func(f, 'rb') as i:
            buf = i.read(LEARN_SIZE)
            size /= (len(buf) // buf.count(b'\n'))
        om = math.floor(math.log(size, 10))
        note = 'estimated'
        size = math.pow(10,om+1)
    else:
        note = 'exact'
        size = mapcount(f)
    return note,int(size)



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



def progressBar(value, endvalue, bar_length=20):
    '''
    Writes progress bar, given value (eg.current row) and endvalue(eg. total number of rows)
    '''

    percent = float(value) / endvalue
    arrow = '-' * int(round(percent * bar_length)-1) + '>'
    spaces = ' ' * (bar_length - len(arrow))

    sys.stdout.write("\rPercent: [{0}] {1}%".format(arrow + spaces, int(round(percent * 100))))
    sys.stdout.flush()

# you need to definte it like this or the defaultdict is not pickable and multiprocessing can't use it
def map_default_(value):return value
def read_map(map_path,default_value="NA"):
    if default_value:
        default_ = partial(map_default_,default_value)
        map_dict = dd(default_)
    else:
        map_dict = {}
    with open(map_path) as i:
        for elem in i:
            elem = elem.strip().split()
            map_dict[elem[0]] = elem[1]
    return map_dict


def batched(iterable, n):
    "Batch data into lists of length n. The last batch may be shorter."
    # batched('ABCDEFG', 3) --> ABC DEF G
    it = iter(iterable)
    while True:
        batch = list(islice(it, n))
        if not batch:
            return
        yield batch


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



def write_chunk(df,i,out_file,out_cols,logger = None):
    # write header for first chunk along with df and print some info
    mode,header ='a',False
    # write header and create new file if it's first chunk
    if i ==0:
        if logger:
            logger.debug(df.head())
        mode = 'w'
        header = True

    df[out_cols].to_csv(out_file, mode=mode, index=False, header=header,sep="\t")
