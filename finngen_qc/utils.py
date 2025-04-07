import os,logging,sys,errno,gzip,mmap,math
from itertools import islice,zip_longest
from collections import defaultdict as dd
from functools import partial
import pandas as pd
import urllib.request
import http.client as httplib

dir_path = os.path.dirname(os.path.realpath(__file__))

def init_harmonization(args,logger):

    logger.info("UPDATING USAGI")
    repo = args.config['harmonization_repo']
    urls = [(repo+elem[1],os.path.join(dir_path,'data',elem[1])) for elem in args.config['harmonization_files'].values()]

    try:
        for url,out_file in urls:
            urllib.request.urlretrieve(url,out_file)
    except:
        logger.warning("COULD NOT DOWNLOAD FILES: USAGI FILES NOT UPDATED")
        logger.warning(urls)

    # READ IN JAVIER/USAGI FILES WITH PROPER RENAMING OF COLUMNS
    for key,value in args.config['harmonization_files'].items():
        cols,fname = value
        sep = ',' if fname.endswith('.csv') else '\t'
        rename = {col:new_col for col,new_col in args.config['harmonization_col_map'].items() if col in cols}
        args.config[key] = pd.read_csv(os.path.join(dir_path,'data',fname),usecols = cols,sep = sep).rename(columns=rename).drop_duplicates()
        
    #SPECIFIC RENAMING NEEDED
    args.config['unit_conversion']= args.config['unit_conversion'].rename(columns={'source_unit_valid':'MEASUREMENT_UNIT'})
    args.config['usagi_mapping']['harmonization_omop::OMOP_ID'] =args.config['usagi_mapping']['harmonization_omop::OMOP_ID'].astype(int)

    tmp_df = args.config['usagi_mapping'][args.config['usagi_mapping']["TEST_NAME_ABBREVIATION"] == 'p-vrab-o']
    logger.debug(tmp_df)

    if args.harmonization:
        #merges harmonization table from vincent with chosen target unit for each concept id
        logger.debug('merge harmonization counts and table')
        harmonization_counts = pd.read_csv(args.harmonization,sep='\t')
        args.config['unit_conversion'] = pd.merge(args.config['unit_conversion'],harmonization_counts,on=['harmonization_omop::omopQuantity','harmonization_omop::MEASUREMENT_UNIT'])
        #DEBUG
        logger.debug(args.config['unit_conversion'][args.config['unit_conversion']['harmonization_omop::OMOP_ID'] == 3027238])


    #logger.debug(args.config['usagi_units'])
    logger.debug("USGAGI MAPPING")
    logger.debug(args.config['usagi_mapping'])
    #logger.debug(args.config['unit_abbreviation_fix'])
    logger.debug("UNIT CONVERSION")
    logger.debug(args.config['unit_conversion'])
    return args




    
def init_log_files(args):
    # setup error file
    args.err_file = os.path.join(args.out,f"{args.prefix}_err.txt")
    with open(args.err_file,'wt') as err:err.write('\t'.join(args.config['err_cols']) + '\n')
    args.warn_file = os.path.join(args.out,f"{args.prefix}_warn.txt")
    with open(args.warn_file,'wt') as warn:warn.write('\t'.join(args.config['err_cols']) + '\n')

    args.unit_file = os.path.join(args.out,f"{args.prefix}_unit.txt")
    with open(args.unit_file,'wt') as unit:unit.write('\t'.join(['FINREGISTRYID','TEST_DATE_TIME','TEST_NAME_ABBREVIATION','old_unit','MEASUREMENT_UNIT','SOURCE']) + '\n')

    args.abbr_file = os.path.join(args.out,f"{args.prefix}_abbr.txt")
    with open(args.abbr_file,'wt') as abbr:abbr.write('\t'.join(['FINREGISTRYID','TEST_DATE_TIME','old_abbr','MEASUREMENT_UNIT','TEST_NAME_ABBREVIATION']) + '\n')
    


def have_internet() -> bool:
    conn = httplib.HTTPSConnection("8.8.8.8", timeout=5)
    try:
        conn.request("HEAD", "/")
        return True
    except Exception:
        return False
    finally:
        conn.close()


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
        # if file is large
        if os.path.getsize(f)/(1024**3) > 1:
            with open_func(f, 'rb') as i:
                buf = i.read(LEARN_SIZE)
                size /= (len(buf) // buf.count(b'\n'))
            om = math.floor(math.log(size, 10))
            note = 'estimated'
            size = math.pow(10,om+1)
        else:
            with gzip.open(f, 'rb') as f:
                for i, l in enumerate(f):pass
            note = 'exact'
            size = i+1
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
# dict where i can initialize value based on key
class smart_dict(dict):
    def __missing__(self, key):
        return key

def read_map(map_path,keep_original=True,default_value=None):
    # if a default value is passed it wil return a dict that initalizes such value as default
    if default_value:
        default_ = partial(map_default_,default_value)
        map_dict = dd(default_)
    # this options intead keeps the original value if missing in the mapping
    elif keep_original:
        map_dict =smart_dict()
    # standard dictionary
    else:
        map_dict = {}
    with open(map_path) as i:
        for elem in i:
            elem = elem.strip().split('\t')
            map_dict[elem[0]] = elem[1]
    return map_dict

class DefaultDict(dd):
    def __missing__(self, key):
        return self.default_factory(key)


    
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



def write_chunk(df,i,out_file,out_cols,final_rename,logger = None):
    # write header for first chunk along with df and print some info
    mode,header ='a',False
    # write header and create new file if it's first chunk
    if i ==0:
        if logger:
            logger.debug(df.head())
        mode = 'w'
        header = True

    df.rename(columns = final_rename,inplace=True)
    df[out_cols].to_csv(out_file, mode=mode, index=False, header=header,sep="\t")
