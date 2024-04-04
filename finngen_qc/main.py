import pandas as pd
import argparse,logging,os
from functools import partial
import multiprocessing as mp
import numpy as np
from utils import file_exists,log_levels,configure_logging,make_sure_path_exists,progressBar,batched,read_thl_map
from magic_config import config
from datetime import datetime
from filters.filter_minimal import filter_minimal 


dir_path = os.path.dirname(os.path.realpath(__file__))

def chunk_reader(raw_file,chunk_size,config,err_file):
    """
    Iterator that spews out chunks and exits early in case of test
    """
    
    logger.debug(args.config['cols'])
    size = 0
    with pd.read_csv(raw_file, chunksize=args.chunk_size,sep="\t",dtype=str,usecols = args.config['cols']) as reader:
        for i,chunk in enumerate(reader):
            # INIT ERR AND ERR_VALUE columns
            chunk['ERR'] = "0"
            chunk['ERR_VALUE'] = "NA"
            if args.test and i==1:
                break

            yield chunk.rename(columns=config['rename_cols'])


def all_filters(df,args):
    df = df.pipe(filter_minimal,args)
    return df

def write_chunk(df,i,args):
    # write header for first chunk along with df and print some info
    out_file = os.path.join(args.out,f"{args.prefix}_munged.txt")
    mode,header ='a',False
    # write header and create new file if it's first chunk
    if i ==0:
        logger.debug(df.head())
        mode = 'w'
        header = True

    # write err_df args.err
    mask = df['ERR'] == '0'
    err_df =df[~mask][args.config['err_cols']]
    err_df.to_csv(args.err_file, mode='a', index=False, header=False,sep="\t")
    # write final df to out_file
    df = df[mask][args.config['out_cols']]
    df.to_csv(out_file, mode=mode, index=False, header=header,sep="\t")
    return len(df)+len(err_df)


def result_iterator(args):
    """
    Regular result iterator that applies the filter to each args.chunk_size sized chunk
    """
    for i,chunk in enumerate(chunk_reader(args.raw_data,args.chunk_size,args.config,args.err_file)):
        df= all_filters(chunk,args)
        yield i,df

def result_iterator_multi(args):
    """
    Multiproc result iterator. It reads in the raw file in args.jobs chunks of args.chunk_size each
    """
    logger.info(f"Input path:{args.raw_data}")
    ctx = mp.get_context('spawn')
    pool = ctx.Pool(args.jobs)
    # get function with only df as input
    multi_func = partial(all_filters,args=args)
    # read in chunk and apply further split
    for i,chunks in enumerate(batched(chunk_reader(args.raw_data,int(args.chunk_size/args.jobs),args.config,args.err_file),args.jobs)):
        results = pool.imap(multi_func, chunks)
        df = pd.concat(list(results))
        yield i,df

def main(args):
    """
    Main functions that calls the iterators based on regular/multiproc version
    """
    res_it = result_iterator_multi if args.mp else result_iterator
    size = 0
    start_time = datetime.now()
    for i,df in res_it(args):
        size += write_chunk(df,i,args)
        progressBar(str(size))

    print('\nDone.')
    logger.info('Duration: {}'.format(datetime.now() - start_time))

    return

    
if __name__=='__main__':
    
    parser=argparse.ArgumentParser(description="KANTA LAB preprocecssing/QC pipeline.")
    parser.add_argument("--raw-data", type=file_exists, help =  "Path to input raw file", default = os.path.join(dir_path,"test","raw_data_test.txt"))
    parser.add_argument("--log",  default="warning", choices = log_levels, help=(  "Provide logging level. Example --log debug', default='warning'"))
    parser.add_argument("--test",action='store_true')
    parser.add_argument("--mp",action='store_true',help="run multiproc")

    parser.add_argument("--jobs",default = os.cpu_count(),type = int, help ="number of jobs to run in parallel")
    parser.add_argument('-o',"--out",type = str, help = "Folder in which to save the results", default = os.getcwd())
    parser.add_argument("--prefix",type=str,default="kanta")
    parser.add_argument("--chunk-size",type=int,help="Number of rows to be processed by each chunk",default = 100)
    args = parser.parse_args()

    make_sure_path_exists(args.out)
    # setup logging
    logger = logging.getLogger(__name__)
    log_file = os.path.join(args.out,f"{args.prefix}_log.txt")
    configure_logging(logger,log_levels[args.log],log_file)

    # setup config
    args.config = config
    args.config['cols']  = list(config['rename_cols'].keys()) + config['other_cols']
    logger.debug(args.config)

    # replace path with actual map
    args.config['thl_lab_map'] = read_thl_map(os.path.join(dir_path,args.config['thl_lab_map']))
    logger.debug(args.config['thl_lab_map'])
    # setup error file
    args.err_file = os.path.join(args.out,f"{args.prefix}_err.txt")
    with open(args.err_file,'wt') as err:err.write('\t'.join(args.config['err_cols']) + '\n')
    logger.info("START")
    if os.path.basename(args.raw_data) == "raw_data_test.txt":
        logger.warning("RUNNING IN TEST MODE")
    # make sure the chunk size is at least the size of the the jobs
    args.chunk_size = max(args.chunk_size,args.jobs)

    main(args)
        
    logger.info("END")
