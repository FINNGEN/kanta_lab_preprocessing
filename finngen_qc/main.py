import pandas as pd
import argparse,logging,os
from functools import partial
import multiprocessing as mp
import numpy as np
from utils import file_exists,log_levels,configure_logging,make_sure_path_exists,progressBar,batched,mapcount,read_thl_map
from magic_config import config
from datetime import datetime
from filters.filter_minimal import filter_minimal 
from filters.lab_unit import unit_fixing
dir_path = os.path.dirname(os.path.realpath(__file__))


def chunk_reader(raw_file,chunk_size,config):
    """
    Iterator that spews out chunks and exits early in case of test
    """
    logger.debug(args.config['cols'])
    with pd.read_csv(raw_file, chunksize=chunk_size,sep="\t",dtype=str,usecols = args.config['cols']) as reader:
        for i,chunk in enumerate(reader):
            if args.test and i==1:
                break
            yield chunk.rename(columns=config['rename_cols'])


def all_filters(df,args):
    df = (
        df
        .pipe(filter_minimal,args)
        .pipe(unit_fixing,args)
    )
    return df

def write_chunk(df,i,args):
    # write header for first chunk along with df and print some info
    mode,header ='a',False
    # write header and create new file if it's first chunk
    if i ==0:
        logger.debug(df.head())
        mode = 'w'
        header = True

    df[args.config['out_cols']].to_csv(args.out_file, mode=mode, index=False, header=header,sep="\t")

def result_iterator(args):
    """
    Regular result iterator that applies the filter to each args.chunk_size sized chunk
    """
    for i,chunk in enumerate(chunk_reader(args.raw_data,args.chunk_size,args.config)):
        df= all_filters(chunk,args)
        yield i,df,len(chunk)

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
    chunk_size = int(args.chunk_size/args.jobs)
    for i,chunks in enumerate(batched(chunk_reader(args.raw_data,chunk_size,args.config),args.jobs)):
        results = pool.imap(multi_func, chunks)
        df = pd.concat(list(results))
        size = np.sum([len(elem) for elem in chunks])
        yield i,df,size

def main(args):
    """
    Main functions that calls the iterators based on regular/multiproc version
    """
    res_it = result_iterator_multi if args.mp else result_iterator
    size = 0
    start_time = datetime.now()
    for i,df,tmp_size in res_it(args):
        write_chunk(df,i,args)
        size += tmp_size
        progressBar(str(size))

    print('\nDone.')

    # Read sizes of out files and make sure it adds up
    c1 =mapcount(args.out_file) 
    c2 =mapcount(args.err_file)
    logger.info(f"{c1} {c2} {c1+c2-2}")
    logger.info('Duration: {}'.format(datetime.now() - start_time))
    
    return

    
if __name__=='__main__':
    
    parser=argparse.ArgumentParser(description="KANTA LAB preprocecssing/QC pipeline.")
    parser.add_argument("--raw-data", type=file_exists, help =  "Path to input raw file. File should be tsv.", default = os.path.join(dir_path,"test","raw_data_test.txt"))
    parser.add_argument("--log",  default="warning", choices = log_levels, help=(  "Provide logging level. Example --log debug', default='warning'"))
    parser.add_argument("--test",action='store_true',help="Reads first 1k lines only")
    parser.add_argument("--mp",action='store_true',help="Run multiproc")

    parser.add_argument("--jobs",default = os.cpu_count(),type = int, help ="Number of jobs to run in parallel (default = cpu count)")
    parser.add_argument('-o',"--out",type = str, help = "Folder in which to save the results (default = cwd)", default = os.getcwd())
    parser.add_argument("--prefix",type=str,default="kanta",help = "Prefix of the out files (default = kanta)")
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

    args.config['thl_lab_map'] = read_thl_map(os.path.join(dir_path,args.config['thl_lab_map_file']),'MISSING')
    args.config['thl_sote_map'] = read_thl_map(os.path.join(dir_path,args.config['thl_sote_map_file']),'NA')

    logger.debug(dict(list(args.config['thl_lab_map'].items())[0:2]))
    # setup error file
    args.err_file = os.path.join(args.out,f"{args.prefix}_err.txt")
    with open(args.err_file,'wt') as err:err.write('\t'.join(args.config['err_cols']) + '\n')
    args.unit_file = os.path.join(args.out,f"{args.prefix}_unit.txt")
    with open(args.unit_file,'wt') as unit:unit.write('\t'.join(['old_unit','new_unit']) + '\n')

    logger.info("START")
    
    if os.path.basename(args.raw_data) == "raw_data_test.txt":
        logger.warning("RUNNING IN TEST MODE")

    # make sure the chunk size is at least the size of the the jobs
    args.chunk_size = max(args.chunk_size,args.jobs)
    args.out_file = os.path.join(args.out,f"{args.prefix}_munged.txt")

    main(args)
        
    logger.info("END")
