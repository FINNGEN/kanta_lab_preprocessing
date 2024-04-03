import pandas as pd
import argparse,logging,os
from functools import partial
import multiprocessing as mp
import numpy as np
from utils import file_exists,log_levels,configure_logging,make_sure_path_exists,progressBar,batched
from filters.filter_minimal import filter_minimal 
dir_path = os.path.dirname(os.path.realpath(__file__))



def chunk_reader(raw_file,chunk_size):
    """
    Iterator that spews out chunks and exits early in case of test
    """
    with pd.read_csv(raw_file, chunksize=chunk_size,sep="\t",dtype=str) as reader:
        for i,chunk in enumerate(reader):
            if args.test and i==1:
                break
            else:
                yield chunk


def all_filters(df,args):
    df.pipe(filter_minimal,args)
    return df


def main(args):
    logger.info(f"Input path:{args.raw_data}")
    out_file = os.path.join(args.out,f"{args.prefix}_munged.txt")
   
    # get function with only df as input
    multi_func = partial(all_filters,args=args)
    # read in chunk and apply further split
    logger.info("START")
    for i,chunk in enumerate(chunk_reader(args.raw_data,args.chunk_size)):
        df= all_filters(chunk,args)
        # write header for first chunk along with df and print some info
        if i ==0:
            logger.debug(f"before:{chunk.laboratoriotutkimusnimikeid}")
            logger.debug(f"after:{df.laboratoriotutkimusnimikeid}")
            size = len(df)
            df.to_csv(out_file, mode='w', index=False, header=True,sep="\t")
            logger.info(f"chunksize:{len(df)}")
        else:
            size += len(df)
            df.to_csv(out_file, mode='a', index=False, header=False,sep="\t")
        progressBar(str(size))
    print('\nDone.')
    logger.info("END")
    return


def multi_main(args):
    """
    Multiproc version
    """
    logger.info(f"Input path:{args.raw_data}")
    out_file = os.path.join(args.out,f"{args.prefix}_munged.txt")
    ctx = mp.get_context('spawn')
    pool = ctx.Pool(args.jobs)
    # get function with only df as input
    multi_func = partial(all_filters,args=args)
    # read in chunk and apply further split
    logger.info("START")
    for i,chunks in enumerate(batched(chunk_reader(args.raw_data,int(args.chunk_size/args.jobs)),args.jobs)):
        results = pool.imap(multi_func, chunks)
        df = pd.concat(list(results))
        # write header for first chunk along with df and print some info
        if i ==0:
            logger.debug(f"before:{df.laboratoriotutkimusnimikeid}")
            logger.debug(f"after:{df.laboratoriotutkimusnimikeid}")
            size = len(df)
            df.to_csv(out_file, mode='w', index=False, header=True,sep="\t")
            logger.info(f"chunksize:{len(df)}")
        else:
            size += len(df)
            df.to_csv(out_file, mode='a', index=False, header=False,sep="\t")
        progressBar(str(size))
    print('\nDone.')
    logger.info("END")
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
    
    logger.debug("START")
    if os.path.basename(args.raw_data) == "raw_data_test.txt":
        logger.warning("RUNNING IN TEST MODE")

    # make sure the chunk size is at least the size of the the jobs
    args.chunk_size = max(args.chunk_size,args.jobs)
    if mp:
        multi_main(args)
    else:
        main(args)
        
    logger.debug("END")
