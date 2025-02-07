import pandas as pd
import argparse,logging,os
from functools import partial
import multiprocessing as mp
import numpy as np
from datetime import datetime
from utils import file_exists,log_levels,configure_logging,make_sure_path_exists,progressBar,batched,mapcount,read_map,estimate_lines,write_chunk,init_log_files,init_unit_table
from magic_config import config
from datetime import datetime
from filters.impute import impute_all
dir_path = os.path.dirname(os.path.realpath(__file__))


def chunk_reader(raw_file,chunk_size,config,separator):
    """
    Iterator that spews out chunks and exits early in case of test
    """
    logger.debug(args.config['cols'])
    
    # TODO(Vincent 2024-05-03)
    # Set engine='pyarrow' to enable faster parsing on multicore machines.
    # It is currently not feature complete and is still experimental.
    # For example, it currently doesn't support the 'chunksize' option.
    #
    # More info:
    # https://pandas.pydata.org/pandas-docs/stable/whatsnew/v1.4.0.html#multi-threaded-csv-reading-with-a-new-csv-engine-based-on-pyarrow
    with pd.read_csv(raw_file, chunksize=chunk_size,sep=separator,dtype=str,usecols = args.config['cols'],engine='python',on_bad_lines='warn') as reader:
        for i,chunk in enumerate(reader):
            if args.test and i==1:
                break
            yield chunk

def all_filters(df,args):
    """
    Combines all functions/filters from the filters folder
    """
    df = (
        df
        .pipe(impute_all,args)
    )
    return df


def result_iterator(args):
    """
    Regular result iterator that applies the filter to each args.chunk_size sized chunk
    """
    logger.info("Running in single mode")
    for i,chunk in enumerate(chunk_reader(args.raw_data,args.chunk_size,args.config,args.sep)):
        df= all_filters(chunk,args)
        yield i,df,len(chunk)

def result_iterator_multi(args):
    """
    Multiproc result iterator. It reads in the raw file in args.mp chunks of args.chunk_size each
    """
    logger.info(f"Running in multiproc mode using {args.mp} cpus")
    ctx = mp.get_context('spawn')
    pool = ctx.Pool(args.mp)
    # get function with only df as input
    multi_func = partial(all_filters,args=args)
    # read in chunk and apply further split
    chunk_size = int(args.chunk_size/args.mp)
    for i,chunks in enumerate(batched(chunk_reader(args.raw_data,chunk_size,args.config,args.sep),args.mp)):
        results = pool.imap(multi_func, chunks)
        df = pd.concat(list(results))
        size = np.sum([len(elem) for elem in chunks])
        yield i,df,size


def setup_pandas():
    """
    Enable some pandas options for future-proofness and better performance.
    """

    # Strings backed by PyArrow instead of numpy will become a default in pandas 3.0.
    # PyArrow strings improve performances.
    #
    # More info about this change:
    # https://pandas.pydata.org/pandas-docs/stable/whatsnew/v2.2.0.html#dedicated-string-data-type-backed-by-arrow-by-default
    #
    # TODO(Vincent 2024-05-03) Remove explicitely setting this option once we have pandas 3.0 as a requirement.
    pd.options.future.infer_string = True

    # Silent downcasting will be removed in pandas 3.0
    # TODO(Vincent 2024-05-03) Remove explicitely setting this option once we have pandas 3.0 as a requirement.
    pd.options.future.no_silent_downcasting = True

    # Copy-on-write (CoW) will be the default in pandas 3.0, set it now to be
    # future-proof.
    # It's easy to make mistake by updating a dataframe that is based on
    # another one. CoW prevents these mistakes.
    # This option enables raising an error if using soon-to-be-deprecated chained assignments.
    #
    # More info about the introduction of the setting:
    # https://pandas.pydata.org/pandas-docs/stable/whatsnew/v2.2.0.html#copy-on-write
    #
    # More info about pandas CoW in general:
    # https://pandas.pydata.org/pandas-docs/stable/user_guide/copy_on_write.html
    #
    # TODO(Vincent 2024-05-03) Remove explicitely setting this option once we have pandas 3.0 as a requirement.
    pd.options.mode.copy_on_write = True


def main(args):
    """
    Main functions that calls the iterators based on regular/multiproc version
    """
    res_it = result_iterator_multi if args.mp else result_iterator
    size = 0
    start_time = datetime.now()
    if args.lines:
        note,lines = 'exact',args.lines
    else:
        note,lines = estimate_lines(args.raw_data)
    logger.info(f"Input path:{args.raw_data}")
    logger.info(f"{lines} input lines {note}")

    output_lines,err_lines = 0,0
    for i,df,tmp_size in res_it(args):
        write_chunk(df,i,args.out_file,args.config['out_cols'],logger=logger)
        size += tmp_size #size of input df
        output_lines += len(df) # size of output df
        diff_err = mapcount(args.err_file) -1 - err_lines # size of err df
        # dump if df sizes don't add up
        if size - output_lines - diff_err != 0:
            err_dump = os.path.join(args.out,f"{args.prefix}_duplicates_{i}.txt.gz")
            logger.critical(f"chunk {i}:lines don't add up")
            write_chunk(df,0,err_dump,args.config['out_cols'],logger=logger)
            
        progressBar(size,lines)


    print('\nDone.')

    # Read sizes of out files and make sure it adds up
    logger.info(f"{size} lines processed")
    logger.info(f"{output_lines} final entries")
    logger.info(f"{mapcount(args.err_file) -1} err entries")
    assert size == output_lines + mapcount(args.err_file) -1
    logger.info('Duration: {}'.format(datetime.now() - start_time))
    
    return

    
if __name__=='__main__':
    
    parser=argparse.ArgumentParser(description="Kanta Lab preprocessing pipeline: raw data ⇒ clean data.")
    parser.add_argument("--raw-data", type=file_exists, help="Path to input raw file. File should be tsv.", required=True)
    parser.add_argument("--log", default="warning", choices=log_levels, help="Provide logging level. Example '--log debug', default = 'warning'")
    parser.add_argument("--test", action='store_true', help="Reads first chunk only")
    parser.add_argument("--gz", action='store_true', help="Ouputs to gz")
    parser.add_argument("--mp", default=0, const=os.cpu_count(), nargs='?', type=int, help="Flag for multiproc. Default is '0' (no multiproc). If passed it defaults to cpu count, but one can also specify the number of cpus to use: e.g. '--mp' or '--mp 4'.")
    parser.add_argument('-o', "--out", type=str, help="Folder in which to save the results (default = current working directory)", default=os.getcwd())
    parser.add_argument("--prefix", type=str, default=f"kanta_{datetime.today().strftime('%Y_%m_%d')}", help="Prefix of the out files (default = 'kanta_YYYY_MM_DD')")
    parser.add_argument("--sep", type=str, default="\\t", help="Separator (default = tab)")
    parser.add_argument("--chunk-size", type=int, help="Number of rows to be processed by each chunk (default = '1000*n_cpus').", default=10000*os.cpu_count())
    parser.add_argument("--lines", type=int, help="Number of lines in input file (calculated/estimated otherwise).")
    args = parser.parse_args()
    
    make_sure_path_exists(args.out)
    # setup logging
    logger = logging.getLogger(__name__)
    log_file = os.path.join(args.out,f"{args.prefix}_log.txt")
    configure_logging(logger,log_levels[args.log],log_file)
    logger.info("START")

    # setup config
    args.config = config
    args.config['cols']  = config['cols']
    args.config['out_cols']  = config['cols'] + config['added_cols']
    logger.debug(args.config['out_cols'])
    init_log_files(args)

    #init stuff
    args.omop_unit_table = init_unit_table(args)
    
    if os.path.basename(args.raw_data) == "raw_data_test.txt":
        logger.warning("RUNNING IN TEST MODE")

    # make sure the chunk size is at least the size of the the jobs
    args.chunk_size = max(args.chunk_size,args.mp)
    args.out_file = os.path.join(args.out,f"{args.prefix}_analysis.txt")  
    if args.gz: args.out_file += ".gz"

    # Setup pandas
    setup_pandas()

    main(args)    
    logger.info("END")
