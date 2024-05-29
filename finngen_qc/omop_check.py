import sys,os,requests,argparse
import pandas as pd
from functools import partial
import multiprocessing as mp
import numpy as np
from utils import file_exists,log_levels,configure_logging,make_sure_path_exists,progressBar,batched,mapcount,read_thl_map,estimate_lines,write_chunk
dir_path = os.path.dirname(os.path.realpath(__file__))
OMOP_URL="https://docs.google.com/spreadsheets/d/1Rw8TnYSN2n5JUz5QCMX1k-ZoAQM5XEd-zJuUbhRiMuk/export?format=csv"


def chunk_reader(raw_file,chunk_size):
    """
    Iterator that spews out chunks and exits early in case of test
    """
    with pd.read_csv(raw_file, chunksize=chunk_size,sep='\t',dtype=str) as reader:
        for i,chunk in enumerate(reader):
            if args.test and i==1:
                break
            yield chunk

def get_omop_mapping(args):

    df =  pd.read_csv(args.omop_file,sep = '\t',dtype=str,on_bad_lines='warn').fillna("0")

    # CHECK THAT THERE ARE NO DUPLICATE MAPPINGS
    args.mapping_columns = [col for col in df.columns if "OMOP" not in col]
    print(args.mapping_columns)
    if not len(df[df.duplicated(args.mapping_columns)]) == 0:
        print('warning:duplicate columns')

    df = df.drop_duplicates(subset = args.mapping_columns)
    
    return  df


def omop_map(chunk,args):
    """
    Returns the data that is *not* mapped
    """
    merged=pd.merge(chunk,args.omop_df,on=args.mapping_columns,how='left').fillna("NA")
    return merged

def result_iterator(args):
    """
    Regular result iterator that applies the filter to each args.chunk_size sized chunk
    """
    print("Running in single mode")
    for i,chunk in enumerate(chunk_reader(args.raw_data,args.chunk_size)):
        df= omop_map(chunk,args)
        yield i,df,len(chunk)

def result_iterator_multi(args):
    """
    Multiproc result iterator. It reads in the raw file in args.mp chunks of args.chunk_size each
    """
    print(f"Running in multiproc mode using {args.mp} cpus")
    ctx = mp.get_context('spawn')
    pool = ctx.Pool(args.mp)
    # get function with only df as input
    multi_func = partial(omop_map,args=args)
    # read in chunk and apply further split
    chunk_size = int(args.chunk_size/args.mp)
    for i,chunks in enumerate(batched(chunk_reader(args.raw_data,chunk_size),args.mp)):
        results = pool.imap(multi_func, chunks)
        df = pd.concat(list(results))
        size = np.sum([len(elem) for elem in chunks])
        yield i,df,size

def main(args):
    args.omop_df = get_omop_mapping(args)
    note,tot_lines = estimate_lines(args.raw_data)
    print(f"{tot_lines} input lines {note}")
    res_it = result_iterator_multi if args.mp else result_iterator
    size,final = 0,0
    # i is index,df the filtered chunk and tmp_size the size of the original
    for i,df,tmp_size in res_it(args):
        omop_mask = df.OMOP_ID =="NA"
        out_cols = ['FINREGISTRYID','TEST_DATE_TIME','TEST_ID','TEST_ID_SOURCE','TEST_NAME_ABBREVIATION','MEASUREMENT_UNIT','OMOP_ID']
        write_chunk(df[~omop_mask],i,args.success_file,out_cols)
        write_chunk(df[omop_mask],i,args.failed_file,out_cols)
        size += tmp_size
        progressBar(size,tot_lines)
        
    print('\nDone')
        
if __name__=='__main__':
    
    parser=argparse.ArgumentParser(description="KANTA LAB preprocecssing/QC pipeline. OMOP check.")
    parser.add_argument("--raw-data", help =  "Output of munging", required=True,type=file_exists)
    parser.add_argument("--omop-file", help =  "Omop mapping (default in data)",  default = os.path.join(os.path.join(dir_path),'data/mapping_abbreviation_and_unit.tsv') ,type=file_exists)
    parser.add_argument('-o',"--out",type = str, help = "Folder in which to save the results, default is same as raw-data")
    parser.add_argument("--test",action='store_true',help="Reads first 1k lines only")
    parser.add_argument("--chunk-size",type=int,help="Number of rows to be processed by each chunk",default = 1000)
    parser.add_argument("--prefix",type=str,help = "Prefix of the out files (default = root of raw file)")
    parser.add_argument("--mp",default =0,const=os.cpu_count(),nargs='?',type = int, help ="Flag for multiproc. Default is 0 (no multiproc). If passed it defaults to cpu count, but one can also specify the number of cpus to use: e.g. --mp or --mp 4")


    args = parser.parse_args()
    if not args.out:
        args.out = os.path.dirname(os.path.realpath(args.raw_data))
    if not args.prefix:
        args.prefix = os.path.splitext(os.path.basename(args.raw_data))[0]
    args.chunk_size = max(args.chunk_size,args.mp)
    args.failed_file = os.path.join(args.out,args.prefix +"_omop_failed.txt")
    args.success_file = os.path.join(args.out,args.prefix +"_omop_success.txt")
    
    main(args)
