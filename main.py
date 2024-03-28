import pandas as pd
import argparse,logging,os
from utils import file_exists,log_levels,configure_logging,make_sure_path_exists
from filters import filter_minimal
dir_path = os.path.dirname(os.path.realpath(__file__))



def chunk_reader(raw_file,chunk_size):
    with pd.read_csv(raw_file, chunksize=chunk_size,sep="\t") as reader:
        for chunk in reader:
            yield chunk


def main(args):

    logger.info(f"Input path:{args.raw_data}")
    for df in chunk_reader(args.raw_data,args.chunk_size):
        logger.debug(df)
        df.pipe(filter_minimal.remove_spaces)
        logger.debug(df)
    return





if __name__=='__main__':
    
    parser=argparse.ArgumentParser(description="KANTA LAB preprocecssing/QC pipeline.")
    parser.add_argument("--raw-data", type=file_exists, help =  "Path to input raw file", default = os.path.join(dir_path,"test","raw_data_test.txt"))
    parser.add_argument("--log",  default="warning", choices = log_levels, help=(  "Provide logging level. Example --log debug', default='warning'"))
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

    main(args)
    logger.debug("END")
