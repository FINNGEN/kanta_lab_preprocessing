import pandas as pd
import argparse,logging,os
from utils import file_exists,log_levels


def main(raw_file):

    args.logging.info(f"Input path:{raw_file}")
    
    return




if __name__=='__main__':
    
    parser=argparse.ArgumentParser(description="KANTA LAB preprocecssing/QC pipeline.")
    parser.add_argument("--raw-data", type=file_exists, help =  "Path to input raw file", required = True)
    parser.add_argument( "-log",  "--log",  default="warning", choices = log_levels, help=(  "Provide logging level. " "Example --log debug', default='warning'"))
    parser.add_argument('-o',"--out_path",type = str, help = "Folder in which to save the results", default = os.getcwd())

    args = parser.parse_args()


    
    # logging level
    level = log_levels[args.log]
    logging.basicConfig(level=level,format="%(levelname)s: %(message)s")
    args.logging = logging


    main(args.raw_data)
