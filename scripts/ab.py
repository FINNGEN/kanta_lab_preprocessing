#!/bin/python3
import sys,os,argparse,gzip,shutil,shlex
from pathlib import Path
from collections import defaultdict
import subprocess
import numpy as np
from utils import make_sure_path_exists,count_lines,progressBar
from operator import itemgetter


def split_input(munged_file,omop_dir):
    """
    Go through the munged file and split the input based on omop ID if there is abnormality and a value
    """
    print(omop_dir)
    print(munged_file)
    #remove all previous files
    shutil.rmtree(omop_dir,ignore_errors=True)
    make_sure_path_exists(omop_dir)
    with gzip.open(munged_file,'rt') as i:
        header=next(i).strip().split() # get col indices
        cols = [header.index(elem) for elem in [ 'harmonization_omop::OMOP_ID','harmonization_omop::MEASUREMENT_VALUE','RESULT_ABNORMALITY']]
        print(cols)
        for line in i:
            line = line.strip().split()
            omop,value,abnorm = [line[elem] for elem in cols]
            if int(omop) > 0 and value != "NA":
                with open(os.path.join(omop_dir,f"{omop}.txt"),'at') as o:
                    o.write(value + '\t' + abnorm + '\n')
        return


def sort_omop(abnorm_file,sorted_dir,ID):
    out_file_low = os.path.join(sorted_dir,f"{ID}_low.txt")
    if not os.path.isfile(out_file_low):
        with open(out_file_low,'wt') as o:subprocess.run(shlex.split(f"sort -gk1 {abnorm_file}"),stdout=o)
    # HIGH
    out_file_high = os.path.join(sorted_dir,f"{ID}_high.txt")
    if not os.path.isfile(out_file_high):
        with open(out_file_high,'wt') as o: subprocess.run(shlex.split(f"sort -rgk1 {abnorm_file}"),stdout=o)

    return out_file_low,out_file_high
    

def return_bound(sorted_file,t_hold,n_lines,numerator_keys,denominator_keys):
    # ok now there are NA values. gotta take care of that.
    # need to keep track of the "age" of the candidate result as i also go through NA values
    with open(sorted_file) as i:
        counts = defaultdict(int)
        # age is a counter that tells me how many valid entries i've met without update
        res,is_valid="NA",""
        for j in range(n_lines):
            value,status = next(i).strip().split()
            counts[status] +=1
            num = np.sum([counts[elem] for elem in numerator_keys])
            den = np.sum([counts[elem] for elem in denominator_keys]) +0.0001
            if status != "NA": 
                if den !=0 :
                    if num/den > t_hold:
                        res = value
                        is_valid = "*"
                    else:
                        is_valid = ""
    return str(res) + is_valid

def count_abnorm(f):
    with open(f) as i:
        counts = defaultdict(int)
        for line in i:
            _,status = line.strip().split()
            counts[status] +=1
    return {k: v for k, v in sorted(counts.items(), key=lambda item: item[1],reverse=True)}
         

def abnormality(out_file,omop_dir,t_holds,max_walk,min_count,test):

    paths =[entry.path for entry in os.scandir(omop_dir) if entry.is_file()]
    sorted_dir = os.path.join(omop_dir,'sorted')
    make_sure_path_exists(sorted_dir)
    results = []
    IDS = [(Path(f).stem,f) for f in paths]
    if args.test:
        IDS = [elem for elem in IDS if elem[0] in ['3008486','3009201','3027238','3032333','3023199','3020460']]
    for i,elem in enumerate(IDS):
        ID,omop_file = elem
        # skip if not enough lines
        count = count_lines(omop_file)
        if count < min_count: continue
        lines = int(count*max_walk)
        # srot files
        out_file_low,out_file_high = sort_omop(omop_file,sorted_dir,ID)
        try:
            num_keys = ['A','L','LL']
            den_keys = num_keys  + ['N','H','HH']
            low_estimates = [return_bound(out_file_low,t_hold,lines,num_keys,den_keys) for t_hold in t_holds]
        except:
            low_estimates = ["NA" for elem in t_holds]
            print(f"problems with {ID} low")
        try:
            num_keys = ['A','H','HH']
            den_keys = num_keys  + ['N']
            high_estimates = [return_bound(out_file_high,t_hold,lines,num_keys,den_keys) for t_hold in t_holds]
        except:
            high_estimates = ["NA" for elem in t_holds]
            print(f"problems with {ID} high")
        counts = count_abnorm(out_file_low)
        if args.test:
            print(ID,count,low_estimates,high_estimates)
            print(out_file_low)
            print(out_file_high)
        else:
            progressBar(i,len(IDS))

        results.append([ID] +   [x for z in zip(low_estimates,high_estimates) for x in z] + [count,str(dict(counts))] )
            
    with open(out_file,'wt') as o:
        header = ['ID']
        for t_hold in t_holds:header += [f"LOWER_{t_hold}",f"UPPER_{t_hold}"]
        header += ['ENTRIES','COUNTS']
        o.write('\t'.join(header) + '\n')
        for res in sorted(results, key=itemgetter(-2),reverse=True):
            o.write('\t'.join(map(str,res)) + '\n')
          
    print('\nDone')
    return



def main(args):
    omop_dir = os.path.join(args.out,'omop_files/')
    # create all omop files if needed
    if args.split: split_input(args.kanta_file,omop_dir)
    else:
        out_file = os.path.join(args.out,f'abnormality_estimation.txt')
        abnormality(out_file,omop_dir,args.thresholds,args.max_walk,args.min_count,args.test)
    return

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--kanta_file',default = '/home/pete/fg-3/kanta/munged/kanta_2024_08_09_munged.txt.gz')
    parser.add_argument('--out',default ="/mnt/disks/data/kanta/abnorm/")
    parser.add_argument('--min-count',default =1000,type=int)
    parser.add_argument('--max-walk',default =.5,type = float)
    parser.add_argument('--thresholds',default = [0.95],nargs='*',type=float)
    parser.add_argument("--split", action='store_true', help="Splits the input file (needs to be run only once)")
    parser.add_argument("--test", action='store_true', help="Test run")

    args = parser.parse_args()
    make_sure_path_exists(args.out)
    assert 0 < args.max_walk <=.5
    print(args.thresholds)
    main(args)
    
