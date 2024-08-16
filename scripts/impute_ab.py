import numpy as np
data_path = '/mnt/disks/data/kanta/abnorm/abnormality_estimation.txt'

with open(data_path) as i,open(data_path.replace(".txt",'.table.tsv'),'wt') as o:
    header = next(i).strip().split()
    print(header)
    o.write('\t'.join(["ID",'LOW_LIMIT','HIGH_LIMIT','LOW_PROBLEM','HIGH_PROBLEM']) + '\n')
    for line in i:
        data = line.strip().split('\t')
        ID,*results,entries,_ = data
        # for these IDs take .99 anyways
        if int(entries) > 100000:
            low_col  = data[header.index('LOWER_0.99')]
            high_col = data[header.index('UPPER_0.99')]
        # for these take .95 as a starter
        else:
            low_col  = data[header.index('LOWER_0.95')]
            high_col = data[header.index('UPPER_0.95')]
            # if the value is problematic try .99
            if '*' in low_col:
                new_low = data[header.index('LOWER_0.99')]
                low_col = new_low if '*' not in new_low else low_col
            if '*' in high_col:
                new_high = data[header.index('UPPER_0.99')]
                high_col = new_high if '*' not in new_high else high_col
        # check if problematic
        low_problem  = 1 if '*' in low_col else 0
        high_problem = 1 if '*' in high_col else 0
        # map NAs to +- inf
        low_res  =  -np.inf if low_col=="NA" else low_col
        high_res  =  np.inf if high_col=="NA" else high_col
        # write results
        o.write('\t'.join(map(str,[ID,low_res,high_res,low_problem,high_problem])) +'\n')


    
