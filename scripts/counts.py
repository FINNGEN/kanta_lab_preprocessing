#!/bin/pythyon3
import numpy as np

def return_block(f):
    value = ""
    with open(f) as i:
        line = next(i).strip().split()
        key = line[:2]
        values = {str(key):[line[2]]}
        for line in i:
            line = line.strip().split()
            new = line[:2]
            if new == key:
                values[str(key)].append(line[2])
            else:
                yield key,values[str(key)]
                key = new
                values = {str(key):[line[2]]}

    yield key,values[str(key)]

def is_float(element: any) -> bool:
    #If you expect None to be passed:
    if element is None: 
        return False
    try:
        float(element)
        return True
    except ValueError:
        return False

F='/mnt/disks/data/kanta/test/ABBR_UNIT_COUNT.txt'
it = return_block(F)

def return_results(f):
    it = return_block(f)
    for res in it:
        key,values = res
        float_values = [float(elem) for elem in values if is_float(elem)]
        f = len(float_values)/len(values)
        p =  np.percentile(float_values,np.arange(0, 100, 10))
        yield key[0],key[1],len(values),f,p


res = return_results(F)
out_file =F.replace('.txt','_table.txt') 
with open(f"{out_file}.tmp",'wt') as out:
    out.write('\t'.join(['ABBR','UNIT','TOT_COUNT','FRACTION_VALID','DECILE','VALUE'])+'\n')
    for result in res:
        abbr,unit,n,f,deciles = result
        for i,res in enumerate(deciles):
            p = round(0.1*i,1)
            out_line = list(map(str,[abbr,unit,n,f,p,round(res,2)]))
            out.write('\t'.join(out_line) + '\n')
    

