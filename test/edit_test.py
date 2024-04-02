import sys,os
import pandas as pd
import numpy as np
path = os.path.dirname(os.path.realpath(__file__))
raw_path = os.path.join(path,"raw_data_test.txt")


N= int(sys.argv[1])
print(N)

out_path = sys.argv[2]
"""
Here i make modifications to the few lines Vincent gave to try to reproduce issues

"""


df = pd.read_csv(raw_path,sep = "\t")

#build new lines randomly
df = pd.concat([df,df.sample(n=N,replace=True)],ignore_index=True)
print(df)
# add random Puutttu values
arvo_col = "tutkimustulosarvo"
other_cols = df.columns.difference([arvo_col])

# randomly replace
for col in other_cols:
    random_idx = np.random.choice(df.index.values, size=int(df.index.size/10), replace=False)
    rej_values = ['Puuttuu','""',"TYHJÄ","_","NULL"] 
    if col == "tutkimustulosarvo":
        rej_values = ['Puuttuu','""',"TYHJÄ","_","NULL"] 
    else:
        rej_values = ['Puuttuu','""',"TYHJÄ","_","NULL","-1"] 
    random_values = np.random.choice(rej_values,size = random_idx.size, replace=True)
    df.loc[random_idx,col] = random_values

df.to_csv(out_path,sep="\t",index=False,compression='gzip')
