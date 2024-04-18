import sys,os,random,string
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


df = pd.read_csv(raw_path,sep = "\t",dtype = str)

#build new lines randomly
df = pd.concat([df,df.sample(n=N,replace=True)],ignore_index=True)
print(df)

# get random values of lab id:
lab_map=os.path.join(os.path.dirname(path),'data/thl_lab_id_abbrv_map.tsv')
with open(lab_map) as i:
    lab_names=[elem.strip().split()[0] for elem in i.readlines()]

random_idx = np.random.choice(df.index.values, size=int(df.index.size*.9), replace=False)
df.loc[random_idx,'laboratoriotutkimusoid'] = np.random.choice(lab_names,size = random_idx.size,replace=True)
random_idx = np.random.choice(df.index.values, size=int(df.index.size*.9), replace=False)
df.loc[random_idx,'laboratoriotutkimusnimikeid'] = np.random.choice(lab_names,size = random_idx.size,replace=True)


#GENERATE 100 samples IDS
IDS=np.random.randint(100,size=df.index.size)
id_col = ["FAKE" +f'{n:04}' for n in IDS]
df.loc[:,'potilashenkilotunnus'] = id_col

# ADD MALFORMED het_root
print("het root")
random_idx = np.random.choice(df.index.values, size=int(df.index.size/10), replace=False)
col='hetu_root'
df =df.assign(hetu_root="1.2.246.21")
df.loc[random_idx,col] = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))

#UPDATE LAB PROVIDER
lab_map=os.path.join(os.path.dirname(path),'data/thl_sote_map_named.tsv')
with open(lab_map) as i:
    lab_names=[elem.strip().split()[0] for elem in i.readlines()]

random_idx = np.random.choice(df.index.values, size=int(df.index.size*.9), replace=False)
df.loc[random_idx,'palvelutuottaja_organisaatio'] = np.random.choice(lab_names,size = random_idx.size,replace=True)



col = 'tutkimustulosyksikko'
values = [' ','_',',','.','-','(',')','{','}',"\\",'?','!']
random_idx = np.random.choice(df.index.values, size=int(df.index.size*.1), replace=False)
new_data = [elem + random.choice(values) for elem in df.loc[random_idx,col]]
df.loc[random_idx,col] = new_data

values = ["sudhe","lomake","liter","mmol","mol","inrarvo","kpl","arvio","umol","tilo","mg"]
random_idx = np.random.choice(df.index.values, size=int(df.index.size*.8), replace=False)
df.loc[random_idx,col] = np.random.choice(values,size = random_idx.size,replace=True) 


col = 'tuloksenpoikkeavuus'
values = ['A','AA','H','HH','L','N','NEG','<','>','POS','NEG',"OTHER"]
df.loc[:,col] = np.random.choice(values,size = df.index.size,replace=True)


# UPDATE MEASUREMENT STATUS
print("measurement status")
col='tutkimusvastauksentila'
valid_entries =["K",'W','X','I','C','D','F']
values = np.random.choice(valid_entries,size=len(df))
df[col] = values

# randomly replace
# add random Puutttu values
print('random missing')
for col in df.columns:
    random_idx = np.random.choice(df.index.values, size=int(df.index.size/10), replace=False)
    rej_values = ['Puuttuu','""',"TYHJÄ","_","NULL","-1"] 
    random_values = np.random.choice(rej_values,size = random_idx.size, replace=True)
    df.loc[random_idx,col] = random_values

    # randomly add whitespace
    random_idx = np.random.choice(df.index.values, size=int(df.index.size/20), replace=False)
    v1,v2 = np.split(df.loc[random_idx,col].values,2)
    v1 = [" " +elem for elem in map(str,v1)]
    v2 = [elem + " " for elem in map(str,v2)]
    df.loc[random_idx,col] = v1 + v2



df.to_csv(out_path,sep="\t",index=False,compression='gzip')
