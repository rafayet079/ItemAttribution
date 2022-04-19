import pandas as pd
from os.path import exists
import numpy as np
import matplotlib.pyplot as plt



input = pd.read_csv("../data/input.csv")
dates = input['dt'].unique().tolist()
co_cust = input['co_cust_nbr'].unique().tolist()
# co_cust = [str(i).zfill(9) for i in co_cust]
attr = input['attr_grp_nm'].unique().tolist()

got = np.zeros([4])
result = dict()
processed = dict()
for i in range(input.shape[0]):
    date = input.iloc[i]['dt']
    co_cust = input.iloc[i]['co_cust_nbr']
    attr_grp_nm = input.iloc[i]['attr_grp_nm']
    attr = attr_grp_nm.replace("/", "_")
    df = input[(input['dt']==date)&(input['co_cust_nbr']==co_cust)&(input['attr_grp_nm']==attr_grp_nm)]
    opco = str(input.iloc[i].co_cust_nbr).zfill(9)[:3]
    got = result.get(opco, None)
    if got is None:
        got = np.zeros([4])
        result[opco] = got
        processed[opco] = 0
    if df.shape[0] > 0:
        purchased_supc = df['itm_nbr'].tolist()
        path_to_file = "../data/output/" + str(co_cust).zfill(9) + "_" + attr + "_" + date + ".csv"
        file_exists = exists(path_to_file)
        if file_exists:
            output = pd.read_csv(path_to_file)
            predicted_supc = output['supc'].tolist()
            got[0] += (len([j for j in purchased_supc if j in predicted_supc[:10]]) > 0) * 1
            got[1] += (len([j for j in purchased_supc if j in predicted_supc[:20]]) > 0) * 1
            got[2] += (len([j for j in purchased_supc if j in predicted_supc[:30]]) > 0) * 1
            got[3] += (len([j for j in purchased_supc if j in predicted_supc[:40]]) > 0) * 1
            processed[opco] += 1
            result[opco] = got
    if df.shape[0] > 1:
        path_to_file = "../data/output/" + str(co_cust).zfill(9) + "_" + attr + "_" + date + ".csv"
        print(path_to_file)
# print(got/processed)
# for j in range(4):
#     print("Purchased SUPC in first", (j+1)*10, "predicted SUPC	:", np.round(got[j]*100/processed,2),"%")

opco_list = list(result.keys())
opco_list = [(result[i][0]/processed[i], i) for i in opco_list if processed[i] > 75]
opco_list.sort(reverse=True)
opco_list = [i[1] for i in opco_list]

for j in range(4):
    conv_rate = [result[i][j]*100/processed[i] if processed[i] > 0 else 0 for i in opco_list]
    plt.rcParams["figure.figsize"] = (20, 10)
    plt.rcParams.update({'font.size': 12})
    plt.bar(np.arange(len(opco_list)), conv_rate)
    plt.xticks(np.arange(len(opco_list)), opco_list, rotation=90)
    plt.title("Purchased SUPC in first "+str((j+1)*10)+" predicted SUPC")
    plt.xlabel("opco")
    plt.ylabel("Conversion Rate")
    plt.savefig("figs/conv_rate_first_"+str((j+1)*10)+"_recommends_by_opco.png", bbox_inches='tight')
    plt.clf()

plt.bar(np.arange(len(opco_list)), [processed[i] for i in opco_list])
plt.xticks(np.arange(len(opco_list)), opco_list, rotation=90)
plt.xlabel("opco")
plt.ylabel("Count of Processed customers")
plt.savefig("figs/count_of_processed_cust_by_opco.png", bbox_inches='tight')
plt.clf()


