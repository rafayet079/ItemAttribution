import pandas as pd
from os.path import exists
import numpy as np
import traceback
import logging
import datetime

input = pd.read_csv("../data/input.csv")
dates = input['dt'].unique().tolist()
co_cust = input['co_cust_nbr'].unique().tolist()
# co_cust = [str(i).zfill(9) for i in co_cust]
attr = input['attr_grp_nm'].unique().tolist()

processed = 0
# try:
#     processed = processed/processed
# except Exception as e:
#     logging.error(traceback.format_exc())
got = np.zeros([4])
for i in range(input.shape[0]):
    date = input.iloc[i]['dt']
    co_cust = input.iloc[i]['co_cust_nbr']
    attr_grp_nm = input.iloc[i]['attr_grp_nm']
    attr = attr_grp_nm.replace("/", "_")
    df = input[(input['dt'] == date) & (input['co_cust_nbr'] == co_cust) & (input['attr_grp_nm'] == attr_grp_nm)]
    if df.shape[0] > 0:
        purchased_supc = df['itm_nbr'].tolist()
        path_to_file = "../data/output/" + str(co_cust).zfill(9) + "_" + attr + "_" + date + ".csv"
        file_exists = exists(path_to_file)
        if file_exists:
            output = pd.read_csv(path_to_file)
            predicted_supc = output['supc'].tolist()
            # print(len(predicted_supc), path_to_file)
            # for j in range(1, 5):
            #     got[j-1] += (len([j for j in purchased_supc if j in predicted_supc[:(j*10)]]) > 0) * 1
            #     print(got,j, (len([j for j in purchased_supc if j in predicted_supc[:(j*10)]]) > 0) * 1, len(predicted_supc[:(j*10)]))
            got[0] += (len([j for j in purchased_supc if j in predicted_supc[:10]]) > 0) * 1
            got[1] += (len([j for j in purchased_supc if j in predicted_supc[:20]]) > 0) * 1
            got[2] += (len([j for j in purchased_supc if j in predicted_supc[:30]]) > 0) * 1
            got[3] += (len([j for j in purchased_supc if j in predicted_supc[:40]]) > 0) * 1
            processed += 1
    if df.shape[0] > 1:
        path_to_file = "../data/output/" + str(co_cust).zfill(9) + "_" + attr + "_" + date + ".csv"
        print(path_to_file)
# print(got/processed)
for j in range(4):
    print("Purchased SUPC in first", (j + 1) * 10, "predicted SUPC	:", np.round(got[j] * 100 / processed, 2), "%")

# for i in range(input.shape[0]):
#     if '/' in input.iloc[i]['dt']:
#         dt = [int(i) for i in input.iloc[i]['dt'].split('/')]
#         dt = datetime.date(dt[2], dt[0], dt[1])
#         input.loc[i, 'dt'] = str(dt)

# for i in range(input.shape[0]):
#     input.loc[i, 'co_cust_nbr'] = int(input.iloc[i]['co_cust_nbr'])
