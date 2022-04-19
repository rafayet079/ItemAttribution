import os
from ds_utils.deploy.mode import Mode
from ds_utils.sql import SQLAccess, SQLQuery
import pandas as pd
import matplotlib.pyplot as plt
from os.path import exists
import numpy as np
import datetime
import traceback
import logging
import time

for file in os.listdir("../data/output"):
    print(file)
    df = pd.read_csv("../data/output/"+file)
    supc = [str(i) for i in df['supc'].tolist()[0:20]]
    query = SQLQuery(path='../ItemAttribution/sql/get_lot_for_supc.sql',
                     clusternm='SEED_PROD',
                     params={'supc': tuple(supc)})
    access = SQLAccess(Mode(Mode.LOCAL))
    df_lot = access.select_sql(query)
    # print (df_lot)
    df_lot.use_lot_nm.hist()
    plt.title(file[:-4])
    plt.ylabel("Count")
    plt.xlabel("use lot name")
    plt.savefig("figs/use_lot_nm/"+file[:-4]+".png")
    plt.clf()

