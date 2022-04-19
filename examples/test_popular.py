import os
from ds_utils.deploy.mode import Mode
from ds_utils.sql import SQLAccess, SQLQuery
import pandas as pd
from os.path import exists
import numpy as np
import datetime
import traceback
import logging
import time

input = pd.read_csv("../data/input.csv")

# for i in range(input.shape[0]):
got = 0
processed = 0
for i in range(input.shape[0]):
    try:
        print("processing ", i, " of ", input.shape[0])
        df = input.iloc[i]
        dt = [int(i) for i in df['dt'].split('-')]
        dt = datetime.date(dt[0], dt[1], dt[2])
        start_date = dt - datetime.timedelta(45)
        end_date = dt - datetime.timedelta(15)
        opco = df['co_nbr']
        co_cust = df['co_cust_nbr']
        co_cust = str(int(co_cust)).zfill(9)
        attr_grp_nm = df['attr_grp_nm']
        attr = attr_grp_nm.replace("/", "_")
        index = attr_grp_nm.find("'")
        if index != -1:
            attr_grp_nm = attr_grp_nm[:index] + "'" + attr_grp_nm[index:]
        opco = str(opco).zfill(3)
        if opco+'.csv' in os.listdir("../data/cuisine_opco/"):
            df_cuisin = pd.read_csv("../data/cuisine_opco/"+opco+".csv")
            cuisin = df_cuisin[df_cuisin['co_cust_nbr']==int(co_cust)]
            if cuisin.shape[0] > 0:
                cuisin = cuisin.fillna('Not A Restaurant')
                cuisin = cuisin.iloc[0].cuisine_lvl_2_desc
            else:
                cuisin = 'Not A Restaurant'
        if cuisin != 'Not A Restaurant':
            df_popular = None
            dates = [end_date - datetime.timedelta(i) for i in range(30)]
            try:
                for date in dates:
                    fname = opco + "_" + attr + "_" + str(date) + ".csv"
                    if fname in os.listdir("../data/popular_supc/"):
                        df_popular = pd.read_csv("../data/popular_supc/"+fname)
                        print("Got Purchase Hist from cache ", fname, str(date))
                        break
                if df_popular is None:
                    fname = opco + "_" + attr + "_" + str(end_date) + ".csv"
                    df_popular = pd.read_csv("../data/popular_supc/"+fname)

            except FileNotFoundError:
                query = SQLQuery(path='../ItemAttribution/sql/get_popular_by_attr_dt_opco.sql',
                                 clusternm='SEED_PROD',
                                 params={'opco': tuple([opco]),
                                         'attr_grp_nm': "\'" + attr_grp_nm + "\'",
                                         'start_date': "\'" + str(start_date) + "\'",
                                         'end_date': "\'" + str(end_date) + "\'"})
                access = SQLAccess(Mode(Mode.LOCAL))
                df_popular = access.select_sql(query)
                df_popular.to_csv("../data/popular_supc/" + opco + "_" + attr + "_" + str(end_date) + ".csv")
            popular_supc = [int(k) for k in df_popular['itm_nbr'].tolist()]
            if int(df['itm_nbr']) in popular_supc[:10]:
                got += 1
            processed += 1
    except Exception as e:
        logging.error(traceback.format_exc())
        time.sleep(10)
        i = i - 1