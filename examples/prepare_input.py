import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from ds_utils.deploy.mode import Mode
from ds_utils.sql import SQLAccess, SQLQuery
from datetime import date

cxan_host = 'cxan-dw-bi.chozh4ko8s5t.us-east-1.redshift.amazonaws.com'
cxan_db = 'cxan_dw'
cxan_driver = "redshift"
cxan_port = 5439
CONN_CXAN = 'CXAN_DW'

start_date = '2021-12-18'
end_date = '2022-02-09'
today = str(date.today())
page_names = []

def get_input():
    try:
        df = pd.read_csv("../data/input.csv")
        return df
    except FileNotFoundError:
        query = SQLQuery(path='../ItemAttribution/sql/get_input.sql',
                         clusternm='SEED_PROD')
        access = SQLAccess(Mode(Mode.LOCAL))
        df = access.select_sql(query)
        df.to_csv("../data/input.csv")
        return df

get_input()