import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from ds_utils.deploy.mode import Mode
from ds_utils.sql import SQLAccess, SQLQuery
from datetime import date


def get_input():
    try:
        df = pd.read_csv("../data/input.csv")
        return df
    except FileNotFoundError:
        query = SQLQuery(path='../ItemAttribution/sql/get_input_test_set.sql',
                         clusternm='SEED_PROD',
                         params={'start_date': '2022-03-29'})
        access = SQLAccess(Mode(Mode.LOCAL))
        df = access.select_sql(query)
        df.to_csv("../data/input.csv", index=False)
        return df


def add_new_input():
    df = pd.read_csv("../data/input.csv")
    query = SQLQuery(path='../ItemAttribution/sql/get_input_test_set.sql',
                     clusternm='SEED_PROD',
                     params={'start_date': '2022-03-14'})
    access = SQLAccess(Mode(Mode.LOCAL))
    df_new = access.select_sql(query)
    df = pd.concat([df, df_new], ignore_index=True)
    df.to_csv("../data/input.csv", index=False)
    return df


df_test = get_input()
# df_test = add_new_input()
