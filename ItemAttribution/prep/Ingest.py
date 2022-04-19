import pandas as pd
from ds_utils.deploy.mode import Mode
from ds_utils.sql import SQLAccess, SQLQuery
import datetime
import os


class Ingest:

    def __init__(self):
        self.op_co = 67

    def get_input(self) -> pd.DataFrame:
        df = pd.read_csv("../data/input.csv")
        return df

    def get_attr_grp_nm(self, opco, today) -> list:
        try:
            df = pd.read_csv("../data/attr_grp_nm/attr_grp_nm_" + opco + "_" + str(today) + ".csv")
        except FileNotFoundError:
            query = SQLQuery(path='../ItemAttribution/sql/get_attr_grp_nm_for_opco.sql',
                             clusternm='SEED_PROD',
                             params={'opco': tuple([opco])})
            access = SQLAccess(Mode(Mode.LOCAL))
            df = access.select_sql(query)
            df.to_csv("../data/attr_grp_nm/attr_grp_nm_" + opco + "_" + str(today) + ".csv")
        return df.attr_grp_nm.tolist()

    ''' 
    returns cuisine type 2 desc for a co_cust    
    '''

    def get_cuisine_old(self, co_cust) -> str:
        co_cust = str(co_cust).zfill(9)
        opco = co_cust[:3]
        if len(co_cust) == 9:
            try:
                df = pd.read_csv("../data/cuisine_cust/" + co_cust + ".csv")
            except FileNotFoundError:
                query = SQLQuery(path='../ItemAttribution/sql/get_cuisine_for_co_cust.sql',
                                 clusternm='SEED_PROD',
                                 params={'co_cust': tuple([co_cust])})
                access = SQLAccess(Mode(Mode.LOCAL))
                df = access.select_sql(query)
                df.to_csv("../data/cuisine_cust/" + co_cust + ".csv")

            if df.shape[0] > 0:
                df.cuisine_lvl_2_desc = df.cuisine_lvl_2_desc.fillna('Not a Restaurant')
                return df.iloc[0].cuisine_lvl_2_desc
            else:
                return 'Not a Restaurant'
        else:
            return 'Not a Restaurant'

    def get_cuisine(self, co_cust) -> str:
        co_cust = int(co_cust)
        co_cust = str(co_cust).zfill(9)
        opco = co_cust[:3]
        if len(co_cust) == 9:
            try:
                df = pd.read_csv("../data/cuisine_opco/" + opco + ".csv")
            except FileNotFoundError:
                query = SQLQuery(path='../ItemAttribution/sql/get_cuisine_for_opco.sql',
                                 clusternm='SEED_PROD',
                                 params={'opco': tuple([opco])})
                access = SQLAccess(Mode(Mode.LOCAL))
                df = access.select_sql(query)
                df.to_csv("../data/cuisine_opco/" + opco + ".csv")
            df = df[df['co_cust_nbr'] == int(co_cust)]
            if df.shape[0] > 0:
                df.cuisine_lvl_2_desc = df.cuisine_lvl_2_desc.fillna('Not a Restaurant')
                return df.iloc[0].cuisine_lvl_2_desc
            else:
                return 'Not a Restaurant'
        else:
            return 'Not a Restaurant'

    '''
    for a given cuisine and op_co find all other co_cust with same cuisine in the same op_co 
    '''

    def get_mkt_lvl(self, opco) -> str:
        try:
            df_mkt = pd.read_csv("../data/org_co_dim.csv")
        except FileNotFoundError:
            query = SQLQuery(path='../ItemAttribution/sql/get_mkt_lvl_by_opco.sql',
                             clusternm='SEED_PROD')
            access = SQLAccess(Mode(Mode.LOCAL))
            df_mkt = access.select_sql(query)
            opco = str(opco).zfill(3)
            df_mkt.to_csv("../data/org_co_dim.csv", index=False)
        df_mkt = df_mkt[df_mkt['co_nbr'] == opco]
        if df_mkt.shape[0] > 0:
            mkt = df_mkt.iloc[0]['mkt_lvl']
        else:
            mkt = 'unknown'
        return mkt

    def get_co_cust_with_cuisine(self, cuisine, mkt_lvl) -> list:
        # opco = str(opco).zfill(3)
        # df = None
        # try:
        #     df = pd.read_csv("../data/cuisine_opco/" + opco + ".csv")
        # except FileNotFoundError:
        #     query = SQLQuery(path='../ItemAttribution/sql/get_cuisine_for_opco.sql',
        #                      clusternm='SEED_PROD',
        #                      params={'opco': tuple([opco])})
        #     access = SQLAccess(Mode(Mode.LOCAL))
        #     df = access.select_sql(query)
        #     df.to_csv("../data/cuisine_opco/" + opco + ".csv")
        # df = df[df['cuisine_lvl_2_desc'] == cuisine]
        # # return df.co_cust_nbr.tolist()
        try:
            df = pd.read_csv("../data/cuisine_opco/" + mkt_lvl + ".csv")
        except FileNotFoundError:
            query = SQLQuery(path='../ItemAttribution/sql/get_cuisine_for_mkt_lvl.sql',
                             clusternm='SEED_PROD',
                             params={'mkt_lvl': tuple([mkt_lvl])})
            access = SQLAccess(Mode(Mode.LOCAL))
            df = access.select_sql(query)
            df.to_csv("../data/cuisine_opco/" + mkt_lvl + ".csv")
        df = df[df['cuisine_lvl_2_desc'] == cuisine]
        return df.co_cust_nbr.tolist()

    def get_purchase_hist(self, mkt_lvl, attr_grp_nm, co_cust):
        query = SQLQuery(path='../ItemAttribution/sql/get_hist_attribute.sql',
                         clusternm='SEED_PROD',
                         params={'mkt_lvl': "\'" + mkt_lvl + "\'",
                                 'attr_grp_nm': "\'" + attr_grp_nm + "\'",
                                 'co_cust': tuple(co_cust)
                                 }
                         )
        access = SQLAccess(Mode(Mode.LOCAL))
        df = access.select_sql(query)
        return df

    def get_purchase_hist_old(self, co_cust, attr_grp_nm, cuisine, mkt_lvl, today):
        dates = [today - datetime.timedelta(i) for i in range(10)]
        co_cust = [str(i).zfill(9) for i in co_cust]
        attr = attr_grp_nm.replace("/", "_")
        index = attr_grp_nm.find("'")
        if index != -1:
            attr_grp_nm = attr_grp_nm[:index] + "'" + attr_grp_nm[index:]
        df = None
        try:
            for i in dates:
                # fname = opco + "_" + attr + "_" + cuisine + "_" + str(i) + ".csv"
                fname = mkt_lvl + "_" + attr + "_" + cuisine + "_" + str(i) + ".csv"
                if fname in os.listdir("../data/purchase_hist/"):
                    df = pd.read_csv("../data/purchase_hist/" + fname)
                    print("Got Purchase Hist from cache ", fname, str(today))
                    break
            if df is None:
                # fname = opco + "_" + attr + "_" + cuisine + "_" + str(today) + ".csv"
                fname = mkt_lvl + "_" + attr + "_" + cuisine + "_" + str(today) + ".csv"
                df = pd.read_csv("../data/purchase_hist/" + fname)

        except FileNotFoundError:
            query = SQLQuery(path='../ItemAttribution/sql/get_purchase_hist.sql',
                             clusternm='SEED_PROD',
                             params={'co_cust': tuple(co_cust),
                                     'attr_grp_nm': "\'" + attr_grp_nm + "\'",
                                     'start_date': "\'" + str(today) + "\'"})
            access = SQLAccess(Mode(Mode.LOCAL))
            df = access.select_sql(query)
            # df.to_csv("../data/purchase_hist/"+opco + "_" + attr + "_" + cuisine + "_" + str(today) + ".csv")
            df.to_csv("../data/purchase_hist/" + mkt_lvl + "_" + attr + "_" + cuisine + "_" + str(today) + ".csv",
                      index=False)
        return df

    def get_itm_attribute(self, itm_skey, attr_grp_nm, cuisine, mkt_lvl, today):
        dates = [today - datetime.timedelta(i) for i in range(10)]
        attr = attr_grp_nm.replace("/", "_")
        index = attr_grp_nm.find("'")
        if index != -1:
            attr_grp_nm = attr_grp_nm[:index] + "'" + attr_grp_nm[index:]
        df = None
        try:
            for i in dates:
                fname = mkt_lvl + "_" + attr + "_" + cuisine + "_" + str(i) + ".csv"
                if fname in os.listdir("../data/itm_attribute/"):
                    df = pd.read_csv("../data/itm_attribute/" + fname)
                    print("Got item attribute from cache ", fname, str(today))
                    break
            if df is None:
                fname = mkt_lvl + "_" + attr + "_" + cuisine + "_" + str(today) + ".csv"
                df = pd.read_csv("../data/itm_attribute/" + fname)
        except FileNotFoundError:
            query = SQLQuery(path='../sql/get_itm_attribute_for_itm_skey.sql',
                             clusternm='SEED_PROD',
                             params={'itm_skey': tuple(itm_skey)}
                             )
            access = SQLAccess(Mode(Mode.LOCAL))
            df = access.select_sql(query)
            df.to_csv("../data/itm_attribute/" + mkt_lvl + "_" + attr + "_" + cuisine + "_" + str(today) + ".csv")
        return df

    def get_items_available(self, opco, attr_grp_nm):
        query = SQLQuery(path='../ItemAttribution/sql/get_available_itms.sql',
                         clusternm='SEED_PROD',
                         params={'opco': tuple([opco]),
                                 'attr_grp_nm': "\'" + attr_grp_nm + "\'"})
        access = SQLAccess(Mode(Mode.LOCAL))
        df_itms = access.select_sql(query)
        df_itms['value'] = 1
        return df_itms

    def get_itm_skey_for_opco(self, opco, attr_grp_nm):
        today = datetime.date.today()
        dates = [today - datetime.timedelta(i) for i in range(10)]
        opco = str(opco).zfill(3)
        attr = attr_grp_nm.replace("/", "_")
        df = None
        try:
            for i in dates:
                if attr + "_" + opco + "_" + str(i) + ".csv" in os.listdir("../data/itm_skey_in_opco/"):
                    df = pd.read_csv(
                        "../data/itm_skey_in_opco/" + attr + "_" + opco + "_" + str(i) + ".csv")
            if df is None:
                df = pd.read_csv(
                    "../data/itm_skey_in_opco/" + attr + "_" + opco + "_" + str(today) + ".csv")

        except FileNotFoundError:
            index = attr_grp_nm.find("'")
            if index != -1:
                attr_grp_nm = attr_grp_nm[:index] + "'" + attr_grp_nm[index:]
            query = SQLQuery(path='../ItemAttribution/sql/get_itm_skey_by_opco_attr_grp_nm.sql',
                             clusternm='SEED_PROD',
                             params={'opco': tuple([opco]),
                                     'attr_grp_nm': "\'" + attr_grp_nm + "\'"})

            access = SQLAccess(Mode(Mode.LOCAL))
            df = access.select_sql(query)
            df.to_csv("../data/itm_skey_in_opco/" + attr + "_" + opco + "_" + str(today) + ".csv")
        return df
