from ds_utils.deploy.mode import Mode
from ds_utils.sql import SQLAccess, SQLQuery
import pandas as pd
import datetime
attr_grp_nm = "FLOUR"
today = datetime.date.today()
query = SQLQuery(path='../ItemAttribution/sql/get_hist_attribute.sql',
                             clusternm='SEED_PROD',
                             params={'attr_grp_nm': "\'" + attr_grp_nm + "\'",
                                     'start_date': "\'" + str(today) + "\'"})
access = SQLAccess(Mode(Mode.LOCAL))
df = access.select_sql(query)

# df_itms_in_opco.pivot_table(index='supc', columns=['osd_decided_val'], values='val', fill_value=0)
df_sum = df.pivot_table(index='itm_skey', columns=['osd_decided_val'], values='sum', fill_value=0).sum()
df_dist = df_sum/df_sum.sum()

opco = 67
query = SQLQuery(path='../ItemAttribution/sql/get_itm_skey_by_opco_attr_grp_nm.sql',
                 clusternm='SEED_PROD',
                 params={'opco': tuple([opco]),
                         'attr_grp_nm': "\'" + attr_grp_nm + "\'"})

access = SQLAccess(Mode(Mode.LOCAL))
df_itms = access.select_sql(query)
df_itms['sum'] = 1
df_one_hot = df_itms.pivot_table(index='itm_skey', columns=['osd_decided_val'], values='sum', fill_value=0)
common_attributes = set(df_one_hot.columns).intersection(set(df_dist.index))
sorted_supc = (df_one_hot[common_attributes]*df_dist[common_attributes]).sum(axis=1).sort_values(ascending=False).index[:10]