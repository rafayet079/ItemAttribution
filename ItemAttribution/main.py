from Defaults import Defaults
from prep.Ingest import Ingest
from model.purchase_dist import Purchase_dist
from model.model import Model
from post.Write import Write
import datetime
import traceback
import logging
import time
start = time.time()

opco = Defaults.op_co[0]
attr_grp_nm = Defaults.attr_grp_nm_list[0]
cuisine = Defaults.cuisins[0]

ingest = Ingest()
model = Model()
write = Write(datetime.date.today())

mkt_lvl = ingest.get_mkt_lvl(int(opco))
co_cust_cuisine = ingest.get_co_cust_with_cuisine(cuisine, mkt_lvl)
df_purchase = ingest.get_purchase_hist(mkt_lvl, attr_grp_nm, co_cust_cuisine)
df_items_available = ingest.get_items_available(opco, attr_grp_nm)
if df_purchase.shape[0] > Defaults.min_purchase_history:
    attribute_dist = model.calculate_distribution(df_purchase)
    prioritized_list = model.get_similarity_and_sort(attribute_dist, df_items_available)
    write.write_to_file_formatted(prioritized_list, opco, attr_grp_nm, cuisine)
    write.write_list_to_separate_file(prioritized_list, opco, attr_grp_nm)
else:
    print("Not enough history Opco =", opco, " Cuisine = ", cuisine, " attr_grp_nm = ", attr_grp_nm)

end = time.time()
print(end - start)
#
# for opco in Defaults.op_co:
#     mkt_lvl = ingest.get_mkt_lvl(int(opco))
#     for cuisine in Defaults.cuisins:
#         co_cust_cuisine = ingest.get_co_cust_with_cuisine(cuisine, mkt_lvl)
#         attr_grp_nm_list = ingest.get_attr_grp_nm(opco, datetime.date.today()) #add start end date, add default var for time length
#         for attr_grp_nm in attr_grp_nm_list:
#             print("Opco =", opco, " Cuisine = ", cuisine, " attr_grp_nm = ", attr_grp_nm)
#             df_itms_in_opco = ingest.get_itm_skey_for_opco(opco, attr_grp_nm)
#             df_purchase = ingest.get_purchase_hist(co_cust_cuisine, attr_grp_nm, cuisine, mkt_lvl, datetime.date.today())
#             if df_purchase.shape[0] > 10:
#                 df_itm_attribute = ingest.get_itm_attribute(df_purchase.itm_skey.unique().tolist(),
#                                                             attr_grp_nm, cuisine, mkt_lvl, datetime.date.today())
#                 df_itm_attribute['catman_attr_prr_nbr'] = df_itm_attribute['catman_attr_prr_nbr'].astype(str)
#                 purchase_dist = Purchase_dist(df_purchase, df_itm_attribute, attr_grp_nm, opco, cuisine, mkt_lvl, datetime.date.today())
#                 dist = purchase_dist.calculate_purchse_dist()
#                 supc_list = purchase_dist.get_similarity_and_sort(df_itms_in_opco)
#                 write.write_to_file_formatted(supc_list, opco, attr_grp_nm, cuisine)
#                 write.write_list_to_separate_file(supc_list, opco, attr_grp_nm)
#             else:
#                 print("Not enough history Opco =", opco, " Cuisine = ", cuisine, " attr_grp_nm = ", attr_grp_nm)
#
#         # print( len(attr_grp_nm_list))
#         # for attr_grp_nm in attr_grp_nm_list:
#         #     print()


#
# ingest = Ingest()
# input = ingest.get_input()
# print("input size =", input.shape[0])
# for i in range(3410, input.shape[0]):
#     print("processing ", i, " of ", input.shape[0])
#     try:
#         cuisine = ingest.get_cuisine(input.iloc[i].co_cust_nbr)
#         mkt_lvl = ingest.get_mkt_lvl(input.iloc[i].co_nbr)
#         cuisine = cuisine.replace('/', '_')
#         print("co_cust: ", input.iloc[i].co_cust_nbr, cuisine, input.iloc[i].attr_grp_nm, input.iloc[i]['dt'], mkt_lvl)
#         if cuisine != 'Not a Restaurant' and mkt_lvl != 'unknown':
#             co_cust_cuisine = ingest.get_co_cust_with_cuisine(cuisine, mkt_lvl, input.iloc[i].co_nbr)
#             if len(co_cust_cuisine) > 0:
#                 dt = [int(i) for i in input.iloc[i]['dt'].split('-')]
#                 dt = datetime.date(dt[0], dt[1], dt[2]) - datetime.timedelta(1)
#                 df_purchase = ingest.get_purchase_hist(co_cust_cuisine, input.iloc[i].attr_grp_nm, input.iloc[i].co_nbr, cuisine, mkt_lvl, dt)
#                 if df_purchase.shape[0] > 10:
#                     df_itm_attribute = ingest.get_itm_attribute(df_purchase.itm_skey.unique().tolist(), input.iloc[i].attr_grp_nm, input.iloc[i].co_nbr, cuisine, mkt_lvl, dt)
#                     df_itm_attribute['catman_attr_prr_nbr'] = df_itm_attribute['catman_attr_prr_nbr'].astype(str)
#                     # print(df_itm_attribute.dtypes)
#                     purchase_dist = Purchase_dist(df_purchase, df_itm_attribute, input.iloc[i].attr_grp_nm, input.iloc[i].co_nbr, cuisine, mkt_lvl, dt)
#                     dist = purchase_dist.calculate_purchse_dist()
#                     df_itms_in_opco = ingest.get_itm_skey_for_opco(input.iloc[i].co_nbr, input.iloc[i].attr_grp_nm)
#                     supc_list = purchase_dist.get_similarity_and_sort(df_itms_in_opco)
#                     write = Write(input.iloc[i]['dt'])
#                     write.write_list_to_file(supc_list, input.iloc[i].co_cust_nbr, input.iloc[i].attr_grp_nm)
#                 else:
#                     print("Not enough history")
#     except Exception as e:
#         logging.error(traceback.format_exc())
#         time.sleep(10)
#         i = i - 1



