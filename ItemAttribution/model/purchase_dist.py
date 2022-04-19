import numpy as np
import pandas as pd
from scipy.spatial import distance
import pickle
import datetime
import os
from sklearn.metrics.pairwise import cosine_similarity


class Purchase_dist:

    def __init__(self, df_purchase_hist, df_itm_attribute, attr_grp_nm, opco, cuisine, mkt_lvl, dt):
        self.df_purchase_hist = df_purchase_hist
        self.df_itm_attribute = df_itm_attribute
        attr_grp_nm = attr_grp_nm.replace("/", "_")
        self.attr_grp_nm = attr_grp_nm
        self.opco = str(opco)
        self.cuisine = cuisine
        self.today = dt
        self.decided_val_index_dict = []
        self.mkt_lvl = mkt_lvl
        for priority in [1, 2, 3, 4, 5]:
            decided_vals = self.df_itm_attribute[
                self.df_itm_attribute['catman_attr_prr_nbr'] == str(priority)].osd_decided_val.unique().tolist()
            self.decided_val_index_dict.append({decided_vals[i]: i for i in range(len(decided_vals))})

    def calculate_purchse_dist(self):
        dates = [self.today - datetime.timedelta(i) for i in range(10)]
        found_distribution = False
        for i in dates:
            file_name = self.mkt_lvl + "_" + self.attr_grp_nm + "_" + self.cuisine + "_" + str(i) + ".pickle"
            if file_name in os.listdir("../data/purchase_dist/"):
                found_distribution = True
                print("found distribution from cache", file_name)
                with open("../data/purchase_dist/" + file_name, 'rb') as handle:
                    purchase_dist = pickle.load(handle)
                    self.dist = purchase_dist['dist']
                break
        if not found_distribution:
            dist_len = sum([len(self.df_itm_attribute[self.df_itm_attribute['catman_attr_prr_nbr'] == str(
                priority)].osd_decided_val.unique().tolist()) for priority in [1, 2, 3]])
            self.dist = np.zeros([dist_len])
            dist = [np.zeros([len(self.decided_val_index_dict[i])]) for i in range(5)]
            for i in range(self.df_purchase_hist.shape[0]):
                df = self.df_purchase_hist.iloc[i]
                i_skey = df['itm_skey']
                df_attr_skey = self.df_itm_attribute[self.df_itm_attribute['itm_skey'] == i_skey]
                for priority in [1, 2, 3, 4, 5]:
                    decided_val = df_attr_skey[df_attr_skey['catman_attr_prr_nbr'] == str(priority)][
                        'osd_decided_val'].tolist()
                    if len(decided_val) > 0:
                        decided_val = decided_val[0]
                        index = self.decided_val_index_dict[priority - 1].get(decided_val, -1)
                        if index > -1:
                            dist[priority - 1][index] += df['sum']
            self.dist = np.concatenate([i / np.nansum(i) for i in dist]).ravel().tolist()
            purchase_dist = {'dist': self.dist}
            file_name = "../data/purchase_dist/" + self.mkt_lvl + "_" + self.attr_grp_nm + "_" + self.cuisine + "_" + str(
                self.today) + ".pickle"
            with open(file_name, 'wb') as handle:
                pickle.dump(purchase_dist, handle, protocol=pickle.HIGHEST_PROTOCOL)
        return self.dist

    def get_similarity_and_sort(self, df_itms_in_opco):
        itm_skey_list = df_itms_in_opco.itm_skey.unique().tolist()
        list_of_supc_similarity = []
        for itm in itm_skey_list:
            df = df_itms_in_opco[df_itms_in_opco['itm_skey'] == itm]
            dist = [np.zeros([len(self.decided_val_index_dict[i])]) for i in range(5)]
            for priority in [1, 2, 3, 4, 5]:
                decided_val = df[df['catman_attr_prr_nbr'] == str(priority)]['osd_decided_val'].tolist()
                if len(decided_val) > 0:
                    decided_val = decided_val[0]
                    index = self.decided_val_index_dict[priority - 1].get(decided_val, -1)
                    if index > -1:
                        dist[priority - 1][index] = 1
            dist = np.concatenate([i for i in dist]).ravel().tolist()
            # print(len(dist), len(self.dist))
            supc = df.supc.unique().tolist()
            if len(supc) > 0:
                supc = supc[0]
            else:
                supc = 0
            # list_of_supc_similarity.append((distance.cosine(dist, self.dist), supc))
            list_of_supc_similarity.append((cosine_similarity([dist], [self.dist])[0], supc))
        return [i[1] for i in sorted(list_of_supc_similarity, reverse=True)]
