class Model:
    def __init__(self):
        self.purchase_hist = None
        self.distribution = None

    def calculate_distribution(self, df_purchase_hist):
        df_sum = df_purchase_hist.pivot_table(index='itm_nbr', columns=['osd_decided_val'], values='case_sum',
                                              fill_value=0).sum()
        df_dist = df_sum / df_sum.sum()
        return df_dist

    def get_similarity_and_sort(self, attribute_dist, df_itms_available):
        df_one_hot = df_itms_available.pivot_table(index='itm_nbr', columns=['osd_decided_val'], values='value',
                                                   fill_value=0)
        common_attributes = set(df_one_hot.columns).intersection(set(attribute_dist.index))
        sorted_supc = (df_one_hot[common_attributes] * attribute_dist[common_attributes]).sum(axis=1).sort_values(
            ascending=False).index[:10]
        return sorted_supc
