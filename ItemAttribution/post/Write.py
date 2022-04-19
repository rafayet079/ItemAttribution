import pandas as pd


class Write:
    def __init__(self, today):
        self.df = pd.DataFrame()
        self.date = str(today)

    def write_list_to_file(self, supc_list, co_cust, attr_grp_nm):
        attr_grp_nm = attr_grp_nm.replace("/", "_")
        co_cust = int(co_cust)
        co_cust = str(co_cust).zfill(9)
        df = pd.DataFrame()
        df['supc'] = supc_list
        df.to_csv("../data/output/" + co_cust + "_" + attr_grp_nm + "_" + self.date + ".csv")

    def write_list_to_separate_file(self, supc_list, opco, attr_grp_nm):
        attr_grp_nm = attr_grp_nm.replace("/", "_")
        df = pd.DataFrame()
        df['supc'] = supc_list
        df.to_csv("../data/output/" + opco + "_" + attr_grp_nm + "_" + self.date + ".csv")

    def write_to_file_formatted(self, supc_list, opco, attr_grp_nm, cuisine):
        try:
            df = pd.read_csv("../data/output/PA_out.csv")
        except FileNotFoundError:
            df = pd.DataFrame()
        df_new = pd.DataFrame()
        item_count = min(10, len(supc_list))
        df_new['opco'] = [opco]*item_count
        df_new['cuisine_lvl_2_desc'] = [cuisine]*item_count
        df_new['attr_grp_nm'] = [attr_grp_nm]*item_count
        df_new['supc'] = supc_list[:item_count]
        df_new['dt'] = [self.date]*item_count
        df = pd.concat([df, df_new], ignore_index=True)
        df.to_csv("../data/output/PA_out.csv", index=False)
