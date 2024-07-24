class Transformer:
    def __init__(self):
        pass

    @staticmethod
    def find_account_type(df):
        first_account_type_idx = df.columns.get_loc('account_type')
        df = df.loc[:, ~((df.columns == 'account_type') & (
                df.columns.duplicated(keep='first') | (df.columns.get_loc('account_type') != first_account_type_idx)))]
        return df

    @staticmethod
    def __filter_data(coupa_df):
        df_filtered = coupa_df[coupa_df['expense_level'] != 'Ignore']
        df_filtered = df_filtered[df_filtered['vessel_code'] != 'Overhead']
        pl_map_df = df_filtered[df_filtered['pl_mapping_1'].isin(
            ['L1 Expenses', 'L2 Expenses', 'L3 Expenses', 'L4 Expenses', 'Non-recurring Costs'])]
        pl_map_df_2 = Transformer.find_account_type(pl_map_df)
        final_df = pl_map_df_2[pl_map_df_2['account_type'] != 'REEF Corporate']
        return final_df

    @staticmethod
    def __rename_columns(df):
        column_name_mapping = {
            'vessel_code': 'Vessel',
            'pl_mapping_1': 'Line Item',
            'pnl_contribution_daily_usd_rounded': 'Amount',
            'country': 'Country',
            'line_item': 'Line Item',
            'amount': 'Amount',
            'gl_account': 'Gl Account',
            'vessel name': 'Vessel Name',
        }
        df.rename(columns=column_name_mapping, inplace=True)
        return df

    @staticmethod
    def __select_relevant_columns(df):
        columns = ['Vessel', 'start_date', 'end_date', 'Line Item', 'Amount', 'Gl Account', 'Business Date Local',
                   'Vessel Name', 'Country']
        return df[columns]

    def start_transform(self, coupa_df):
        print("Transforming coupa data...")
        filtered_df = self.__filter_data(coupa_df)
        renamed_df = self.__rename_columns(filtered_df)
        final_df = self.__select_relevant_columns(renamed_df)
        print("Transforming coupa data DONE!")
        return final_df
