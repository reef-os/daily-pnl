class SecondTransformer:
    def __init__(self):
        pass
    @staticmethod
    def __rename_columns(df):
        column_name_mapping = {
            'vessel_code': 'Vessel',
            'account_category': 'Line Item',
            'pnl_contribution_daily_usd_rounded': 'Amount',
            'country': 'Country',
            'vessel name': 'Vessel Name',
        }
        df.rename(columns=column_name_mapping, inplace=True)
        return df
    @staticmethod
    def __select_relevant_columns(df):
        columns = ['vessel_code', 'expense_level', 'order_date', 'account_category',
                   'pnl_contribution_daily_usd_rounded', 'Business Date Local']
        return df[columns]

    def start_transform(self, df):
        print("Second Transform...")
        first_account_type_idx = df.columns.get_loc('account_type')
        df = df.loc[:, ~((df.columns == 'account_type') & (df.columns.duplicated(keep='first') | (
                df.columns.get_loc('account_type') != first_account_type_idx)))]
        filtered_df = df[df['account_type'] == 'REEF Corporate']
        df_cleaned = filtered_df.dropna(subset=['account_category'])
        df_final = self.__select_relevant_columns(df_cleaned)
        renamed_df = self.__rename_columns(df_final)
        grouped_df = renamed_df.groupby(['Vessel', 'expense_level', 'order_date', 'Line Item', 'Business Date Local'],
                                        as_index=False).agg({'Amount': 'sum'})
        print("Second Transform DONE!")
        return grouped_df
