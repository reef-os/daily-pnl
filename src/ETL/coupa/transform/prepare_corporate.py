mapping_dict = {
    '(-)SG&A - Profession': 'L6-01-01',
    '(-)SG&A - People Cos': 'L6-02-01',
    '(-)SG&A - Insurance': 'L6-01-02',
    '(-)SG&A - Other': 'L6-01-03',
    '(-)Other Income & Ex': 'L6-01-04',
    '(-)SG&A - T&E': 'L6-01-05',
    '(-)SG&A Application': 'L6-01-06',
    '(-)SG&A - Utility Ex': 'L6-01-07',
    '(-)SG&A - Rent': 'L6-01-08'
}


class PrepareCorporate:

    @staticmethod
    def __line_order_mapping(df):
        df['Line Order'] = df['Line Item'].map(mapping_dict)
        return df

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

    @staticmethod
    def __filter_account_type(df):
        first_account_type_idx = df.columns.get_loc('account_type')
        df = df.loc[:, ~((df.columns == 'account_type') & (df.columns.duplicated(keep='first') | (
                df.columns.get_loc('account_type') != first_account_type_idx)))]
        filtered_df = df[df['account_type'] == 'REEF Corporate']
        df_cleaned = filtered_df.dropna(subset=['account_category'])
        return df_cleaned
    def start_transform(self, df):
        print("Second Transform...")
        df_filtered = self.__filter_account_type(df)
        df_final = self.__select_relevant_columns(df_filtered)
        renamed_df = self.__rename_columns(df_final)
        grouped_df = renamed_df.groupby(['Vessel', 'expense_level', 'order_date', 'Line Item', 'Business Date Local'],as_index=False).agg({'Amount': 'sum'})
        mapped_df = self.__line_order_mapping(grouped_df)
        print("Second Transform DONE!")
        return mapped_df
