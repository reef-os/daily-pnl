import pandas as pd


class Transformer:
    def __init__(self):
        pass

    @staticmethod
    def transform(df):
        df_final = df.drop(['monthly_standard_amount_local', 'daily_amount_local'], axis=1)
        renamed_df = df_final.rename(columns={
            'vessel code': 'Vessel',
            'country': 'Country',
            'item': 'Line Item',
            'daily_amount_usd': 'Amount',
            'business date local': 'Business Date Local',
            'vessel name': 'Vessel Name'
        })
        return renamed_df

    @staticmethod
    def map_pl_mapping4_to_line_order(df):
        df_mapping = pd.read_csv('static/line_order_mapping.csv')
        line_item_dict = dict(zip(df_mapping['pl_mapping_4'], df_mapping['Line Order']))
        df['Line Order'] = df['Line Item'].map(line_item_dict)
        return df