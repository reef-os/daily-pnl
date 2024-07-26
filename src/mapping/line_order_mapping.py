import pandas as pd


class LineOrderMapping:
    def __init__(self):
        pass

    def map_line_order_and_line_item(self, df):
        df.to_csv('first_df.csv')
        df_mapping = pd.read_csv('static/line_order_mapping.csv')
        mapping_dict = df_mapping.set_index('Line Item')['Line Order'].to_dict()
        df['Line Order'] = df['Line Item'].map(mapping_dict).fillna(df['Line Order'])
        df.to_csv('second_df.csv')
        return df