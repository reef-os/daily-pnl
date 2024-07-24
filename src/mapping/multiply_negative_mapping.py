import pandas as pd


class MultiplyNegativeMapping:

    @staticmethod
    def eksi_bir_carp(df):
        df2 = pd.read_csv('static/eksi-ile-carp.csv')
        df2_filtered = df2[df2['Check'] == 'Yes']
        line_orders_to_update = df2_filtered['Line Order'].unique()
        df.loc[df['Line Order'].isin(line_orders_to_update), 'Amount'] *= -1
        return df
