import time
from src.etl_manager import ETLManager
import pandas as pd

def eksi_bir_carp(df):
    df2 = pd.read_csv('eksi-ile-carp.csv')
    df2_filtered = df2[df2['Check'] == 'Yes']
    line_orders_to_update = df2_filtered['Line Order'].unique()
    df.loc[df['Line Order'].isin(line_orders_to_update), 'Amount'] *= -1
    return df


def map_line_order_and_line_item(df):
    df_mapping = pd.read_csv('line_order_mapping.csv')
    mapping_dict = df_mapping.set_index('Line Item')['Line Order'].to_dict()

    df['Line Order'] = df['Line Item'].map(mapping_dict).fillna(df['Line Order'])

    return df


if __name__ == "__main__":


    #etl_manager = ETLManager('2024-06-01', '2024-06-30')
    #etl_manager.start()

    df = pd.read_csv('mapped_final.csv')
    df_final = eksi_bir_carp(df)
    df_final.to_csv('eksi_ile_carpilmis.csv', index=False)




