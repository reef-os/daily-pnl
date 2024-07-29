import pandas as pd
from src.ETL.statement.extract.extractor import Extractor


def start_statement(start_date_str, end_date_str):
    extractor = Extractor(start_date_str, end_date_str)
    df = extractor.get_data()
    df_final = df.drop(['monthly_standard_amount_local', 'daily_amount_local'], axis=1)
    renamed_df = df_final.rename(columns={
        'vessel code': 'Vessel',
        'country': 'Country',
        'item': 'Line Item',
        'daily_amount_usd': 'Amount',
        'business date local': 'Business Date Local',
        'vessel name': 'Vessel Name'
    })

    df_mapping = pd.read_csv('src/static/line_order_mapping.csv')
    ### Line Item -> Pl Mapping
    line_item_dict = dict(zip(df_mapping['pl_mapping_4'], df_mapping['Line Order']))
    renamed_df['Line Order'] = renamed_df['Line Item'].map(line_item_dict)

    ### Line Order - Line Item
    line_item_dict = dict(zip(df_mapping['Line Order'], df_mapping['Line Item']))
    renamed_df['Line Item'] = renamed_df['Line Order'].map(line_item_dict)


    #final_transformed_df = renamed_df[renamed_df['Vessel'] == 'MIA-009-07']
    #renamed_df.to_csv('data/statement.csv', index=False)
    return renamed_df


#start_statement()
