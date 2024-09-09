import pandas as pd

from src.transform.after_merge.distribute import distribute_labor_costs, distrubute_reef_commission_expense
from src.transform.after_merge.middle_transform import start_transform, add_new_line_item
from src.transform.statement.statement_exception import apply_statement_exceptions
from transform.after_merge.create_data_date_range import create_data_between_range
from transform.after_merge.adjusment import adjusment, adjusment_with_percantage


def process_data(df, start_date_str, end_date_str):
    df['Vessel Name'] = df['Vessel Name'].replace('', 'Unknown Vessel Name').fillna('Unknown Vessel Name')
    df['Line Order'] = df['Line Order'].replace('', 'Unnamed LineOrder').fillna('Unnamed LineOrder')
    df = df[df['Line Order'] != 'Unnamed LineOrder']
    df = distribute_labor_costs(df)
    df = start_transform(df)
    df = distrubute_reef_commission_expense(df)
    df = apply_statement_exceptions(df)
    df = add_new_line_item(df)
    df = df.groupby(['Vessel', 'Line Item', 'Business Date Local', 'Vessel Name', 'Country', 'is_ulysses', 'Line Order',
                     'pl_mapping_2', 'pl_mapping_3', 'pl_mapping_4', 'Region']).sum().reset_index()
    df = adjusment_with_percantage(df)
    created_dfs = create_data_between_range(start_date_str, end_date_str)
    df = pd.concat([df, created_dfs], ignore_index=True)
    return df
