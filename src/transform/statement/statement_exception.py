import numpy as np
from src.helpers.db_reader import DbReader


def get_statement_exceptions():
    db_reader = DbReader()
    return db_reader.get_data("statement_exceptions")


def apply_statement_exceptions(main_df):
    result_df = main_df.copy()

    currency_country_mapping = {
        'US': 1,
        'GB': 1.4,
        'AE': 0.27,
        'CA': 0.74,
        'ES': 1.2
    }

    df_statement = get_statement_exceptions()
    df_statement = df_statement[~df_statement['vessel'].str.startswith('NYC')]

    for index, row in df_statement.iterrows():
        df_statement.at[index, 'amount_local'] = row['amount_local'] * currency_country_mapping.get(row['country'], 1)

    result_df = result_df.replace('nan', np.nan)
    mask = result_df['pl_mapping_4'].notna()

    for index, row in result_df[mask].iterrows():
        filter_statement_df = df_statement[
            (df_statement['vessel'] == row['Vessel']) & (df_statement['category'] == row['pl_mapping_4'])]
        if len(filter_statement_df) > 0:
            main_df.at[index, 'Amount'] = (filter_statement_df['amount_local'].values[0] * 12) / 365
    print("Statement Exceptions Applied")
    return main_df
