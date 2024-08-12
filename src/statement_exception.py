from helpers.db_reader import DbReader


def get_statement_exceptions():
    db_reader = DbReader()
    return db_reader.get_data("statement_exceptions")


def apply_statement_exceptions(main_df):
    df_statement = get_statement_exceptions()
    df_statement = df_statement[~df_statement['vessel'].str.startswith('NYC')]
    main_df = main_df[main_df['pl_mapping_4'].notna()]

    for index, row in main_df.iterrows():
        filter_statement_df = df_statement[
            (df_statement['vessel'] == row['Vessel']) &
            (df_statement['category'] == row['pl_mapping_4'])]
        if len(filter_statement_df) > 0:
            main_df.at[index, 'Amount'] = (filter_statement_df['amount_local'].values[0] * 12) / 365
    print("Statement Exceptions Applied")
    return main_df