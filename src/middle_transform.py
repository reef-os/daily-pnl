import pandas as pd


def map_region(country):
    region_mapping = {
        'NA': ['US', 'CA'],
        'MENA': ['AE']
    }
    for region, countries in region_mapping.items():
        if country in countries:
            return region
    return 'EU'


def map_pl_columns(df):
    df_mapping = pd.read_csv('static/line_order_mapping.csv')
    mapping_dict = dict(zip(df_mapping['Line Order'], df_mapping['Line Item']))
    def update_line_item(row):
        if row['Line Order'] in mapping_dict:
            return mapping_dict[row['Line Order']]
        else:
            return row['Line Item']

    # Line Item sütununu güncelle
    df['Line Item'] = df.apply(update_line_item, axis=1)

    df['pl_mapping_2'] = df['Line Order'].map(dict(zip(df_mapping['Line Order'], df_mapping['pl_mapping_2'])))
    df['pl_mapping_3'] = df['Line Order'].map(dict(zip(df_mapping['Line Order'], df_mapping['pl_mapping_3'])))
    df['pl_mapping_4'] = df['Line Order'].map(dict(zip(df_mapping['Line Order'], df_mapping['pl_mapping_4'])))
    df[['pl_mapping_2', 'pl_mapping_3', 'pl_mapping_4']] = df[['pl_mapping_2', 'pl_mapping_3', 'pl_mapping_4']].astype(
        str)
    print("PL columns mapped")
    return df


def multiply_amount_by_negative(df):
    df_negative = pd.read_csv('static/eksi-ile-carp.csv')
    negative_line_orders = df_negative[df_negative['Check'] == 'Yes']['Line Order'].unique()
    df.loc[df['Line Order'].isin(negative_line_orders), 'Amount'] *= -1
    print("Amounts multiplied by negative")
    return df


def update_unknown_vessels(df):
    unknown_vessels = df[df['Vessel Name'] == 'Unknown Vessel Name']
    for index, row in unknown_vessels.iterrows():
        matching_vessel = df[(df['Vessel'] == row['Vessel']) & (df['Vessel Name'] != 'Unknown Vessel Name')]
        if not matching_vessel.empty:
            df.at[index, 'Vessel Name'] = matching_vessel['Vessel Name'].values[0]
    print("Unknown vessels updated")
    return df


def start_transform(df):
    df = update_unknown_vessels(df)
    df = multiply_amount_by_negative(df)
    df = map_pl_columns(df)
    df['Region'] = df['Country'].apply(map_region)
    return df