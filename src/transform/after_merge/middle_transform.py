import pandas as pd

def add_new_line_item(df):
    new_rows = []
    df_filtered = df[(df['is_ulysses'] == True) & (df['Line Order'] == 'L1-05')]
    df_unique = df_filtered.drop_duplicates(subset=['Vessel', 'Business Date Local']).sort_values(
        by=['Vessel', 'Business Date Local'])
    for index, row in df_unique.iterrows():
        new_row = row.copy()
        new_row['Line Item'] = 'L1 Expenses'
        new_row['Line Order'] = 'L1-11-02'
        new_row['pl_mapping_2'] = '2. Residual Content Royalty'
        new_row['pl_mapping_3'] = '(+)charges to Ulysses'
        new_row['pl_mapping_4'] = ''
        new_row['Amount'] = row['Amount'] * 0.05
        new_rows.append(new_row)
    return pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)


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


def old_update_unknown_vessels(df):
    unknown_vessels = df[df['Vessel Name'] == 'Unknown Vessel Name']
    for index, row in unknown_vessels.iterrows():
        matching_vessel = df[(df['Vessel'] == row['Vessel']) & (df['Vessel Name'] != 'Unknown Vessel Name')]
        if not matching_vessel.empty:
            df.at[index, 'Vessel Name'] = matching_vessel['Vessel Name'].values[0]
    print("Unknown vessels updated")
    return df

def update_unknown_vessels(df):
    # Find the 'Vessel Name' that are not 'Unknown Vessel Name'
    vessel_name_map = df[df['Vessel Name'] != 'Unknown Vessel Name'].groupby('Vessel')['Vessel Name'].first()

    # Use the map to update the 'Unknown Vessel Name' entries
    df['Vessel Name'] = df.apply(
        lambda row: vessel_name_map.get(row['Vessel'], row['Vessel Name'])
        if row['Vessel Name'] == 'Unknown Vessel Name' else row['Vessel Name'], axis=1
    )

    print("vessel named updated")
    return df


def start_transform(df):
    df = update_unknown_vessels(df)
    df = multiply_amount_by_negative(df)
    df = map_pl_columns(df)
    df['Region'] = df['Country'].apply(map_region)
    return df