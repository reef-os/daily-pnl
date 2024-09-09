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


def date_by_date_vessel_unique_count(df):
    df['Region'] = df['Region'].fillna('NA').str.strip().str.upper()

    country_vessel_counts_by_date = {}
    for region in ['MENA', 'NA', 'EU']:
        # Orijinal df'yi koruyarak her region için ayrı bir DataFrame oluştur
        region_df = df[df['Region'] == region]
        vessel_counts_by_date = region_df.groupby('Business Date Local')['Vessel'].nunique().to_dict()
        country_vessel_counts_by_date[region] = vessel_counts_by_date

    return country_vessel_counts_by_date


def adjusment_with_percantage(df):
    adjusment_df = pd.read_csv('static/adjusments/Jun,Jul,Aug Adjustment.csv')
    new_rows = []
    df_gross_sales = df[df['Line Item'] == 'Gross Sales']
    df_unique = df_gross_sales.drop_duplicates(subset=['Vessel', 'Business Date Local'])
    df_unique = df_unique.sort_values(by=['Vessel', 'Business Date Local'])

    df_unique['Region'] = df_unique['Country'].apply(map_region)
    adjusment_df['Region'] = adjusment_df['Region'].fillna('NA').str.strip().str.upper()

    for index, row in df_unique.iterrows():
        df_country_filtered = adjusment_df[adjusment_df['Region'] == row['Region']]
        if len(df_country_filtered) > 0:
            for index_spesific, row_spesific in df_country_filtered.iterrows():
                new_row = row.copy()
                new_row['Line Item'] = row_spesific['Line Item']
                new_row['Line Order'] = row_spesific['Line Order']
                new_row['pl_mapping_2'] = row_spesific['pl_mapping_2']
                new_row['pl_mapping_3'] = row_spesific['pl_mapping_3']
                new_row['pl_mapping_4'] = row_spesific['pl_mapping_4']
                new_row['Region'] = row['Region']
                new_row['Amount'] = row['Amount'] * row_spesific['Percentage']
                new_rows.append(new_row)

    df_new_rows = pd.DataFrame(new_rows)
    final_df = pd.concat([df, df_new_rows], ignore_index=True)
    final_df['Region'] = final_df['Region'].fillna('NA').str.strip().str.upper()
    return final_df


def adjusment(df):
    print("1-first main (df): ", len(df))
    adjusment_df = pd.read_csv(
        'static/adjusments/Apr Adjustments.csv')
    new_rows = []
    df_unique = df.drop_duplicates(subset=['Vessel', 'Business Date Local']).sort_values(
        by=['Vessel', 'Business Date Local'])

    country_vessel_counts_by_date = date_by_date_vessel_unique_count(df)
    df_unique['Region'] = df_unique['Country'].apply(map_region)
    adjusment_df['Region'] = adjusment_df['Region'].fillna('NA').str.strip().str.upper()

    for index, row in df_unique.iterrows():
        df_country_filtered = adjusment_df[adjusment_df['Region'] == row['Region']]
        if len(df_country_filtered) > 0:
            for index_spesific, row_spesific in df_country_filtered.iterrows():
                new_row = row.copy()
                new_row['Line Item'] = row_spesific['Line Item']
                new_row['Line Order'] = row_spesific['Line Order']
                new_row['pl_mapping_2'] = row_spesific['pl_mapping_2']
                new_row['pl_mapping_3'] = row_spesific['pl_mapping_3']
                new_row['pl_mapping_4'] = row_spesific['pl_mapping_4']
                new_row['Region'] = row['Region']
                vessel_count = country_vessel_counts_by_date.get(row['Region'], {}).get(row['Business Date Local'], 1)
                new_row['Amount'] = row_spesific['Amount'] / vessel_count
                new_rows.append(new_row)
        else:
            print(f"No match found for Region: {row['Region']}")
            print(f"Unique Regions in df_unique: {df_unique['Region'].unique()}")
            print(f"Unique Regions in adjusment_df: {adjusment_df['Region'].unique()}")
    new_df = pd.DataFrame(new_rows)
    new_df = new_df.drop_duplicates()
    final_df = pd.concat([df, new_df], ignore_index=True)
    final_df['Region'] = final_df['Region'].fillna('NA').str.strip().str.upper()
    return final_df
