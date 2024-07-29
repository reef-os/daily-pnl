import time
import pandas as pd
from src.coupa import start_coupa
from src.pnl_orders import start_pnl_orders
from src.statement import start_statement
from datetime import datetime, timedelta


def concat_dfs(df1, df2, df3):
    #df1 = pd.read_csv("data/coupa.csv")
    #df2 = pd.read_csv("data/pnl_orders.csv")
    #df3 = pd.read_csv("data/statement.csv")

    result = pd.concat([df1, df2, df3], ignore_index=True)
    result = result[result['Line Item'] != 'Commission Usd']
    result = result[result['Amount'] != 0]
    #result.to_csv("data/gold/concat.csv", index=False)
    return result


def eksi_ile_carp(df):
    df2 = pd.read_csv('src/static/eksi-ile-carp.csv')
    df2_filtered = df2[df2['Check'] == 'Yes']
    line_orders_to_update = df2_filtered['Line Order'].unique()
    df.loc[df['Line Order'].isin(line_orders_to_update), 'Amount'] *= -1
    return df


def find_vessels_name(df):
    unknown_vessels = df[df['Vessel Name'] == 'Unknown Vessel Name']
    for index, row in unknown_vessels.iterrows():
        matching_vessel = df[(df['Vessel'] == row['Vessel']) & (df['Vessel Name'] != 'Unknown Vessel Name')]
        if not matching_vessel.empty:
            df.at[index, 'Vessel Name'] = matching_vessel['Vessel Name'].values[0]
    return df


def spread_labor(df):
    df['Vessel Name'] = df['Vessel Name'].replace('', 'Unknown Vessel Name').fillna('Unknown Vessel Name')
    df['Line Order'] = df['Line Order'].replace('', 'Unnamed LineOrder').fillna('Unnamed LineOrder')
    df['Business Date Local'] = pd.to_datetime(df['Business Date Local']).dt.strftime('%Y-%m-%d')
    df['Business Date Local'] = pd.to_datetime(df['Business Date Local'])
    ### LABOR DAGITMA ###
    labor_mapping = pd.read_csv('src/static/labor_mapping.csv')
    labor_mapping['Apr'] = labor_mapping['Apr'].str.replace(',', '').astype(float)
    labor_mapping['May'] = labor_mapping['May'].str.replace(',', '').astype(float)
    labor_mapping['Jun'] = labor_mapping['Jun'].str.replace(',', '').astype(float)

    us_vessels = df[df['Country'] == 'US']
    ca_vessels = df[df['Country'] == 'CA']
    gb_vessels = df[df['Country'] == 'GB']
    ae_vessels = df[df['Country'] == 'AE']

    unique_us_vessel_count = len(us_vessels['Vessel'].unique())
    unique_ca_vessel_count = len(ca_vessels['Vessel'].unique())
    unique_gb_vessel_count = len(gb_vessels['Vessel'].unique())
    unique_ae_vessel_count = len(ae_vessels['Vessel'].unique())

    country_vessel_counts = {
        'US': unique_us_vessel_count,
        'CA': unique_ca_vessel_count,
        'GB': unique_gb_vessel_count,
        'AE': unique_ae_vessel_count
    }
    new_rows = []
    df_unique_ = df.drop_duplicates(subset=['Vessel', 'Business Date Local'])
    df_unique = df_unique_.sort_values(by=['Vessel', 'Business Date Local'])
    for index, row in df_unique.iterrows():
        df_country_spesific = labor_mapping[labor_mapping['Country'] == row['Country']]
        month = row['Business Date Local'].month
        if month == 4:
            month_col = 'Apr'
        elif month == 5:
            month_col = 'May'
        elif month == 6:
            month_col = 'Jun'
        else:
            print(f"!!! MONTH BULUNAMADI !!! month: {month} | date local: {row['Business Date Local']}", )
            month_col = ''

        if len(df_country_spesific) > 0:
            for index_spesific, row_spesific in df_country_spesific.iterrows():
                new_row = row.copy()
                new_row['Vessel'] = row['Vessel']
                new_row['Vessel Name'] = row['Vessel Name']
                new_row['Business Date Local'] = row['Business Date Local']
                new_row['Country'] = row['Country']
                new_row['Line Item'] = row_spesific['Line_Item']
                new_row['Amount'] = int(row_spesific[month_col]) / country_vessel_counts[row['Country']]
                new_row['Line Order'] = row_spesific['Line_Order']
                new_rows.append(new_row)

    ### GLOBAL  DAGITMA ###
    unique_vessel_names_list = df_unique['Vessel'].unique().tolist()
    filtered_array = [item for item in unique_vessel_names_list if
                      '-' in item and not (item.split('-')[1] in ['900', '000'] or item.startswith('RHQ'))]
    uniq_vessle_count = len(filtered_array)

    global_row = labor_mapping[labor_mapping['Country'] == 'Global']
    for index, row in df_unique.iterrows():
        month = row['Business Date Local'].month
        if month == 4:
            month_col = 'Apr'
        elif month == 5:
            month_col = 'May'
        elif month == 6:
            month_col = 'Jun'
        else:
            print(f"!!! MONTH BULUNAMADI !!! month: {month} | date local: {row['Business Date Local']}", )
            month_col = ''

        if len(global_row) > 0:
            new_row = row.copy()
            new_row['Vessel'] = row['Vessel']
            new_row['Vessel Name'] = row['Vessel Name']
            new_row['Business Date Local'] = row['Business Date Local']
            new_row['Country'] = row['Country']
            new_row['Line Item'] = global_row['Line_Item'].iloc[0]
            new_row['Amount'] = float(global_row[month_col].iloc[0] / uniq_vessle_count)
            new_row['Line Order'] = global_row['Line_Order'].iloc[0]
            new_rows.append(new_row)

    new_df = pd.DataFrame(new_rows)
    final_df = pd.concat([df, new_df], ignore_index=True)
    return final_df


def start(merged_df):
    #df = pd.read_csv("data/gold/concat.csv")
    df = spread_labor(merged_df)
    print("Labor spreaded")
    df = find_vessels_name(df)
    print("Vessels name found")
    df = eksi_ile_carp(df)
    print("Eksi ile carpildi")
    #df.to_csv("data/gold/haziran_data.csv", index=False)
    return df


def get_all():
    yesterday = datetime.today() - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')

    df_coupa = start_coupa("2024-06-01", "2024-06-30")
    df_pnl = start_pnl_orders("2024-06-01", "2024-06-30")
    df_statement = start_statement("2024-06-01", "2024-06-30")

    merged_df = concat_dfs(df_coupa, df_pnl, df_statement)

    final_df = start(merged_df)
    return final_df


def merge_with_chunk():
    chunksize = 100000
    result = pd.DataFrame()

    for chunk in pd.read_csv("data/gold/nisan_data.csv", chunksize=chunksize):
        result = pd.concat([result, chunk], ignore_index=True)

    for chunk in pd.read_csv("data/gold/mayis_data.csv", chunksize=chunksize):
        result = pd.concat([result, chunk], ignore_index=True)

    for chunk in pd.read_csv("data/gold/haziran_data.csv", chunksize=chunksize):
        result = pd.concat([result, chunk], ignore_index=True)

    # Birleştirilmiş dosyayı kaydedelim
    result.to_csv("data/data2.csv", index=False)


if __name__ == "__main__":
    print("Started")
