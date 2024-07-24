import pandas as pd
from src.ETL.pnl_orders.pnl_order_manager import PnlOrderManager
from src.ETL.statement.statement_manager import StatementManager
from src.ETL.coupa.coupa_manager import CoupaManager
from src.merge_manager import merge_pnl_coupa, merge_statement_merged_data


def get_unique_vessel_name(df):
    unique_vessel_names_list = df['Vessel'].unique().tolist()
    filtered_array = [item for item in unique_vessel_names_list if
                      '-' in item and not (item.split('-')[1] in ['900', '000'] or item.startswith('RHQ'))]
    return len(filtered_array)


def preapre_labor_mapping():
    df = pd.read_csv('labor_mapping.csv')
    df['Apr'] = df['Apr'].str.replace(',', '').astype(float)
    df['May'] = df['May'].str.replace(',', '').astype(float)
    df['Jun'] = df['Jun'].str.replace(',', '').astype(float)
    return df


def final_transform(df):
    new_rows = []
    labor_mapping = preapre_labor_mapping()
    df['Business Date Local'] = pd.to_datetime(df['Business Date Local'])

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
            #print(f"----- İŞLEM YAPILACAK VESSEL: {row['Vessel']} | Tarih: {row['Business Date Local']} -----")
            for index_spesific, row_spesific in df_country_spesific.iterrows():
                #print(f"İşlem yapılıyor; {row_spesific['Line Item']} | Line Order: {row_spesific['Line_Order']} | ")
                new_row = row.copy()
                new_row['Vessel'] = row['Vessel']
                new_row['Vessel Name'] = row['Vessel Name']
                new_row['Business Date Local'] = row['Business Date Local']
                new_row['Country'] = row['Country']
                new_row['Line Item'] = row_spesific['Line_Item']
                new_row['Amount'] = int(row_spesific[month_col]) / country_vessel_counts[row['Country']]
                new_row['Line Order'] = row_spesific['Line_Order']
                new_rows.append(new_row)
            #print("------------------------------ İŞLEM BİTTİ ------------------------------")

    uniq_vessle_count = int(get_unique_vessel_name(df_unique))
    global_row = labor_mapping.loc[df['Country'] == 'Global']
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
            new_row['Line Item'] = global_row['Line_Item']
            new_row['Amount'] = int(global_row[month_col]) / uniq_vessle_count
            new_row['Line Order'] = global_row['Line']
            new_rows.append(new_row)

    new_df = pd.DataFrame(new_rows)
    final_df = pd.concat([df, new_df], ignore_index=True)
    print("Merging first and second coupa DONE!")
    return final_df


class ETLManager:
    def __init__(self, start_date_str, end_date_str):
        self.__pnl_manager = PnlOrderManager(start_date_str, end_date_str)
        self.__coupa_manager = CoupaManager(start_date_str, end_date_str)
        self.__statement_manager = StatementManager(start_date_str, end_date_str)

    def start(self):
        pnl_df = self.__pnl_manager.start(local=False)
        pnl_df.to_csv("data/bronze/pnl.csv")

        statement_df = self.__statement_manager.start(local=False)
        statement_df.to_csv("data/bronze/statement.csv")

        coupa_df = self.__coupa_manager.start(local=False)
        coupa_df.to_csv("data/bronze/coupa.csv")

        merge_pnl_coupa_df = merge_pnl_coupa(pnl_df, coupa_df)
        merge_pnl_coupa_df.to_csv("data/silver/merge_pnl_coupa.csv")

        final_df = merge_statement_merged_data(merge_pnl_coupa_df, statement_df)

        final_df['Vessel Name'] = final_df['Vessel Name'].replace('', 'Unnamed Vessel').fillna('Unnamed Vessel')
        final_df['Line Order'] = final_df['Line Order'].replace('', 'Unnamed LineOrder').fillna('Unnamed LineOrder')

        final_df['Business Date Local'] = pd.to_datetime(final_df['Business Date Local']).dt.strftime('%Y-%m-%d')
        last_final = final_transform(final_df)
        last_final.to_csv("data/gold/final_haziran.csv")