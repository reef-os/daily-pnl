import numpy as np
import pandas as pd
from src.ETL.coupa.transform.transformer import Transformer
from src.ETL.coupa.extract.extractor import Extractor


def get_unique_vessel_name(df):
    unique_vessel_names_list = df['Vessel'].unique().tolist()
    filtered_array = [item for item in unique_vessel_names_list if
                      '-' in item and not (item.split('-')[1] in ['900', '000'] or item.startswith('RHQ'))]
    #print("Unique Vessel Names: ", filtered_array)
    #print("Length of Unique Vessel Names: ", len(filtered_array))
    return len(filtered_array)


def merge_first_and_second_coupa(df_first, df_second):
    print("Merging first and second coupa...")
    new_rows = []
    uniq_vessle_count = int(get_unique_vessel_name(df_first))

    df_first['Business Date Local'] = pd.to_datetime(df_first['Business Date Local'])
    df_second['Business Date Local'] = pd.to_datetime(df_second['Business Date Local'])
    df_unique_ = df_first.drop_duplicates(subset=['Vessel', 'Business Date Local'])
    df_unique = df_unique_.sort_values(by=['Vessel', 'Business Date Local'])

    for index, row in df_unique.iterrows():
        if not row['Vessel'].startswith('RHQ') and '-' in row['Vessel'] and not row['Vessel'].split('-')[1] in ['900',
                                                                                                                '000']:
            filtered_df = df_second[df_second['Business Date Local'] == row['Business Date Local']]
            grouped_df = filtered_df.groupby(['Line Item', 'Business Date Local'], as_index=False).agg(
                {'Amount': 'sum'})
            if len(grouped_df) > 0:
                #print(f"----- İŞLEM YAPILACAK VESSEL: {row['Vessel']} | Tarih: {row['Business Date Local']} -----")
                for grouped_index, grouped_row in grouped_df.iterrows():
                    new_row = row.copy()
                    #print(f"İşlem yapılıyor; {grouped_row['Line Item']} | Amount: {int(grouped_row['Amount'])} | uniq_vessle_count: {uniq_vessle_count} gerçek amount: {int(grouped_row['Amount']) / uniq_vessle_count}")
                    new_row['Vessel'] = row['Vessel']
                    new_row['start_date'] = row['start_date']
                    new_row['end_date'] = row['end_date']
                    new_row['Line Item'] = grouped_row['Line Item']
                    new_row['Business Date Local'] = row['Business Date Local']
                    new_row['Amount'] = int(grouped_row['Amount']) / uniq_vessle_count
                    new_row['Gl Account'] = row['Gl Account']
                    new_row['Vessel Name'] = row['Vessel Name']
                    new_row['Country'] = row['Country']
                    new_rows.append(new_row)
                #print("------------------------------ İŞLEM BİTTİ ------------------------------")

    new_df = pd.DataFrame(new_rows)
    final_df = pd.concat([df_first, new_df], ignore_index=True)
    print("Merging first and second coupa DONE!")
    return final_df


def spread_rhq(final_coupa_data):
    print("Spread RHQ...")
    final_coupa_data.to_csv("data/bronze/final_coupa_data.csv")
    final_coupa_data = final_coupa_data[final_coupa_data['Country'].isin(['US', 'GB', 'AE', 'CA'])]

    us_vessels = final_coupa_data[final_coupa_data['Country'] == 'US']
    ca_vessels = final_coupa_data[final_coupa_data['Country'] == 'CA']
    gb_vessels = final_coupa_data[final_coupa_data['Country'] == 'GB']
    ae_vessels = final_coupa_data[final_coupa_data['Country'] == 'AE']

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
    filtered_df = final_coupa_data[final_coupa_data['Vessel'].str.startswith('RHQ')]
    grouped_df = filtered_df.groupby(['Vessel', 'Line Item', 'Gl Account', 'Business Date Local', 'Country'])['Amount'].sum().reset_index()
    #grouped_df.to_csv('data/bronze/grouped_df_rhq.csv')

    grouped_df['Business Date Local'] = pd.to_datetime(grouped_df['Business Date Local'])
    final_coupa_data['Business Date Local'] = pd.to_datetime(final_coupa_data['Business Date Local'])

    df_unique_ = final_coupa_data.drop_duplicates(subset=['Vessel', 'Business Date Local'])
    df_unique = df_unique_.sort_values(by=['Vessel', 'Business Date Local'])
    #df_unique.to_csv('data/bronze/df_unique.csv')

    for index, row in df_unique.iterrows():
        if not row['Vessel'].startswith('RHQ') and '-' in row['Vessel'] and not row['Vessel'].split('-')[1] in ['900',
                                                                                                                '000']:
            filtered_df = grouped_df[grouped_df['Business Date Local'] == row['Business Date Local']]
            if len(filtered_df) > 0:
                #print(f"----- İŞLEM YAPILACAK VESSEL: {row['Vessel']} | Tarih: {row['Business Date Local']} -----")
                for grouped_index, grouped_row in filtered_df.iterrows():
                    new_row = row.copy()
                    #print(f"İşlem yapılıyor; {grouped_row['Line Item']} | Amount: {int(grouped_row['Amount'])}")
                    new_row['Vessel'] = row['Vessel']
                    new_row['start_date'] = row['start_date']
                    new_row['end_date'] = row['end_date']
                    new_row['Line Item'] = grouped_row['Line Item']
                    new_row['Business Date Local'] = row['Business Date Local']
                    new_row['Gl Account'] = row['Gl Account']
                    new_row['Amount'] = int(grouped_row['Amount']) / country_vessel_counts[row['Country']]
                    new_row['Vessel Name'] = row['Vessel Name']
                    new_row['Country'] = row['Country']
                    new_rows.append(new_row)
                #print("------------------------------ İŞLEM BİTTİ ------------------------------")

    new_df = pd.DataFrame(new_rows)
    final_df = pd.concat([final_coupa_data, new_df], ignore_index=True)
    print("Spread RHQ DONE!")
    return final_df


def set_countries(df):
    first_account_type_idx = df.columns.get_loc('account_type')
    df = df.loc[:, ~((df.columns == 'account_type') & (
            df.columns.duplicated(keep='first') | (df.columns.get_loc('account_type') != first_account_type_idx)))]
    df['country'].fillna(df['account_type'], inplace=True)
    country_mapping = {
        'Kitchens (CAN)': 'CA',
        'Kitchens (US)': 'US',
        'REEF MENA': 'AE',
        'REEF UK': 'GB'
    }

    df['country'] = df['country'].replace(country_mapping)
    df = df[df['country'] != 'REEF Europe']
    df = df[df['vessel_code'] != 'Parking']
    return df


class CoupaManager:
    def __init__(self, start_date_str, end_date_str):
        self.__end_date_str = end_date_str
        self.__start_date_str = start_date_str
        self.__extractor = Extractor(self.__start_date_str, self.__end_date_str)
        self.__transformer = Transformer()

    def __rename_columns(self, df):
        column_name_mapping = {
            'vessel_code': 'Vessel',
            'account_category': 'Line Item',
            'pnl_contribution_daily_usd_rounded': 'Amount',
            'country': 'Country',
            'vessel name': 'Vessel Name',
        }
        df.rename(columns=column_name_mapping, inplace=True)
        return df

    def __select_relevant_columns(self, df):
        columns = ['vessel_code', 'expense_level', 'order_date', 'account_category',
                   'pnl_contribution_daily_usd_rounded', 'Business Date Local']
        return df[columns]

    def second_transform(self, df):
        first_account_type_idx = df.columns.get_loc('account_type')
        df = df.loc[:, ~((df.columns == 'account_type') & (df.columns.duplicated(keep='first') | (
                df.columns.get_loc('account_type') != first_account_type_idx)))]
        filtered_df = df[df['account_type'] == 'REEF Corporate']
        df_cleaned = filtered_df.dropna(subset=['account_category'])
        df_final = self.__select_relevant_columns(df_cleaned)
        renamed_df = self.__rename_columns(df_final)
        grouped_df = renamed_df.groupby(['Vessel', 'expense_level', 'order_date', 'Line Item', 'Business Date Local'],
                                        as_index=False).agg({'Amount': 'sum'})
        return grouped_df

    def start(self, local=False):
        print("coupa ETL")
        if local:
            return pd.read_csv('data/bronze/coupa.csv')

        df = self.__extractor.get_data()
        print("Extracted Coupa Data")
        df_with_countries = set_countries(df)
        print("Set Countries")

        df_first = self.__transformer.start_transform(df_with_countries)
        print("First Coupa Transform Done")
        df_second = self.second_transform(df_with_countries)
        print("Second Coupa Transform Done")
        merged_df = merge_first_and_second_coupa(df_first, df_second)
        print("Merged First and Second Coupa")

        final_df = spread_rhq(merged_df)
        print("Finished Coupa ETL")
        return final_df