import numpy as np
import pandas as pd
from src.ETL.coupa.transform.transformer import Transformer
from src.ETL.coupa.transform.second_transform import SecondTransformer
from src.ETL.coupa.transform.coupa_merge import CoupaMerger
from src.ETL.coupa.extract.extractor import Extractor


class CoupaManager:
    def __init__(self, start_date_str, end_date_str):
        self.__end_date_str = end_date_str
        self.__start_date_str = start_date_str
        self.__extractor = Extractor(self.__start_date_str, self.__end_date_str)
        self.__transformer = Transformer()
        self.__second_transformer = SecondTransformer()
        self.__coupa_merger = CoupaMerger()

    def __spread_rhq(self, final_coupa_data):
        print("Spread RHQ...")
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
        grouped_df = filtered_df.groupby(['Vessel', 'Line Item', 'Gl Account', 'Business Date Local', 'Country'])[
            'Amount'].sum().reset_index()

        grouped_df['Business Date Local'] = pd.to_datetime(grouped_df['Business Date Local'])
        final_coupa_data['Business Date Local'] = pd.to_datetime(final_coupa_data['Business Date Local'])

        df_unique_ = final_coupa_data.drop_duplicates(subset=['Vessel', 'Business Date Local'])
        df_unique = df_unique_.sort_values(by=['Vessel', 'Business Date Local'])

        for index, row in df_unique.iterrows():
            if not row['Vessel'].startswith('RHQ') and '-' in row['Vessel'] and not row['Vessel'].split('-')[1] in [
                '900',
                '000']:
                filtered_df = grouped_df[grouped_df['Business Date Local'] == row['Business Date Local']]
                if len(filtered_df) > 0:
                    for grouped_index, grouped_row in filtered_df.iterrows():
                        new_row = row.copy()
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

        new_df = pd.DataFrame(new_rows)
        final_df = pd.concat([final_coupa_data, new_df], ignore_index=True)
        print("Spread RHQ DONE!")
        return final_df

    def __set_countries(self, df):
        print("Setting countries...")
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
        print("Setting countries DONE!")
        return df

    def start(self, local=False):
        if local:
            return pd.read_csv('data/bronze/coupa.csv')
        print("--- Starting Coupa ETL... ---")
        df = self.__extractor.get_data()
        df.to_csv("data/bronze/1_ilk_data.csv")

        df_with_countries = self.__set_countries(df)
        df_with_countries.to_csv("data/bronze/2_countries_eslesmesi.csv")

        df_first = self.__transformer.start_transform(df_with_countries)
        df_first.to_csv("data/bronze/3_transformed_data.csv")

        df_second = self.__second_transformer.start_transform(df_with_countries)
        df_second.to_csv("data/bronze/4_second_transformed_data.csv")

        merged_df = self.__coupa_merger.start_merge(df_first, df_second)
        merged_df.to_csv("data/silver/5_coupa_merged_data.csv")

        final_df = self.__spread_rhq(merged_df)
        final_df.to_csv("data/silver/6_spread_rhq_data.csv")
        print("--- Finished Coupa ETL! ---")
        return final_df
