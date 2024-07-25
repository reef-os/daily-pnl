import numpy as np
import pandas as pd
from src.ETL.coupa.transform.transformer import Transformer
from src.ETL.coupa.extract.extractor import Extractor

from src.ETL.coupa.transform.prepare_corporate import PrepareCorporate
from src.ETL.coupa.transform.spread_corporate import SpreadCorporate

from src.ETL.coupa.transform.spread_rhq import SpreadRHQ


class CoupaManager:
    def __init__(self, start_date_str, end_date_str):
        self.__end_date_str = end_date_str
        self.__start_date_str = start_date_str
        self.__extractor = Extractor(self.__start_date_str, self.__end_date_str)
        self.__transformer = Transformer()

        self.__prepare_corporate = PrepareCorporate()
        self.__spread_corporate = SpreadCorporate()

        self.__spread_rhq = SpreadRHQ()

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

    def final_mapping(self, coupa_df_2):
        coupa_df = coupa_df_2[~coupa_df_2['Vessel'].str.startswith('RHQ')]
        mapping_df = pd.read_csv('static/coupa-updated-mapping-gl.csv')
        gl_account_to_line_order = mapping_df.set_index('Gl Acount')['Line Order'].to_dict()

        coupa_df['Line Order'] = coupa_df.apply(
            lambda row: gl_account_to_line_order.get(row['Gl Account'], row['Line Order']) if pd.isna(
                row['Line Order']) else row['Line Order'],
            axis=1
        )
        return coupa_df

    def start(self, local=False):
        if local:
            return pd.read_csv('data/bronze/coupa.csv')
        print("--- Starting Coupa ETL... ---")
        df = self.__extractor.get_data()
        df = self.__set_countries(df)
        main_df = self.__transformer.start_transform(df)

        corporate_df = self.__prepare_corporate.start_transform(df)
        corporate_spreaded_df = self.__spread_corporate.spread_corporate(main_df, corporate_df)

        rhq_spreaded_df = self.__spread_rhq.spread_rhq(corporate_spreaded_df)

        final_df = self.final_mapping(rhq_spreaded_df)
        print("--- Finished Coupa ETL! ---")
        return final_df
