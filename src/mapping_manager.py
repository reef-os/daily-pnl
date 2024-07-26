import pandas as pd

from src.mapping.labor_mapping import LaborMapping
from src.mapping.line_order_mapping import LineOrderMapping
from src.mapping.multiply_negative_mapping import MultiplyNegativeMapping


class MappingManager:
    def __init__(self):
        self.__labor_mapping = LaborMapping()
        self.__line_order_mapping = LineOrderMapping()
        self.__multiply_negative_mapping = MultiplyNegativeMapping()

    def transform(self, df):
        df = df[df['Line Item'] != 'Commission Usd']
        df = df[df['Amount'] != 0]
        #df = df[df['Line Order'] != 'Unnamed LineOrder']
        return df

    def pl_mapping(self, df):
        line_order_mapping_df = pd.read_csv('static/line_order_mapping.csv')
        """
        line_item_dict = dict(zip(line_order_mapping_df['Line Order'], line_order_mapping_df['Line Item']))
        pl_mapping_2_dict = dict(zip(line_order_mapping_df['Line Order'], line_order_mapping_df['pl_mapping_2']))
        pl_mapping_3_dict = dict(zip(line_order_mapping_df['Line Order'], line_order_mapping_df['pl_mapping_3']))
        pl_mapping_4_dict = dict(zip(line_order_mapping_df['Line Order'], line_order_mapping_df['pl_mapping_4']))

        df['Line Item'] = df['Line Order'].map(line_item_dict)
        df['pl_mapping_2'] = df['Line Order'].map(pl_mapping_2_dict)
        df['pl_mapping_3'] = df['Line Order'].map(pl_mapping_3_dict)
        df['pl_mapping_4'] = df['Line Order'].map(pl_mapping_4_dict)
        """
        df['pl_mapping_2'] = ''
        df['pl_mapping_3'] = ''
        df['pl_mapping_4'] = ''

        for index, row in df.iterrows():
            mapping_row = line_order_mapping_df[line_order_mapping_df['Line Order'] == row['Line Order']]
            if not mapping_row.empty:
                df.at[index, 'Line Item'] = mapping_row['Line Item'].iloc[0]
                df.at[index, 'pl_mapping_2'] = mapping_row['pl_mapping_2'].iloc[0]
                df.at[index, 'pl_mapping_3'] = mapping_row['pl_mapping_3'].iloc[0]
                df.at[index, 'pl_mapping_4'] = mapping_row['pl_mapping_4'].iloc[0]
            """
            elif not mapping_row.empty and row['Line Item'] == 'Waste Management':
                df.at[index, 'Line Item'] = row['Line Item']
                df.at[index, 'pl_mapping_2'] = row['pl_mapping_2']
                df.at[index, 'pl_mapping_3'] = row['pl_mapping_3']
                df.at[index, 'pl_mapping_4'] = row['pl_mapping_4']
            """

        return df

    def find_vessels_name(self, df):
        unknown_vessels = df[df['Vessel Name'] == 'Unknown Vessel Name']
        for index, row in unknown_vessels.iterrows():
            matching_vessel = df[(df['Vessel'] == row['Vessel']) & (df['Vessel Name'] != 'Unknown Vessel Name')]
            if not matching_vessel.empty:
                df.at[index, 'Vessel Name'] = matching_vessel['Vessel Name'].values[0]
        return df

    def start(self, df):
        print("--- MappingManager starting... ---")
        df = self.__labor_mapping.start(df)
        df = self.__line_order_mapping.map_line_order_and_line_item(df)
        df = self.__multiply_negative_mapping.eksi_bir_carp(df)
        df = self.transform(df)
        df = self.pl_mapping(df)
        df = self.find_vessels_name(df)
        print("--- MappingManager finished. ---")
        return df
