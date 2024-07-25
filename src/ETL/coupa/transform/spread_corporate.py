import pandas as pd


class SpreadCorporate:
    def __init__(self):
        pass

    def __clear_dfs(self, main_df, corporate_df):
        main_df['Business Date Local'] = pd.to_datetime(main_df['Business Date Local'])
        corporate_df['Business Date Local'] = pd.to_datetime(corporate_df['Business Date Local'])
        main_unique_df = main_df.drop_duplicates(subset=['Vessel', 'Business Date Local']).sort_values(
            by=['Vessel', 'Business Date Local'])
        return main_unique_df, corporate_df

    @staticmethod
    def __get_unique_vessel_name(df):
        unique_vessel_names_list = df['Vessel'].unique().tolist()
        filtered_array = [item for item in unique_vessel_names_list if
                          '-' in item and not (item.split('-')[1] in ['900', '000'] or item.startswith('RHQ'))]
        return len(filtered_array)

    def spread_corporate(self, main_df, corporate_df):
        print("Merging first and second coupa...")
        new_rows = []
        uniq_vessle_count = int(self.__get_unique_vessel_name(main_df))

        main_unique_df, corporate_df = self.__clear_dfs(main_df, corporate_df)

        for index, row in main_unique_df.iterrows():
            if not row['Vessel'].startswith('RHQ') and '-' in row['Vessel'] and not row['Vessel'].split('-')[1] in ['900', '000']:
                corporate_filtered_df = corporate_df[corporate_df['Business Date Local'] == row['Business Date Local']]
                corporate_grouped_df = corporate_filtered_df.groupby(['Line Item', 'Business Date Local', 'Line Order'],
                                                                     as_index=False).agg({'Amount': 'sum'})
                if len(corporate_grouped_df) > 0:
                    for corporate_index, corporate_row in corporate_grouped_df.iterrows():
                        new_row = row.copy()
                        new_row['Vessel'] = row['Vessel']
                        new_row['start_date'] = row['start_date']
                        new_row['end_date'] = row['end_date']
                        new_row['Line Item'] = corporate_row['Line Item']
                        new_row['Business Date Local'] = row['Business Date Local']
                        new_row['Amount'] = int(corporate_row['Amount']) / uniq_vessle_count
                        new_row['Gl Account'] = row['Gl Account']
                        new_row['Vessel Name'] = row['Vessel Name']
                        new_row['Country'] = row['Country']
                        new_row['Line Order'] = corporate_row['Line Order']
                        new_rows.append(new_row)

        final_df = pd.concat([main_df, pd.DataFrame(new_rows)], ignore_index=True)
        print("Merging first and second coupa DONE!")
        return final_df
