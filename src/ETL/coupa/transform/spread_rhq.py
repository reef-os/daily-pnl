import pandas as pd


class SpreadRHQ:

    def __prepare_country_dict(self, final_coupa_data):
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
        return country_vessel_counts

    def prepare_dfs(self, final_coupa_data):
        filtered_df_1 = final_coupa_data[final_coupa_data['Vessel'].str.startswith('RHQ')]

        grouped_df = filtered_df_1.groupby(['Vessel', 'Line Item', 'Gl Account', 'Business Date Local', 'Country'])['Amount'].sum().reset_index()
        grouped_df['Business Date Local'] = pd.to_datetime(grouped_df['Business Date Local'])

        final_coupa_data['Business Date Local'] = pd.to_datetime(final_coupa_data['Business Date Local'])
        df_unique = final_coupa_data.drop_duplicates(subset=['Vessel', 'Business Date Local']).sort_values(by=['Vessel', 'Business Date Local'])
        return df_unique, grouped_df

    def __preapre_rhq(self, filtered_df_last, mapping_df):
        mapping_dict = dict(zip(mapping_df['Gl Acount'], mapping_df['Line Order']))
        filtered_df_last['Line Order'] = filtered_df_last['Gl Account'].map(mapping_dict)
        return filtered_df_last

    def spread_rhq(self, final_coupa_data):
        print("Spread RHQ...")
        mapping_df = pd.read_csv('static/coupa-updated-mapping-gl.csv')
        new_rows = []

        final_coupa_data = final_coupa_data[final_coupa_data['Country'].isin(['US', 'GB', 'AE', 'CA'])]
        country_vessel_counts = self.__prepare_country_dict(final_coupa_data)

        df_unique, grouped_df = self.prepare_dfs(final_coupa_data)
        df_unique.to_csv('df_unique.csv')
        grouped_df.to_csv('grouped_df.csv')

        for index, row in df_unique.iterrows():
            if not row['Vessel'].startswith('RHQ') and '-' in row['Vessel'] and not row['Vessel'].split('-')[1] in ['900', '000']:
                filtered_df_2 = grouped_df[grouped_df['Business Date Local'] == row['Business Date Local']]
                filtered_df_last = self.__preapre_rhq(filtered_df_2, mapping_df)
                filtered_df_last.to_csv('filtered_df_last.csv')
                if len(filtered_df_last) > 0:
                    for grouped_index, grouped_row in filtered_df_last.iterrows():
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
                        new_row['Line Order'] = grouped_row['Line Order']
                        new_rows.append(new_row)


        new_df = pd.DataFrame(new_rows)
        final_df = pd.concat([final_coupa_data, new_df], ignore_index=True)
        final_df.to_csv('final_df.csv')
        print("Spread RHQ DONE!")
        return final_df

