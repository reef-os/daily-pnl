import pandas as pd


class LaborMapping:
    def __init__(self):
        pass

    def __preapare_final_df(self, final_df):
        final_df['Vessel Name'] = final_df['Vessel Name'].replace('', 'Unnamed Vessel').fillna('Unnamed Vessel')
        final_df['Line Order'] = final_df['Line Order'].replace('', 'Unnamed LineOrder').fillna('Unnamed LineOrder')

        final_df['Business Date Local'] = pd.to_datetime(final_df['Business Date Local']).dt.strftime('%Y-%m-%d')
        return final_df

    def __preapre_labor_mapping(self):
        df = pd.read_csv('static/labor_mapping.csv')
        df['Apr'] = df['Apr'].str.replace(',', '').astype(float)
        df['May'] = df['May'].str.replace(',', '').astype(float)
        df['Jun'] = df['Jun'].str.replace(',', '').astype(float)
        return df

    def __get_unique_vessel_name(self, df):
        unique_vessel_names_list = df['Vessel'].unique().tolist()
        filtered_array = [item for item in unique_vessel_names_list if
                          '-' in item and not (item.split('-')[1] in ['900', '000'] or item.startswith('RHQ'))]
        return len(filtered_array)

    def start(self, df):
        new_rows = []
        df = self.__preapare_final_df(df)
        labor_mapping = self.__preapre_labor_mapping()
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

        uniq_vessle_count = int(self.__get_unique_vessel_name(df_unique))
        #TODO vessel countlar sıkıntılı
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
        print("Merging first and second coupa DONE!")
        return final_df
