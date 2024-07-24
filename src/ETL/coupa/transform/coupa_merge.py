import pandas as pd


class CoupaMerger:
    def __init__(self):
        pass

    @staticmethod
    def __get_unique_vessel_name(df):
        unique_vessel_names_list = df['Vessel'].unique().tolist()
        filtered_array = [item for item in unique_vessel_names_list if
                          '-' in item and not (item.split('-')[1] in ['900', '000'] or item.startswith('RHQ'))]
        return len(filtered_array)

    def start_merge(self, df_first, df_second):
        print("Merging first and second coupa...")
        new_rows = []
        uniq_vessle_count = int(self.__get_unique_vessel_name(df_first))

        df_first['Business Date Local'] = pd.to_datetime(df_first['Business Date Local'])
        df_second['Business Date Local'] = pd.to_datetime(df_second['Business Date Local'])
        df_unique_ = df_first.drop_duplicates(subset=['Vessel', 'Business Date Local'])
        df_unique = df_unique_.sort_values(by=['Vessel', 'Business Date Local'])

        for index, row in df_unique.iterrows():
            if not row['Vessel'].startswith('RHQ') and '-' in row['Vessel'] and not row['Vessel'].split('-')[1] in [
                '900',
                '000']:
                filtered_df = df_second[df_second['Business Date Local'] == row['Business Date Local']]
                grouped_df = filtered_df.groupby(['Line Item', 'Business Date Local'], as_index=False).agg(
                    {'Amount': 'sum'})
                if len(grouped_df) > 0:
                    for grouped_index, grouped_row in grouped_df.iterrows():
                        new_row = row.copy()
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

        new_df = pd.DataFrame(new_rows)
        final_df = pd.concat([df_first, new_df], ignore_index=True)
        print("Merging first and second coupa DONE!")
        return final_df
