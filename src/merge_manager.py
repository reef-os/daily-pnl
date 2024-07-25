import pandas as pd


class MergeManager:
    def __init__(self):
        pass

    def __fill_empty_cels(self, df):
        df['Vessel Name'] = df['Vessel Name'].replace('', 'Unnamed Vessel').fillna('Unnamed Vessel')
        df['Country'] = df['Country'].replace('', 'Unknow Country').fillna('Unknow Country')
        return df

    def merge_pnl_coupa(self, df_pnl, df_coupa):
        print("--- Merging PNL and Coupa data... ---")
        new_rows = []
        df_mapping = pd.read_csv('static/gl_mapping.csv').dropna(axis=1, how='all')

        for index, row in df_coupa.iterrows():
            new_row = row.copy()
            new_row['Vessel'] = row['Vessel']
            new_row['Vessel Name'] = row['Vessel Name']
            new_row['Business Date Local'] = row['Business Date Local']
            new_row['Country'] = row['Country']
            new_row['Line Item'] = row['Line Item']
            new_row['Amount'] = row['Amount']
            new_row['is_ulysses'] = ''
            new_row['delivery_platform'] = ''
            #line_order_value = df_mapping.loc[df_mapping['Main account'] == row['Gl Account'], 'x.1'].values
            new_row['Line Order'] = row['Line Order']
            """
            if len(line_order_value) > 0:
                new_row['Line Order'] = line_order_value[0]
            else:
                new_row['Line Order'] = None
            """
            new_rows.append(new_row)

        new_df = pd.concat([df_pnl, pd.DataFrame(new_rows)], ignore_index=True)
        df_final = new_df.drop(['start_date', 'end_date', 'Gl Account'], axis=1)
        df_final = df_final.loc[:, ~df_final.columns.str.contains('^Unnamed')]
        last_df = self.__fill_empty_cels(df_final)
        print("--- Merging PNL and Coupa data DONE! ---")
        return last_df

    def merge_statement_merged_data(self, df_merged, df_statement):
        print("--- Merging Statement and Merged data... ---")
        new_rows = []
        df_final = df_merged.drop(['is_ulysses', 'delivery_platform'], axis=1)

        for index, row in df_statement.iterrows():
            new_row = row.copy()
            new_row['Vessel'] = row['Vessel']
            new_row['Vessel Name'] = ''
            new_row['Business Date Local'] = row['Business Date Local']
            new_row['Country'] = row['Country']
            new_row['Line Item'] = row['Line Item']
            new_row['Amount'] = row['Amount']
            new_row['Line Order'] = ''

            new_rows.append(new_row)

        new_df = pd.concat([df_final, pd.DataFrame(new_rows)], ignore_index=True)
        print("--- Merging Statement and Merged data DONE! ---")
        return new_df
