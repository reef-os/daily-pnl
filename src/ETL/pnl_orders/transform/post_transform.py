import pandas as pd


class PostTransformer:
    def __init__(self):
        pass

    @staticmethod
    def apply_marketplace_fee(df):
        filtered_df = df[(df['is_ulysses'] == True) & (df['Line Item'] == 'Net Sales Usd')]
        new_rows = []
        for index, row in filtered_df.iterrows():
            new_row = row.copy()
            new_row['Line Item'] = '1.Marketplace Fee'
            new_row['Amount'] = row['Amount'] * 0.10
            new_rows.append(new_row)

        new_df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
        return new_df

    @staticmethod
    def apply_cwa_fee(df):
        filtered_df = df[
            df['delivery_platform'].isin(['2nd Kitchen', 'CWA', 'PointOfSale', 'Reef UK', 'Consumer Web App']) & (
                    df['Line Item'] == 'Net Sales Usd')]
        new_rows = []
        for index, row in filtered_df.iterrows():
            new_row = row.copy()
            new_row['Line Item'] = '2.CWA Fee, net'
            new_row['Amount'] = row['Amount'] * 0.075
            new_rows.append(new_row)

        new_df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
        return new_df

    @staticmethod
    def apply_food_purchases(df):
        filtered_df = df[(df['Line Item'] == 'Net Sales Usd')]
        new_rows = []
        for index, row in filtered_df.iterrows():
            new_row = row.copy()
            new_row['Line Item'] = '(-)Food Purchases'
            new_row['Line Order'] = 'L3-01-01'
            new_row['Amount'] = row['Amount'] * 0.30
            new_rows.append(new_row)
        new_df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
        return new_df

    @staticmethod
    def apply_l3_expenses(df):
        # 30 tane net sales var ise, 30 tanede food purchase olucak, 30 tanede l3 expenses olucak.
        filtered_df = df[(df['Line Item'] == '(-)Food Purchases')]
        new_rows = []
        for index, row in filtered_df.iterrows():
            new_row = row.copy()
            new_row['Line Item'] = 'Food Charged L3'
            new_row['Line Order'] = 'L3-01-06'
            new_row['Amount'] = row['Amount']  * 1.10
            new_rows.append(new_row)
        new_df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
        return new_df

    def start_post_transform(self, df):
        df_fee = self.apply_marketplace_fee(df)
        df_cwa = self.apply_cwa_fee(df_fee)
        df_food = self.apply_food_purchases(df_cwa)
        df_final = self.apply_l3_expenses(df_food)
        return df_final
