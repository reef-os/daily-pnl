class Transformer:
    def __init__(self):
        pass

    @staticmethod
    def transform(df):
        df_final = df.drop(['monthly_standard_amount_local', 'daily_amount_local'], axis=1)
        renamed_df = df_final.rename(columns={
            'vessel code': 'Vessel',
            'country': 'Country',
            'item': 'Line Item',
            'daily_amount_usd': 'Amount',
            'business date local': 'Business Date Local',
            'vessel name': 'Vessel Name'
        })
        return renamed_df