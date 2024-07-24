class MappingTransformer:
    def __init__(self):
        pass

    @staticmethod
    def __map_line_items(grouped_df):
        line_item_mapping = {
            'gross_sales_usd': 'Gross Sales Usd',
            'discount_usd': 'Discount Usd',
            'refund_usd': 'Refund Usd',
            'vat_usd': 'Vat Usd',
            'net_sales_usd': 'Net Sales Usd',
            'commission_usd': 'Commission Usd',
            'royalty_usd': 'Royalty Usd'
        }
        grouped_df['line_item'] = grouped_df['line_item'].replace(line_item_mapping)
        return grouped_df

    @staticmethod
    def __rename_columns(df):
        column_name_mapping = {
            'vessel': 'Vessel',
            'vessel_name': 'Vessel Name',
            'business_date_local': 'Business Date Local',
            'country': 'Country',
            'line_item': 'Line Item',
            'amount': 'Amount'
        }
        df.rename(columns=column_name_mapping, inplace=True)
        return df

    @staticmethod
    def __add_line_order(df):
        line_order_mapping = {
            'Gross Sales Usd': 'L1-01',
            'Discount Usd': 'L1-02',
            'Refund Usd': 'L1-03',
            'Vat Usd': 'L1-04',
            'Net Sales Usd': 'L1-05',
            'Commission Usd': 'L1-06',
            'Royalty Usd': 'L1-07'
        }
        df['Line Order'] = df['Line Item'].map(line_order_mapping)
        return df

    def start_mapping(self, grouped_df):
        mapped_df = self.__map_line_items(grouped_df)
        renamed_df = self.__rename_columns(mapped_df)
        final_df = self.__add_line_order(renamed_df)
        return final_df
