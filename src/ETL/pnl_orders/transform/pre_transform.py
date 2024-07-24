class PreTransformer:
    def __init__(self):
        pass

    @staticmethod
    def __filter_columns(pn_orders_df):
        return pn_orders_df[
            ['vessel', 'vessel_name', 'business_date_local', 'country', 'gross_sales_usd', 'discount_usd', 'refund_usd',
             'vat_usd', 'net_sales_usd', 'commission_usd', 'royalty_usd', 'is_ulysses', 'delivery_platform']]

    @staticmethod
    def __melt_data(df_filtered):
        return df_filtered.melt(
            id_vars=['vessel', 'vessel_name', 'business_date_local', 'country', 'is_ulysses', 'delivery_platform'],
            value_vars=['gross_sales_usd', 'discount_usd', 'refund_usd', 'vat_usd', 'net_sales_usd',
                        'commission_usd', 'royalty_usd'],
            var_name='line_item',
            value_name='amount')

    @staticmethod
    def __group_data(melted_df):
        return melted_df.groupby(
            ['vessel', 'vessel_name', 'business_date_local', 'country', 'line_item', 'is_ulysses', 'delivery_platform'],
            as_index=False).sum()

    def start_pre_transform(self, pn_orders_df):
        df_filtered = self.__filter_columns(pn_orders_df)
        melted_df = self.__melt_data(df_filtered)
        grouped_df = self.__group_data(melted_df)
        return grouped_df
