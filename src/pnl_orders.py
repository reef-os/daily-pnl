import pandas as pd
from src.helpers.db_reader import DbReader


def apply_marketplace_fee(df):
    filtered_df = df[(df['is_ulysses'] == True) & (df['Line Item'] == 'Net Sales Usd')]
    new_rows = []
    for index, row in filtered_df.iterrows():
        new_row = row.copy()
        new_row['Line Item'] = '1.Marketplace Fee'
        new_row['Line Order'] = 'L1-06'
        new_row['Amount'] = row['Amount'] * 0.10
        new_rows.append(new_row)

    new_df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    return new_df


def apply_cwa_fee(df):
    filtered_df = df[
        df['delivery_platform'].isin(['2nd Kitchen', 'CWA', 'PointOfSale', 'Reef UK', 'Consumer Web App']) & (
                df['Line Item'] == 'Net Sales Usd')]
    new_rows = []
    for index, row in filtered_df.iterrows():
        new_row = row.copy()
        new_row['Line Item'] = '2.CWA Fee, net'
        new_row['Line Order'] = 'L1-07'
        new_row['Amount'] = row['Amount'] * 0.075
        new_rows.append(new_row)

    new_df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    return new_df


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


def apply_l3_expenses(df):
    # 30 tane net sales var ise, 30 tanede food purchase olucak, 30 tanede l3 expenses olucak.
    filtered_df = df[(df['Line Item'] == '(-)Food Purchases')]
    new_rows = []
    for index, row in filtered_df.iterrows():
        new_row = row.copy()
        new_row['Line Item'] = 'Food Charged L3'
        new_row['Line Order'] = 'L3-01-06'
        new_row['Amount'] = row['Amount'] * 1.10
        new_rows.append(new_row)
    new_df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    return new_df


def start_pnl_orders(start_date_str, end_date_str):
    db_reader = DbReader()
    df = db_reader.get_data("pnl_orders",start_date_str, end_date_str)
    df = df[['vessel', 'vessel_name', 'business_date_local', 'country', 'gross_sales_usd', 'discount_usd', 'refund_usd',
             'vat_usd', 'net_sales_usd', 'commission_usd', 'royalty_usd', 'is_ulysses', 'delivery_platform']]

    melted_df = df.melt(
        id_vars=['vessel', 'vessel_name', 'business_date_local', 'country', 'is_ulysses', 'delivery_platform'],
        value_vars=['gross_sales_usd', 'discount_usd', 'refund_usd', 'vat_usd', 'net_sales_usd', 'commission_usd',
                    'royalty_usd'], var_name='line_item', value_name='amount')
    melted_df['vessel_name'].fillna('Unknown Vessel Name', inplace=True)
    grouped_df = melted_df.assign(vessel_name=melted_df['vessel_name'].fillna('Unknown Vessel Name')).groupby(
        ['vessel', 'vessel_name', 'business_date_local', 'country', 'line_item', 'is_ulysses', 'delivery_platform'],
        as_index=False).sum()
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

    column_name_mapping = {
        'vessel': 'Vessel',
        'vessel_name': 'Vessel Name',
        'business_date_local': 'Business Date Local',
        'country': 'Country',
        'line_item': 'Line Item',
        'amount': 'Amount'
    }
    grouped_df.rename(columns=column_name_mapping, inplace=True)

    line_order_mapping = {
        'Gross Sales Usd': 'L1-01',
        'Discount Usd': 'L1-02',
        'Refund Usd': 'L1-03',
        'Vat Usd': 'L1-04',
        'Net Sales Usd': 'L1-05',
        'Commission Usd': 'L1-06',
        'Royalty Usd': 'L1-11-01'
    }
    grouped_df['Line Order'] = grouped_df['Line Item'].map(line_order_mapping)

    df_fee = apply_marketplace_fee(grouped_df)
    df_cwa = apply_cwa_fee(df_fee)
    df_food = apply_food_purchases(df_cwa)
    df_final = apply_l3_expenses(df_food)
    df_final.drop(columns=['is_ulysses', 'delivery_platform'], inplace=True)
    #df_final = df_final[df_final['Vessel'] == 'MIA-009-07']

    #df_final.to_csv('data/pnl_orders.csv', index=False)
    return df_final


#start_pnl_orders()