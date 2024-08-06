import pandas as pd
from coupa import start_coupa
from pnl_orders import start_pnl_orders
from statement import start_statement
from datetime import datetime, timedelta
from helpers import aws_manager
import warnings

warnings.filterwarnings('ignore')


def map_pl_columns(df):
    df_mapping = pd.read_csv('static/line_order_mapping.csv')
    df['pl_mapping_2'] = df['Line Order'].map(dict(zip(df_mapping['Line Order'], df_mapping['pl_mapping_2'])))
    df['pl_mapping_3'] = df['Line Order'].map(dict(zip(df_mapping['Line Order'], df_mapping['pl_mapping_3'])))
    df['pl_mapping_4'] = df['Line Order'].map(dict(zip(df_mapping['Line Order'], df_mapping['pl_mapping_4'])))
    df[['pl_mapping_2', 'pl_mapping_3', 'pl_mapping_4']] = df[['pl_mapping_2', 'pl_mapping_3', 'pl_mapping_4']].astype(
        str)
    return df


def concat_dfs(*dfs):
    result = pd.concat(dfs, ignore_index=True)
    #result = result[result['Line Item'] != 'Commission Usd']
    result = result[result['Amount'] != 0]
    return result


def multiply_amount_by_negative(df):
    df_negative = pd.read_csv('static/eksi-ile-carp.csv')
    negative_line_orders = df_negative[df_negative['Check'] == 'Yes']['Line Order'].unique()
    df.loc[df['Line Order'].isin(negative_line_orders), 'Amount'] *= -1
    return df


def update_unknown_vessels(df):
    unknown_vessels = df[df['Vessel Name'] == 'Unknown Vessel Name']
    for index, row in unknown_vessels.iterrows():
        matching_vessel = df[(df['Vessel'] == row['Vessel']) & (df['Vessel Name'] != 'Unknown Vessel Name')]
        if not matching_vessel.empty:
            df.at[index, 'Vessel Name'] = matching_vessel['Vessel Name'].values[0]
    return df


def distribute_labor_costs(df):
    df['Business Date Local'] = pd.to_datetime(df['Business Date Local']).dt.strftime('%Y-%m-%d')
    df['Business Date Local'] = pd.to_datetime(df['Business Date Local'])
    ### LABOR DAGITMA ###
    labor_mapping = pd.read_csv('static/labor_mapping.csv')
    labor_mapping[['Apr', 'May', 'Jun', 'Jul', 'Aug']] = labor_mapping[['Apr', 'May', 'Jun', 'Jul', 'Aug']].replace(',',
                                                                                                                    '',
                                                                                                                    regex=True).astype(
        float)

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
    new_rows = []
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
        elif month == 7:
            month_col = 'Jul'
        elif month == 8:
            month_col = 'Aug'
        else:
            print(f"!!! MONTH BULUNAMADI !!! month: {month} | date local: {row['Business Date Local']}", )
            return df
        if len(df_country_spesific) > 0:
            for index_spesific, row_spesific in df_country_spesific.iterrows():
                if row_spesific['Line_Item'] != 'L1 Labor':
                    new_row = row.copy()
                    new_row['Vessel'] = row['Vessel']
                    new_row['Vessel Name'] = row['Vessel Name']
                    new_row['Business Date Local'] = row['Business Date Local']
                    new_row['Country'] = row['Country']
                    new_row['Line Item'] = row_spesific['Line_Item']
                    new_row['Amount'] = int(row_spesific[month_col]) / country_vessel_counts[row['Country']]
                    new_row['Line Order'] = row_spesific['Line_Order']
                    new_rows.append(new_row)
                else:
                    if not row['is_ulysses']:
                        new_row = row.copy()
                        new_row['Vessel'] = row['Vessel']
                        new_row['Vessel Name'] = row['Vessel Name']
                        new_row['Business Date Local'] = row['Business Date Local']
                        new_row['Country'] = row['Country']
                        new_row['Line Item'] = row_spesific['Line_Item']
                        new_row['Amount'] = int(row_spesific[month_col]) / country_vessel_counts[row['Country']]
                        new_row['Line Order'] = row_spesific['Line_Order']
                        new_rows.append(new_row)

    ### GLOBAL  DAGITMA ###
    unique_vessel_names_list = df_unique['Vessel'].unique().tolist()
    filtered_array = [item for item in unique_vessel_names_list if
                      '-' in item and not (item.split('-')[1] in ['900', '000'] or item.startswith('RHQ'))]
    uniq_vessle_count = len(filtered_array)

    global_row = labor_mapping[labor_mapping['Country'] == 'Global']
    for index, row in df_unique.iterrows():
        month = row['Business Date Local'].month
        if month == 4:
            month_col = 'Apr'
        elif month == 5:
            month_col = 'May'
        elif month == 6:
            month_col = 'Jun'
        elif month == 7:
            month_col = 'Jul'
        elif month == 8:
            month_col = 'Aug'
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
    return final_df


def process_data(df):
    df['Vessel Name'] = df['Vessel Name'].replace('', 'Unknown Vessel Name').fillna('Unknown Vessel Name')
    df['Line Order'] = df['Line Order'].replace('', 'Unnamed LineOrder').fillna('Unnamed LineOrder')
    df = df[df['Line Order'] != 'Unnamed LineOrder']
    df = distribute_labor_costs(df)
    print("Labor costs distributed")
    df = update_unknown_vessels(df)
    print("Unknown vessels updated")
    df = multiply_amount_by_negative(df)
    print("Amounts multiplied by negative")
    df = map_pl_columns(df)
    print("PL columns mapped")
    df['Region'] = df['Country'].apply(map_region)
    df = distrubute_reef_commission_expense(df)
    print("Reef commission expense distributed")
    return df


def retrieve_all_data(start_date, end_date_str):
    df_coupa = start_coupa(start_date, end_date_str)
    print("len(df_coupa): ", len(df_coupa))

    df_pnl = start_pnl_orders(start_date, end_date_str)
    print("len(df_pnl): ", len(df_pnl))

    df_statement = start_statement(start_date, end_date_str)
    print("len(df_statement): ", len(df_statement))

    merged_df = concat_dfs(df_coupa, df_pnl, df_statement)
    print("len(merged_df): ", len(merged_df))

    final_df = process_data(merged_df)
    print("len(final_df): ", len(final_df))
    return final_df


def map_region(country):
    region_mapping = {
        'NA': ['US', 'CA'],
        'MENA': ['AE']
    }
    for region, countries in region_mapping.items():
        if country in countries:
            return region
    return 'EU'


def distrubute_adjustment_nisan(df):
    df_adjustment = pd.read_csv(
        '/Users/mertcelikan/PycharmProjects/daily-pnl/daily-pnl/src/static/adjustments_nisan.csv')
    start_date = datetime(2024, 4, 1)
    end_date = datetime(2024, 4, 30)
    new_rows = []
    for index, row in df_adjustment.iterrows():
        current_date = start_date
        while current_date <= end_date:
            new_row = row.copy()
            new_row['Vessel'] = 'Adjustment'
            new_row['Line Item'] = ''
            new_row['Amount'] = row['Amount'] / 30
            new_row['Business Date Local'] = current_date
            new_row['Vessel Name'] = 'Adjustment'
            new_row['Country'] = ''
            new_row['is_ulysses'] = ''
            new_row['Line Order'] = row['Line Order']
            new_row['pl_mapping_2'] = ''
            new_row['pl_mapping_3'] = ''
            new_row['pl_mapping_4'] = ''
            new_row['Region'] = row['Region']
            new_rows.append(new_row)
            current_date += timedelta(days=1)
    final_df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    return final_df


def distrubute_adjustment_mayis(df):
    df_adjustment = pd.read_csv(
        '/Users/mertcelikan/PycharmProjects/daily-pnl/daily-pnl/src/static/adjustments_mayis.csv')
    start_date = datetime(2024, 5, 1)
    end_date = datetime(2024, 5, 31)
    new_rows = []
    for index, row in df_adjustment.iterrows():
        current_date = start_date
        while current_date <= end_date:
            new_row = row.copy()
            new_row['Vessel'] = 'Adjustment'
            new_row['Line Item'] = ''
            new_row['Amount'] = row['Amount'] / 31
            new_row['Business Date Local'] = current_date
            new_row['Vessel Name'] = 'Adjustment'
            new_row['Country'] = ''
            new_row['is_ulysses'] = ''
            new_row['Line Order'] = row['Line Order']
            new_row['pl_mapping_2'] = ''
            new_row['pl_mapping_3'] = ''
            new_row['pl_mapping_4'] = ''
            new_row['Region'] = row['Region']
            new_rows.append(new_row)
            current_date += timedelta(days=1)
    final_df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    return final_df


def distrubute_reef_commission_expense(df):
    # TODO (net sales)  + (commmison usd) - (marketplace fee) + (royalty usd) - (bütün statementda charged to uklysessler)
    # NİSAN MAYISA DOKUNMA
    new_rows = []
    df_ulysses = df[df['is_ulysses']]
    df_unique_ = df_ulysses.drop_duplicates(subset=['Vessel', 'Business Date Local'])
    df_unique = df_unique_.sort_values(by=['Vessel', 'Business Date Local'])

    for index, row in df_unique.iterrows():
        df_filtered = df_ulysses[
            (df_ulysses['Business Date Local'] == row['Business Date Local']) & (df_ulysses['Vessel'] == row['Vessel'])]

        net_sales_amount = df_filtered[df_filtered['Line Order'] == 'L1-05']['Amount'].sum() if not df_filtered[
            df_filtered['Line Order'] == 'L1-05'].empty else 0
        commission_usd_amount = df_filtered[df_filtered['Line Order'] == 'L1-08']['Amount'].sum() if not df_filtered[
            df_filtered['Line Order'] == 'L1-08'].empty else 0
        marketplace_fee_amount = df_filtered[df_filtered['Line Order'] == 'L1-06']['Amount'].sum() if not df_filtered[
            df_filtered['Line Order'] == 'L1-06'].empty else 0
        royalty_usd_amount = df_filtered[df_filtered['Line Order'] == 'L1-11-01']['Amount'].sum() if not df_filtered[
            df_filtered['Line Order'] == 'L1-11-01'].empty else 0

        charged_to_ulysses = df_filtered[df_filtered['pl_mapping_3'] == '(+)charges to Ulysses']['Amount'].sum()
        new_amount = net_sales_amount + commission_usd_amount - marketplace_fee_amount + royalty_usd_amount - charged_to_ulysses
        new_row = row.copy()
        new_row['Line Item'] = "Reef Commission Expense"
        new_row['Amount'] = new_amount * -1
        new_row['Line Order'] = 'L1-15'
        new_rows.append(new_row)
    final_df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)

    return final_df


if __name__ == "__main__":
    yesterday = datetime.today() - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    print("Started: ", yesterday_str)

    aws_manager = aws_manager.AWSManager()
    df = retrieve_all_data(yesterday_str,yesterday_str)
    aws_manager.insert_to_redshift(df)

    """
    ### ADJUSMENTS ###
    df_nisan = pd.read_csv('/Users/mertcelikan/PycharmProjects/daily-pnl/daily-pnl/src/nisan.csv')
    df_mayis = pd.read_csv('/Users/mertcelikan/PycharmProjects/daily-pnl/daily-pnl/src/mayis.csv')
    nisan_mayis_result = pd.concat([df_nisan, df_mayis], ignore_index=True)
    nisan_mayis_result['Region'] = nisan_mayis_result['Country'].apply(map_region)

    nisan_adjustment_mayis_result = distrubute_adjustment_nisan(nisan_mayis_result)
    nisan_adjustment_mayis_adjustment_result = distrubute_adjustment_mayis(nisan_adjustment_mayis_result)
    nisan_adjustment_mayis_adjustment_result = nisan_adjustment_mayis_adjustment_result[nisan_adjustment_mayis_adjustment_result['Amount'] != 0]
    nisan_adjustment_mayis_adjustment_result = nisan_adjustment_mayis_adjustment_result.drop(['Month'], axis=1)
    nisan_adjustment_mayis_adjustment_result.loc[nisan_adjustment_mayis_adjustment_result['Region'].isnull(), 'Region'] = 'NA'
    nisan_adjustment_mayis_adjustment_result['Business Date Local'] = pd.to_datetime(nisan_adjustment_mayis_adjustment_result['Business Date Local'])
    nisan_adjustment_mayis_adjustment_result['Business Date Local'] = nisan_adjustment_mayis_adjustment_result['Business Date Local'].dt.strftime('%Y-%m-%d')
    nisan_adjustment_mayis_adjustment_result.to_csv('nisan_mayis_adjusted.csv', index=False)
    """