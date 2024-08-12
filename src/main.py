import pandas as pd
from coupa import start_coupa
from pnl_orders import start_pnl_orders
from statement import start_statement
from datetime import datetime, timedelta
from helpers import aws_manager
from middle_transform import start_transform
from statement_exception import apply_statement_exceptions
from distribute import distribute_labor_costs, distrubute_reef_commission_expense, distrubute_adjustment_mayis, \
    distrubute_adjustment_nisan
import warnings

warnings.filterwarnings('ignore')


def add_new_line_item(df):
    new_rows = []
    df_filtered = df[(df['is_ulysses'] == True) & (df['Line Item'] == 'Net Sales Usd')]
    df_unique = df_filtered.drop_duplicates(subset=['Vessel', 'Business Date Local']).sort_values(by=['Vessel', 'Business Date Local'])
    for index, row in df_unique.iterrows():
        new_row = row.copy()
        new_row['Line Item'] = 'L1 Expenses'
        new_row['Line Order'] = 'L1-11-02'
        new_row['Amount'] = row['Amount'] * 0.05
        new_rows.append(new_row)
    return pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)


def process_data(df):
    df['Vessel Name'] = df['Vessel Name'].replace('', 'Unknown Vessel Name').fillna('Unknown Vessel Name')
    df['Line Order'] = df['Line Order'].replace('', 'Unnamed LineOrder').fillna('Unnamed LineOrder')
    df = df[df['Line Order'] != 'Unnamed LineOrder']
    df = distribute_labor_costs(df)
    df = start_transform(df) # pl colmuns maplendi




    df = distrubute_reef_commission_expense(df)
    df = apply_statement_exceptions(df)
    df = df.groupby(['Vessel', 'Line Item', 'Business Date Local', 'Vessel Name', 'Country', 'is_ulysses', 'Line Order',
                     'pl_mapping_2', 'pl_mapping_3', 'pl_mapping_4', 'Region']).sum().reset_index()
    df = add_new_line_item(df)
    return df


def concat_dfs(*dfs):
    result = pd.concat(dfs, ignore_index=True)
    #result = result[result['Line Item'] != 'Commission Usd']
    result = result[result['Amount'] != 0]
    return result


def retrieve_all_data(start_date, end_date_str):
    df_pnl = start_pnl_orders(start_date, end_date_str)
    print("len(df_pnl): ", len(df_pnl))

    df_coupa = start_coupa(start_date, end_date_str)
    print("len(df_coupa): ", len(df_coupa))

    df_statement = start_statement(start_date, end_date_str)
    print("len(df_statement): ", len(df_statement))

    merged_df = concat_dfs(df_coupa, df_pnl, df_statement)
    print("len(merged_df): ", len(merged_df))

    final_df = process_data(merged_df)
    print("len(final_df): ", len(final_df))
    return final_df


if __name__ == "__main__":
    yesterday = datetime.today() - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    #print("Started: ", yesterday_str)

    aws_manager = aws_manager.AWSManager()

    df_nisan = retrieve_all_data("2024-04-01", "2024-04-30")
    df_nisan.to_csv('data/latest.csv', index=False)
    """
    aws_manager.insert_to_redshift(df_nisan)

    df_mayis = retrieve_all_data("2024-05-01", "2024-05-31")
    df_mayis.to_csv('data/mayis.csv', index=False)
    aws_manager.insert_to_redshift(df_mayis)

    df_haziran = retrieve_all_data("2024-06-01", "2024-06-30")
    df_haziran.to_csv('data/haziran.csv', index=False)
    aws_manager.insert_to_redshift(df_haziran)

    df_temmuz = retrieve_all_data("2024-07-01", "2024-07-31")
    df_temmuz.to_csv('data/temmuz.csv', index=False)
    aws_manager.insert_to_redshift(df_temmuz)

    df_agustos = retrieve_all_data("2024-08-01", "2024-08-11")
    df_agustos.to_csv('data/agustos.csv', index=False)
    aws_manager.insert_to_redshift(df_agustos)
    """


    """
    ### ADJUSMENTS ###
    df_nisan = pd.read_csv('/Users/mertcelikan/PycharmProjects/daily-pnl/daily-pnl/src/haziran.csv')
    df_mayis = pd.read_csv('/Users/mertcelikan/PycharmProjects/daily-pnl/daily-pnl/src/haziran.csv')
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
