import numpy as np
import pandas as pd
from datetime import datetime, timedelta


def date_by_date_vessel_unique_count(df, is_ulysses=None):
    country_vessel_counts_by_date = {}

    # is_ulysses parametresine göre filtreleme yap
    if is_ulysses is not None:
        df = df[df['is_ulysses'] == is_ulysses]

    for country in ['US', 'CA', 'GB', 'AE']:
        country_df = df[df['Country'] == country]
        vessel_counts_by_date = country_df.groupby('Business Date Local')['Vessel'].nunique().to_dict()
        country_vessel_counts_by_date[country] = vessel_counts_by_date

    return country_vessel_counts_by_date


def distribute_labor_costs(df):
    df['Business Date Local'] = pd.to_datetime(df['Business Date Local']).dt.strftime('%Y-%m-%d')
    df['Business Date Local'] = pd.to_datetime(df['Business Date Local'])
    ### LABOR DAGITMA ###
    labor_mapping = pd.read_csv('static/labor_mapping.csv')
    labor_mapping[['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep']] = labor_mapping[['Apr', 'May', 'Jun', 'Jul', 'Aug','Sep']].replace(',','',regex=True).astype(float)
    country_vessel_counts_by_date = date_by_date_vessel_unique_count(df)
    ulysses_false_counts_by_date = date_by_date_vessel_unique_count(df, is_ulysses=False)


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
        elif month == 9:
            month_col = 'Sep'
        else:
            print(f"!!! MONTH BULUNAMADI !!! month: {month} | date local: {row['Business Date Local']}", )
            return df
        if len(df_country_spesific) > 0:
            for index_spesific, row_spesific in df_country_spesific.iterrows():
                if row_spesific['Line_Item'] != 'L1 Labor':
                    if float(row_spesific[month_col]) == 0:
                        continue
                    # L1 DIŞINDAKİLERİ DAĞITMA KISMI
                    new_row = row.copy()
                    new_row['Vessel'] = row['Vessel']
                    new_row['Vessel Name'] = row['Vessel Name']
                    new_row['Business Date Local'] = row['Business Date Local']
                    new_row['Country'] = row['Country']
                    new_row['Line Item'] = row_spesific['Line_Item']

                    country = row['Country']
                    business_date = row['Business Date Local']
                    vessel_count = country_vessel_counts_by_date.get(country, {}).get(business_date, 1)
                    new_row['Amount'] = int(row_spesific[month_col]) / vessel_count
                    new_row['Line Order'] = row_spesific['Line_Order']
                    new_rows.append(new_row)


                else:
                    # L1 DAGITMA KISMI
                    if not row['is_ulysses']:
                        # ULYSESS OLMAYANLARA DAGITIYORUZ
                        new_row = row.copy()
                        new_row['Vessel'] = row['Vessel']
                        new_row['Vessel Name'] = row['Vessel Name']
                        new_row['Business Date Local'] = row['Business Date Local']
                        new_row['Country'] = row['Country']
                        new_row['Line Item'] = row_spesific['Line_Item']
                        vessel_count = ulysses_false_counts_by_date.get(row['Country'], {}).get(
                            row['Business Date Local'], 1)

                        new_row['Amount'] = int(row_spesific[month_col]) / vessel_count
                        new_row['Line Order'] = row_spesific['Line_Order']
                        new_rows.append(new_row)

    ### GLOBAL  DAGITMA ###

    vessel_counts_by_date = {}

    # Tüm unique vessel isimlerini filtreleme koşuluna göre listele
    for date, group in df.groupby('Business Date Local'):
        unique_vessel_names_list = group['Vessel'].unique().tolist()
        vessel_counts_by_date[date] = len(unique_vessel_names_list)

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
        elif month == 9:
            month_col = 'Sep'
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

            business_date = row['Business Date Local']
            vessel_count = vessel_counts_by_date.get(business_date, 1)

            new_row['Amount'] = float(global_row[month_col].iloc[0] / vessel_count)
            new_row['Line Order'] = global_row['Line_Order'].iloc[0]
            new_rows.append(new_row)
    new_df = pd.DataFrame(new_rows)
    final_df = pd.concat([df, new_df], ignore_index=True)
    print("Labor costs distributed")
    return final_df


def distrubute_adjustment_nisan(df):
    df_adjustment = pd.read_csv(
        '/Users/mertcelikan/PycharmProjects/daily-pnl/daily-pnl/src/Apr Adj Cleaned.csv')
    start_date = datetime(2024, 6, 1)
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
        '/Users/mertcelikan/PycharmProjects/daily-pnl/daily-pnl/src/static/May Adj Cleaned.csv')
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

        charged_to_ulysses = df_filtered[df_filtered['pl_mapping_3'] == '(+)charges to Ulysses']['Amount'].sum() if not df_filtered[
            df_filtered['pl_mapping_3'] == '(+)charges to Ulysses'].empty else 0

        #new_amount = net_sales_amount + commission_usd_amount - marketplace_fee_amount - (net_sales_amount * 0.05) - charged_to_ulysses
        new_amount = net_sales_amount + commission_usd_amount - marketplace_fee_amount - charged_to_ulysses
        new_row = row.copy()
        new_row['Line Item'] = "L1 Expenses"
        new_row['Amount'] = new_amount * -1
        new_row['Line Order'] = 'L1-15'
        new_row['pl_mapping_2'] = '(-)Reef Commission Expense'
        new_row['pl_mapping_3'] = 'nan'
        new_row['pl_mapping_4'] = 'nan'
        new_rows.append(new_row)
        #if net_sales_amount != 0:
            #print(f"Vessel: {row['Vessel']} | Date: {row['Business Date Local']} | new_row['Amount']: {new_row['Amount']} | net_sales_amount: {net_sales_amount} | commission_usd_amount: {commission_usd_amount} | marketplace_fee_amount: {marketplace_fee_amount} | charged_to_ulysses: {charged_to_ulysses}")
    final_df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    print("Reef commission expense distributed")
    return final_df
