import pandas as pd
from src.ETL.coupa.extract.extractor import Extractor


def start_coupa(start_date_str, end_date_str):
    extractor = Extractor(start_date_str,end_date_str)
    df_original = extractor.get_data()

    ### TRANSFORM START ###
    first_account_type_idx = df_original.columns.get_loc('account_type')
    df = df_original.loc[:, ~((df_original.columns == 'account_type') & (
                df_original.columns.duplicated(keep='first') | (
                    df_original.columns.get_loc('account_type') != first_account_type_idx)))]
    df['country'].fillna(df['account_type'], inplace=True)

    country_mapping = {
        'Kitchens (CAN)': 'CA',
        'Kitchens (US)': 'US',
        'REEF MENA': 'AE',
        'REEF UK': 'GB'
    }
    df['country'] = df['country'].replace(country_mapping)
    df = df[df['country'] != 'REEF Europe']
    df_setted_country = df[df['vessel_code'] != 'Parking']

    df = df_setted_country[df_setted_country['expense_level'] != 'Ignore']
    df = df[df['vessel_code'] != 'Overhead']
    df = df[
        df['pl_mapping_1'].isin(['L1 Expenses', 'L2 Expenses', 'L3 Expenses', 'L4 Expenses', 'Non-recurring Costs'])]
    df = df[df['account_type'] != 'REEF Corporate']

    column_name_mapping = {
        'vessel_code': 'Vessel',
        'pl_mapping_1': 'Line Item',
        'pnl_contribution_daily_usd_rounded': 'Amount',
        'country': 'Country',
        'line_item': 'Line Item',
        'amount': 'Amount',
        'gl_account': 'Gl Account',
        'vessel name': 'Vessel Name',
    }
    df.rename(columns=column_name_mapping, inplace=True)

    columns = ['Vessel', 'start_date', 'end_date', 'Line Item', 'Amount', 'Gl Account', 'Business Date Local',
               'Vessel Name', 'Country']
    main_df = df[columns]

    ### TRANSFORM END ###

    ### SPREAD CORPORATE START ###
    ### prepare corporate df

    first_account_type_idx = df_setted_country.columns.get_loc('account_type')
    corporate_df = df_setted_country.loc[:, ~((df.columns == 'account_type') & (
                df_setted_country.columns.duplicated(keep='first') | (
                    df_setted_country.columns.get_loc('account_type') != first_account_type_idx)))]
    corporate_df = corporate_df[corporate_df['account_type'] == 'REEF Corporate']
    corporate_df = corporate_df.dropna(subset=['account_category'])

    columns = ['vessel_code', 'expense_level', 'order_date', 'account_category', 'pnl_contribution_daily_usd_rounded',
               'Business Date Local']
    corporate_df = corporate_df[columns]
    column_name_mapping = {
        'vessel_code': 'Vessel',
        'account_category': 'Line Item',
        'pnl_contribution_daily_usd_rounded': 'Amount',
        'country': 'Country',
        'vessel name': 'Vessel Name',
    }
    corporate_df.rename(columns=column_name_mapping, inplace=True)
    corporate_df = corporate_df.groupby(['Vessel', 'expense_level', 'order_date', 'Line Item', 'Business Date Local'],
                                        as_index=False).agg({'Amount': 'sum'})
    mapping_dict = {
        '(-)SG&A - Profession': 'L6-01-01',
        '(-)SG&A - People Cos': 'L6-02-01',
        '(-)SG&A - Insurance': 'L6-01-02',
        '(-)SG&A - Other': 'L6-01-03',
        '(-)Other Income & Ex': 'L6-01-04',
        '(-)SG&A - T&E': 'L6-01-05',
        '(-)SG&A Application': 'L6-01-06',
        '(-)SG&A - Utility Ex': 'L6-01-07',
        '(-)SG&A - Rent': 'L6-01-08'
    }
    corporate_df['Line Order'] = corporate_df['Line Item'].map(mapping_dict)

    ### prepare corporate df end

    unique_vessel_names_list = main_df['Vessel'].unique().tolist()
    filtered_array = [item for item in unique_vessel_names_list if
                      '-' in item and not (item.split('-')[1] in ['900', '000'] or item.startswith('RHQ'))]
    uniq_vessle_count = len(filtered_array)

    main_df['Business Date Local'] = pd.to_datetime(main_df['Business Date Local'])
    corporate_df['Business Date Local'] = pd.to_datetime(corporate_df['Business Date Local'])
    main_unique_df = main_df.drop_duplicates(subset=['Vessel', 'Business Date Local']).sort_values(
        by=['Vessel', 'Business Date Local'])

    new_rows = []
    for index, row in main_unique_df.iterrows():
        if not row['Vessel'].startswith('RHQ') and '-' in row['Vessel'] and not row['Vessel'].split('-')[1] in ['900',
                                                                                                                '000']:
            corporate_filtered_df = corporate_df[corporate_df['Business Date Local'] == row['Business Date Local']]
            corporate_grouped_df = corporate_filtered_df.groupby(['Line Item', 'Business Date Local', 'Line Order'],
                                                                 as_index=False).agg({'Amount': 'sum'})
            if len(corporate_grouped_df) > 0:
                for corporate_index, corporate_row in corporate_grouped_df.iterrows():
                    new_row = row.copy()
                    new_row['Vessel'] = row['Vessel']
                    new_row['start_date'] = row['start_date']
                    new_row['end_date'] = row['end_date']
                    new_row['Line Item'] = corporate_row['Line Item']
                    new_row['Business Date Local'] = row['Business Date Local']
                    new_row['Amount'] = int(corporate_row['Amount']) / uniq_vessle_count
                    new_row['Gl Account'] = row['Gl Account']
                    new_row['Vessel Name'] = row['Vessel Name']
                    new_row['Country'] = row['Country']
                    new_row['Line Order'] = corporate_row['Line Order']
                    new_rows.append(new_row)

    corporate_spreaded_df = pd.concat([main_df, pd.DataFrame(new_rows)], ignore_index=True)
    ### SPREAD CORPORATE END ###

    ### SPREAD RHQ START ###
    mapping_df = pd.read_csv('src/static/coupa-updated-mapping-gl.csv')
    final_coupa_data = corporate_spreaded_df[corporate_spreaded_df['Country'].isin(['US', 'GB', 'AE', 'CA'])]
    us_vessels = final_coupa_data[final_coupa_data['Country'] == 'US']
    ca_vessels = final_coupa_data[final_coupa_data['Country'] == 'CA']
    gb_vessels = final_coupa_data[final_coupa_data['Country'] == 'GB']
    ae_vessels = final_coupa_data[final_coupa_data['Country'] == 'AE']

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

    filtered_df_1 = final_coupa_data[final_coupa_data['Vessel'].str.startswith('RHQ')]

    grouped_df = filtered_df_1.groupby(['Vessel', 'Line Item', 'Gl Account', 'Business Date Local', 'Country'])[
        'Amount'].sum().reset_index()
    grouped_df['Business Date Local'] = pd.to_datetime(grouped_df['Business Date Local'])

    final_coupa_data['Business Date Local'] = pd.to_datetime(final_coupa_data['Business Date Local'])
    df_unique = final_coupa_data.drop_duplicates(subset=['Vessel', 'Business Date Local']).sort_values(
        by=['Vessel', 'Business Date Local'])
    new_rows = []
    for index, row in df_unique.iterrows():
        if not row['Vessel'].startswith('RHQ') and '-' in row['Vessel'] and not row['Vessel'].split('-')[1] in ['900',
                                                                                                                '000']:
            filtered_df_2 = grouped_df[grouped_df['Business Date Local'] == row['Business Date Local']]
            mapping_dict = dict(zip(mapping_df['Gl Acount'], mapping_df['Line Order']))
            filtered_df_2['Line Order'] = filtered_df_2['Gl Account'].map(mapping_dict)

            if len(filtered_df_2) > 0:
                for grouped_index, grouped_row in filtered_df_2.iterrows():
                    new_row = row.copy()
                    new_row['Vessel'] = row['Vessel']
                    new_row['start_date'] = row['start_date']
                    new_row['end_date'] = row['end_date']
                    new_row['Line Item'] = grouped_row['Line Item']
                    new_row['Business Date Local'] = row['Business Date Local']
                    new_row['Gl Account'] = row['Gl Account']
                    new_row['Amount'] = int(grouped_row['Amount']) / country_vessel_counts[row['Country']]
                    new_row['Vessel Name'] = row['Vessel Name']
                    new_row['Country'] = row['Country']
                    new_row['Line Order'] = grouped_row['Line Order']
                    new_rows.append(new_row)

    rhq_spreaded_df = pd.concat([final_coupa_data, pd.DataFrame(new_rows)], ignore_index=True)

    coupa_df = rhq_spreaded_df[~rhq_spreaded_df['Vessel'].str.startswith('RHQ')]
    mapping_df = pd.read_csv('src/static/coupa-updated-mapping-gl.csv')
    gl_account_to_line_order = mapping_df.set_index('Gl Acount')['Line Order'].to_dict()

    coupa_df['Line Order'] = coupa_df.apply(
        lambda row: gl_account_to_line_order.get(row['Gl Account'], row['Line Order']) if pd.isna(
            row['Line Order']) else row['Line Order'], axis=1)
    #final_df = coupa_df[coupa_df['Vessel'] == 'MIA-009-07']
    coupa_df.drop(columns=['start_date', 'end_date', 'Gl Account'], inplace=True)

    ## mapping
    df_mapping = pd.read_csv('src/static/line_order_mapping.csv')

    line_order_to_item = dict(zip(df_mapping['Line Order'], df_mapping['Line Item']))

    coupa_df['Line Item'] = coupa_df['Line Order'].map(line_order_to_item).fillna(coupa_df['Line Item'])
    return coupa_df


    #coupa_df.to_csv('data/coupa.csv', index=False)


#start_coupa()
