import pandas as pd
from extract.coupa_extractor import Extractor


def clear_duplicate_rows(df):
    df = df.drop_duplicates()
    df = df.sort_values(by='is_ulysses', ascending=False)
    df = df.drop_duplicates(subset=df.columns.difference(['is_ulysses', 'vessel name']), keep='first')
    return df


def clear_data(df):
    df['vessel_code'] = df['vessel_code'].astype(str)
    #df = df[~df['vessel_code'].str.startswith('NYC')]
    first_account_type_idx = df.columns.get_loc('account_type')
    df = df.loc[:, ~((df.columns == 'account_type') & (
            df.columns.duplicated(keep='first') | (df.columns.get_loc('account_type') != first_account_type_idx)))]
    df['country'].fillna(df['account_type'], inplace=True)

    country_mapping = {
        'Kitchens (CAN)': 'CA',
        'Kitchens (US)': 'US',
        'REEF MENA': 'AE',
        'REEF UK': 'GB'
    }
    df['country'] = df['country'].replace(country_mapping)
    #df = df[df['country'] != 'REEF Europe']
    df_setted_country = df[df['vessel_code'] != 'Parking']

    df = df[
        df['pl_mapping_1'].isin(['L1 Expenses', 'L2 Expenses', 'L3 Expenses', 'L4 Expenses', 'Non-recurring Costs'])]
    df = df[df['account_type'] != 'REEF Corporate']
    return df, df_setted_country


def clear_columns(df):
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
    columns = ['Vessel', 'Line Item', 'Amount', 'Gl Account', 'Business Date Local',
               'Vessel Name', 'Country', 'is_ulysses']
    main_df = df[columns]
    return main_df


def prepare_corporate_df(df, df_setted_country):
    first_account_type_idx = df_setted_country.columns.get_loc('account_type')
    corporate_df = df_setted_country.loc[:,
                   ~((df.columns == 'account_type') & (df_setted_country.columns.duplicated(keep='first') | (
                           df_setted_country.columns.get_loc('account_type') != first_account_type_idx)))]
    corporate_df = corporate_df[corporate_df['account_type'] == 'REEF Corporate']
    corporate_df = corporate_df.dropna(subset=['account_category'])

    columns = ['vessel_code', 'account_category', 'pnl_contribution_daily_usd_rounded',
               'Business Date Local']
    corporate_df = corporate_df[columns]
    column_name_mapping = {
        'vessel_code': 'Vessel',
        'account_category': 'Line Item',
        'pnl_contribution_daily_usd_rounded': 'Amount',
        'country': 'Country',
        'vessel name': 'Vessel Name'
    }
    corporate_df.rename(columns=column_name_mapping, inplace=True)
    corporate_df = corporate_df.groupby(['Vessel', 'Line Item', 'Business Date Local'],
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
    return corporate_df


def spread_corporate(main_df, corporate_df):
    main_df['Business Date Local'] = pd.to_datetime(main_df['Business Date Local'])
    corporate_df['Business Date Local'] = pd.to_datetime(corporate_df['Business Date Local'])

    main_unique_df = main_df.drop_duplicates(subset=['Vessel', 'Business Date Local']).sort_values(
        by=['Vessel', 'Business Date Local'])

    date_unique_vessel_count = main_df.groupby('Business Date Local')['Vessel'].apply(lambda vessels: len(set(
        vessel for vessel in vessels if
        '-' in vessel and not (vessel.split('-')[1] in ['900', '000'] or vessel.startswith('RHQ'))))).to_dict()

    new_rows = []
    for index, row in main_unique_df.iterrows():
        if not row['Vessel'].startswith('RHQ') and '-' in row['Vessel'] and not row['Vessel'].split('-')[1] in ['900',
                                                                                                                '000']:
            corporate_filtered_df = corporate_df[corporate_df['Business Date Local'] == row['Business Date Local']]
            corporate_grouped_df = corporate_filtered_df.groupby(['Line Item', 'Business Date Local', 'Line Order'],
                                                                 as_index=False).agg({'Amount': 'sum'})

            test_count = date_unique_vessel_count.get(row['Business Date Local'], 1)

            if len(corporate_grouped_df) > 0:
                for corporate_index, corporate_row in corporate_grouped_df.iterrows():
                    new_row = row.copy()
                    new_row['Vessel'] = row['Vessel']
                    new_row['Line Item'] = corporate_row['Line Item']
                    new_row['Business Date Local'] = row['Business Date Local']
                    new_row['Amount'] = int(corporate_row['Amount']) / test_count
                    new_row['Gl Account'] = row['Gl Account']
                    new_row['Vessel Name'] = row['Vessel Name']
                    new_row['Country'] = row['Country']
                    new_row['Line Order'] = corporate_row['Line Order']
                    new_rows.append(new_row)

    corporate_spreaded_df = pd.concat([main_df, pd.DataFrame(new_rows)], ignore_index=True)

    final_coupa_data = corporate_spreaded_df[
        corporate_spreaded_df['Country'].isin(['US', 'GB', 'AE', 'CA', 'ES', 'SK', 'DE', 'FR'])]
    return final_coupa_data


def prepare_country_vessel_counts(final_coupa_data):
    # Ülke ve Business Date Local'e göre gruplama yap
    grouped = final_coupa_data.groupby(['Country', 'Business Date Local'])['Vessel'].nunique().reset_index()
    grouped.rename(columns={'Vessel': 'Unique Vessel Count'}, inplace=True)

    # Veriyi sözlük olarak döndür: {'Country': {'Business Date Local': unique_vessel_count}}
    country_vessel_counts = {}
    for _, row in grouped.iterrows():
        if row['Country'] not in country_vessel_counts:
            country_vessel_counts[row['Country']] = {}
        country_vessel_counts[row['Country']][row['Business Date Local']] = row['Unique Vessel Count']

    return country_vessel_counts


def spread_rhq(final_coupa_data):
    new_rows = []
    mapping_df = pd.read_csv('static/coupa-updated-mapping-gl.csv')
    country_vessel_counts = prepare_country_vessel_counts(final_coupa_data)

    only_rhq_data = final_coupa_data[final_coupa_data['Vessel'].str.startswith('RHQ')]
    only_rhq_grouped_data = \
    only_rhq_data.groupby(['Vessel', 'Line Item', 'Gl Account', 'Business Date Local', 'Country'])[
        'Amount'].sum().reset_index()
    only_rhq_grouped_data['Business Date Local'] = pd.to_datetime(only_rhq_grouped_data['Business Date Local'])
    mapping_dict = dict(zip(mapping_df['Gl Acount'], mapping_df['Line Order']))
    only_rhq_grouped_data['Line Order'] = only_rhq_grouped_data['Gl Account'].map(mapping_dict)

    final_coupa_data['Business Date Local'] = pd.to_datetime(final_coupa_data['Business Date Local'])
    df_unique = final_coupa_data.drop_duplicates(subset=['Vessel', 'Business Date Local']).sort_values(
        by=['Vessel', 'Business Date Local'])

    for index, row in df_unique.iterrows():
        if not row['Vessel'].startswith('RHQ') and '-' in row['Vessel'] and not row['Vessel'].split('-')[1] in ['900',
                                                                                                                '000']:
            if not row['Vessel'].startswith('FC'):
                rhq_df = only_rhq_grouped_data[
                    (only_rhq_grouped_data['Business Date Local'] == row['Business Date Local']) &
                    (only_rhq_grouped_data['Country'] == row['Country'])]
                if len(rhq_df) > 0:
                    for grouped_index, grouped_row in rhq_df.iterrows():
                        if row['Country'] == 'SK' or row['Country'] == 'FR':
                            continue
                        new_row = row.copy()
                        new_row['Vessel'] = row['Vessel']
                        new_row['Line Item'] = grouped_row['Line Item']
                        new_row['Business Date Local'] = row['Business Date Local']
                        new_row['Gl Account'] = row['Gl Account']
                        country_vessel_count = country_vessel_counts.get(row['Country'], {}).get(
                            row['Business Date Local'], 1)
                        new_row['Amount'] = int(grouped_row['Amount']) / country_vessel_count
                        new_row['Vessel Name'] = row['Vessel Name']
                        new_row['Country'] = row['Country']
                        new_row['Line Order'] = grouped_row['Line Order']
                        new_rows.append(new_row)

    concated_df = pd.concat([final_coupa_data, pd.DataFrame(new_rows)], ignore_index=True)
    rhq_dropped = concated_df[~concated_df['Vessel'].str.startswith('RHQ')]

    gl_account_to_line_order = mapping_df.set_index('Gl Acount')['Line Order'].to_dict()
    rhq_dropped['Line Order'] = rhq_dropped.apply(
        lambda row: gl_account_to_line_order.get(row['Gl Account'], row['Line Order']) if pd.isna(
            row['Line Order']) else row['Line Order'], axis=1)
    rhq_dropped.drop(columns=['Gl Account'], inplace=True)
    return rhq_dropped


def map_line_item(coupa_df):
    df_mapping = pd.read_csv('static/line_order_mapping.csv')
    line_order_to_item = dict(zip(df_mapping['Line Order'], df_mapping['Line Item']))
    coupa_df['Line Item'] = coupa_df['Line Order'].map(line_order_to_item).fillna(coupa_df['Line Item'])
    return coupa_df


def spread_msa(df):
    back_up_df = df
    new_rows = []

    unique_df = df.drop_duplicates(subset=['Vessel', 'Business Date Local']).sort_values(
        by=['Vessel', 'Business Date Local'])
    dagitilacak_df = df[df['Vessel'].str.contains('-900-|000')]
    df = unique_df[~unique_df['Vessel'].str.contains('-900-|000')]
    df_country_spesific = df[(df['Country'] == 'US') | (df['Country'] == 'CA')]

    for index, row in df_country_spesific.iterrows():
        matching_df = dagitilacak_df[dagitilacak_df['Vessel'].str[:3] == row['Vessel'][:3]]

        if not matching_df.empty:
            matching_df = matching_df[matching_df['Business Date Local'] == row['Business Date Local']]
            if not matching_df.empty:
                for _index, _row in matching_df.iterrows():
                    unique_vessels_count = df[df['Vessel'].str[:3] == row['Vessel'][:3]]['Vessel'].nunique() or 1
                    new_row = row.copy()
                    new_row['Line Item'] = _row['Line Item']
                    new_row['Business Date Local'] = row['Business Date Local']
                    new_row['Amount'] = _row['Amount'] / unique_vessels_count
                    new_row['Line Order'] = _row['Line Order']
                    new_row['Country'] = _row['Country']
                    new_rows.append(new_row)

    df_final = pd.concat([back_up_df, pd.DataFrame(new_rows)], ignore_index=True)
    return df_final


def prepare_df_with_fc(df):
    df_with_fc = df[df['vessel_code'].str.startswith('FC')]

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
    df_with_fc.rename(columns=column_name_mapping, inplace=True)

    columns = ['Vessel', 'Line Item', 'Amount', 'Gl Account', 'Business Date Local', 'Vessel Name', 'Country',
               'is_ulysses']
    main_df = df_with_fc[columns]

    mapping_df = pd.read_csv('static/coupa-updated-mapping-gl.csv')
    gl_account_to_line_order = mapping_df.set_index('Gl Acount')['Line Order'].to_dict()
    main_df['Line Order'] = ''

    def map_line_order(row):
        line_order = gl_account_to_line_order.get(row['Gl Account'], None)
        if line_order:
            return line_order
        return row['Line Order']

    main_df['Line Order'] = main_df.apply(map_line_order, axis=1)

    main_df['Vessel'] = main_df['Vessel'].astype(str)
    main_df['Line Item'] = main_df['Line Item'].astype(str)
    main_df['Gl Account'] = main_df['Gl Account'].astype(str)
    main_df['Business Date Local'] = pd.to_datetime(main_df['Business Date Local'])
    main_df['Country'] = main_df['Country'].astype(str)
    main_df['is_ulysses'] = main_df['is_ulysses'].astype(bool)
    main_df['Line Order'] = main_df['Line Order'].astype(str)

    main_df = main_df.groupby(
        ['Vessel', 'Line Item', 'Gl Account', 'Business Date Local', 'Country', 'is_ulysses', 'Line Order'],
        as_index=False).agg({'Amount': 'sum'})

    return main_df


def spread_uk(df, df_with_overhead):
    only_london_df = df[df['Country'] == 'GB']
    only_london_df_unique = only_london_df.drop_duplicates(subset=['Vessel', 'Business Date Local']).sort_values(
        by=['Vessel', 'Business Date Local'])
    new_rows = []

    gb_unqiue_vessile_count = only_london_df_unique['Vessel'].str.strip().unique().tolist()

    for _, row in only_london_df_unique.iterrows():
        df_with_date_filtred_overhead = df_with_overhead[
            df_with_overhead['Business Date Local'] == row['Business Date Local']]
        if len(df_with_date_filtred_overhead) > 0:
            for _index, _row in df_with_date_filtred_overhead.iterrows():
                new_row = row.copy()
                new_row['Line Item'] = _row['Line Item']
                new_row['Line Order'] = _row['Line Order']
                new_row['Amount'] = _row['Amount'] / len(gb_unqiue_vessile_count)
                new_row['Country'] = _row['Country']
                new_rows.append(new_row)

    final_df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    return final_df


def prepare_df_with_pa(df):
    pa_vessels = df[df['vessel_code'].str.startswith('PA-')]
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
    pa_vessels.rename(columns=column_name_mapping, inplace=True)
    columns = ['Vessel', 'Line Item', 'Amount', 'Gl Account', 'Business Date Local',
               'Vessel Name', 'Country', 'is_ulysses']
    main_df = pa_vessels[columns]

    mapping_df = pd.read_csv('static/coupa-updated-mapping-gl.csv')
    gl_account_to_line_order = mapping_df.set_index('Gl Acount')['Line Order'].to_dict()
    main_df['Line Order'] = ''

    def map_line_order(row):
        line_order = gl_account_to_line_order.get(row['Gl Account'], None)
        if line_order:
            return line_order
        return row['Line Order']

    main_df['Line Order'] = main_df.apply(map_line_order, axis=1)

    main_df['Vessel'] = main_df['Vessel'].astype(str)
    main_df['Line Item'] = main_df['Line Item'].astype(str)
    main_df['Gl Account'] = main_df['Gl Account'].astype(str)
    main_df['Business Date Local'] = pd.to_datetime(main_df['Business Date Local'])
    main_df['Country'] = main_df['Country'].astype(str)
    main_df['is_ulysses'] = main_df['is_ulysses'].astype(bool)
    main_df['Line Order'] = main_df['Line Order'].astype(str)
    main_df = main_df.groupby(
        ['Vessel', 'Line Item', 'Gl Account', 'Business Date Local', 'Country', 'is_ulysses', 'Line Order'],
        as_index=False).agg({'Amount': 'sum'})
    return main_df


def spread_pa(df, df_with_pa):
    only_london_df = df[df['Country'] == 'GB']
    only_london_df_unique = only_london_df.drop_duplicates(subset=['Vessel', 'Business Date Local']).sort_values(
        by=['Vessel', 'Business Date Local'])
    new_rows = []

    gb_unqiue_vessile_count = only_london_df_unique['Vessel'].unique().tolist()

    for _, row in only_london_df_unique.iterrows():
        df_with_date_filtred_overhead = df_with_pa[df_with_pa['Business Date Local'] == row['Business Date Local']]
        if len(df_with_date_filtred_overhead) > 0:
            for _index, _row in df_with_date_filtred_overhead.iterrows():
                new_row = row.copy()
                new_row['Line Item'] = _row['Line Item']
                new_row['Line Order'] = _row['Line Order']
                new_row['Amount'] = _row['Amount'] / len(gb_unqiue_vessile_count)
                new_row['Country'] = _row['Country']
                new_rows.append(new_row)

    final_df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
    return final_df


def spread_sa(df):
    rows = []
    df_with_sa = df[df['Vessel'].str.startswith('SA')]

    df_gb = df[df['Country'] == 'GB']
    df_not_sa = df_gb[~df_gb['Vessel'].str.startswith('SA')]
    df_dropped = df_not_sa.drop_duplicates(subset=['Vessel', 'Business Date Local']).sort_values(
        by=['Vessel', 'Business Date Local'])
    gb_unqiue_vessile_count = df_dropped['Vessel'].unique().tolist()

    for index, row in df_dropped.iterrows():
        df_with_date_filtred = df_with_sa[df_with_sa['Business Date Local'] == row['Business Date Local']]
        if len(df_with_date_filtred) > 0:
            for _index, _row in df_with_date_filtred.iterrows():
                new_row = row.copy()
                new_row['Line Item'] = _row['Line Item']
                new_row['Line Order'] = _row['Line Order']
                new_row['Amount'] = _row['Amount'] / len(gb_unqiue_vessile_count)
                new_row['Country'] = 'SK'
                rows.append(new_row)

    spreaded_df = pd.concat([df, pd.DataFrame(rows)], ignore_index=True)
    final_df = spreaded_df[~spreaded_df['Vessel'].str.startswith('SA')]

    return final_df


def start_coupa(start_date_str, end_date_str):
    extractor = Extractor(start_date_str, end_date_str)
    df_original = extractor.get_data()
    #df_original = pd.read_csv('/Users/mertcelikan/PycharmProjects/dail-pnl-v3/input/coupa_output_jun.csv')

    #df_original = clear_duplicate_rows(df_original)

    df, df_setted_country = clear_data(df_original)

    df_with_overhead = prepare_df_with_fc(df)
    df_with_pa = prepare_df_with_pa(df)

    df = df[~df['vessel_code'].str.startswith('PA-')]
    df = df[df['vessel_code'] != 'Overhead']
    main_df = clear_columns(df)

    corporate_df = prepare_corporate_df(df, df_setted_country)
    final_coupa_data = spread_corporate(main_df, corporate_df)

    coupa_df = spread_rhq(final_coupa_data)

    coupa_df = map_line_item(coupa_df)

    coupa_df = spread_msa(coupa_df)
    df_spreaded_uk = spread_uk(coupa_df, df_with_overhead)
    df_spreaded_pa = spread_pa(df_spreaded_uk, df_with_pa)
    mask = df_spreaded_pa['Vessel'].str.contains('-900-|-000-')
    df_dropped = df_spreaded_pa[~mask]
    df_spreaded_sa = spread_sa(df_dropped)
    df_spreaded_sa = df_spreaded_sa[df_spreaded_sa['Amount'] != 0]
    return df_spreaded_sa
