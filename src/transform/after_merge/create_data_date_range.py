from datetime import datetime, timedelta

import pandas as pd


def create_mf_df_for_date_range(start_date_str, end_date_str):
    # Convert the date strings to datetime objects
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

    # Initialize an empty list to store rows
    rows = []

    # Example row template
    row_template = {
        'Vessel': 'MF',
        'Line Item': 'Gross Sales',
        'Business Date Local': '',  # Will be set for each day
        'Vessel Name': '',  # Modify as needed
        'Country': 'US',
        'is_ulysses': False,
        'Line Order': 'L1-01-02',
        'pl_mapping_2': 'MF - Revenue',
        'pl_mapping_3': 'MF - Revenue',
        'pl_mapping_4': 'MF - Revenue',
        'Region': 'NA',
        'Amount': 8155.17
    }

    # Add rows for each day in the date range
    current_date = start_date
    while current_date <= end_date:
        row_copy = row_template.copy()
        row_copy['Business Date Local'] = current_date.strftime('%m/%d/%Y')
        rows.append(row_copy)
        current_date += timedelta(days=1)

    # Create DataFrame from the list of rows
    df = pd.DataFrame(rows)
    df['Region'] = df['Region'].fillna('NA').str.strip().str.upper()
    return df


def create_order_lord_df_for_date_range(start_date_str, end_date_str):
    # Convert the date strings to datetime objects
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

    # Initialize an empty list to store rows
    rows = []

    # Example row template
    row_template = {
        'Vessel': 'OL',
        'Line Item': 'Gross Sales',
        'Business Date Local': '',  # Will be set for each day
        'Vessel Name': '',  # Modify as needed
        'Country': 'SK',
        'is_ulysses': False,
        'Line Order': 'L1-01-03',
        'pl_mapping_2': 'SaaS - Revenue',
        'pl_mapping_3': 'SaaS - Revenue',
        'pl_mapping_4': 'SaaS - Revenue',
        'Region': 'EU',
        'Amount': 1919
    }

    # Add rows for each day in the date range
    current_date = start_date
    while current_date <= end_date:
        row_copy = row_template.copy()
        row_copy['Business Date Local'] = current_date.strftime('%m/%d/%Y')
        rows.append(row_copy)
        current_date += timedelta(days=1)

    # Create DataFrame from the list of rows
    df = pd.DataFrame(rows)
    df['Region'] = df['Region'].fillna('NA').str.strip().str.upper()
    return df


def create_order_lord_expense_df_for_date_range(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

    rows = []

    row_template = {
        'Vessel': 'OL',
        'Line Item': 'L4 Expenses',
        'Business Date Local': '',  # Will be set for each day
        'Vessel Name': '',  # Modify as needed
        'Country': 'SK',
        'is_ulysses': False,
        'Line Order': 'L4-01-02',
        'pl_mapping_2': '1. People G&A',
        'pl_mapping_3': 'SK People',
        'pl_mapping_4': 'SK People',
        'Region': 'EU',
        'Amount': 0.0001
    }

    current_date = start_date
    while current_date <= end_date:
        row_copy = row_template.copy()
        row_copy['Business Date Local'] = current_date.strftime('%m/%d/%Y')
        rows.append(row_copy)
        current_date += timedelta(days=1)

    df = pd.DataFrame(rows)
    df['Region'] = df['Region'].fillna('NA').str.strip().str.upper()
    return df


def create_data_between_range(start_date_str, end_date_str):
    df_mf = create_mf_df_for_date_range(start_date_str, end_date_str)
    df_ol = create_order_lord_df_for_date_range(start_date_str, end_date_str)
    df_ol_expense = create_order_lord_expense_df_for_date_range(start_date_str, end_date_str)
    df = pd.concat([df_mf, df_ol, df_ol_expense], ignore_index=True)
    return df
