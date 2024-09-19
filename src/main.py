import pandas as pd
import multiprocessing
from datetime import datetime, timedelta
from transform.pnl_orders.pnl_orders import start_pnl_orders
from transform.coupa.coupa import start_coupa
from transform.statement.statement import start_statement
from helpers import aws_manager
from manager import process_data

import warnings

warnings.filterwarnings('ignore')


def run_process(func, args):
    return func(*args)


def concat_dfs(*dfs):
    result = pd.concat(dfs, ignore_index=True)
    result = result[result['Amount'] != 0]
    return result


def retrieve_all_data(start_date, end_date_str):
    with multiprocessing.Pool(processes=3) as pool:
        results = pool.starmap(
            run_process,
            [
                (start_coupa, (start_date, end_date_str)),
                (start_pnl_orders, (start_date, end_date_str)),
                (start_statement, (start_date, end_date_str))
            ]
        )

    df_coupa, df_pnl, df_statement = results
    print("len(df_coupa): ", len(df_coupa))
    print("len(df_pnl): ", len(df_pnl))
    print("len(df_statement): ", len(df_statement))

    merged_df = concat_dfs(df_coupa, df_pnl, df_statement)
    print("len(merged_df): ", len(merged_df))
    return merged_df


if __name__ == "__main__":
    yesterday = datetime.today() - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    aws_manager = aws_manager.AWSManager()

    df = retrieve_all_data("2024-09-01", "2024-09-17")

    processed_df = process_data(df, "2024-09-01", "2024-09-17")

    aws_manager.insert_to_redshift(processed_df)
    """
    yesterday = datetime.today() - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    aws_manager = aws_manager.AWSManager()

    df = retrieve_all_data(yesterday_str, yesterday_str)

    processed_df = process_data(df, yesterday_str, yesterday_str)

    aws_manager.insert_to_redshift(processed_df)
    """

