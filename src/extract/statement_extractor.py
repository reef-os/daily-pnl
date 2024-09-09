import pandas as pd
from datetime import datetime, timedelta
from helpers.db_reader import DbReader

class Extractor:
    def __init__(self, start_date_str, end_date_str):
        self.__end_date_str = end_date_str
        self.__start_date_str = start_date_str
        self.__db_reader = DbReader()

    def __fetch_statement_data_for_date(self, current_date):
        current_date_str = current_date.strftime('%Y-%m-%d')
        df = self.__db_reader.get_data("pnl_statement_amounts", start_date=current_date_str)
        return df

    def __get_all_statement_data(self):
        start_date = datetime.strptime(self.__start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(self.__end_date_str, '%Y-%m-%d')

        all_df = pd.DataFrame()
        current_date = start_date
        while current_date <= end_date:
            df = self.__fetch_statement_data_for_date(current_date)
            df['Business Date Local'] = current_date
            all_df = pd.concat([all_df, df], ignore_index=True)
            current_date += timedelta(days=1)
        return all_df

    def get_data(self):
        df = self.__get_all_statement_data()
        return df