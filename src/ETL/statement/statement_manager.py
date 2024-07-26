import pandas as pd

from src.helpers.db_reader import DbReader
from src.ETL.statement.extract.extractor import Extractor
from src.ETL.statement.transform.transformer import Transformer


class StatementManager:
    def __init__(self, start_date_str, end_date_str):
        self.__start_date_str = start_date_str
        self.__end_date_str = end_date_str
        self.__db_reader = DbReader()
        self.__extractor = Extractor(self.__start_date_str, self.__end_date_str)

    def start(self, local=False):
        if local:
            return pd.read_csv('data/bronze/statement.csv')
        print("--- Starting statement ETL ---")
        df = self.__extractor.get_data()
        transformed_df = Transformer.transform(df)
        final_transformed_df = Transformer.map_pl_mapping4_to_line_order(transformed_df)
        print("--- Statement ETL completed ---")
        return final_transformed_df
