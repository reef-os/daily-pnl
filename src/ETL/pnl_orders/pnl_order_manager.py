import pandas as pd

from src.helpers.db_reader import DbReader
from src.ETL.pnl_orders.transform_manager import PnlTransformManager


class PnlOrderManager:
    def __init__(self, start_date_str, end_date_str):
        self.__start_date_str = start_date_str
        self.__end_date_str = end_date_str
        self.__db_reader = DbReader()
        self.__transformer = PnlTransformManager()

    def __get_data(self):
        return self.__db_reader.get_data("pnl_orders", self.__start_date_str, self.__end_date_str)

    def start(self, local=False):
        if local:
            return pd.read_csv("data/bronze/pnl.csv")
        print("Starting PNL Orders ETL")
        df = self.__get_data()
        transformed_df = self.__transformer.start_transform(df)
        print("Finished PNL Orders ETL")
        return transformed_df
