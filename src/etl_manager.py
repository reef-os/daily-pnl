from src.ETL.pnl_orders.pnl_order_manager import PnlOrderManager
from src.ETL.statement.statement_manager import StatementManager
from src.ETL.coupa.coupa_manager import CoupaManager

from src.mapping_manager import MappingManager
from src.merge_manager import MergeManager


class ETLManager:
    def __init__(self, start_date_str, end_date_str):
        self.__pnl_manager = PnlOrderManager(start_date_str, end_date_str)
        self.__coupa_manager = CoupaManager(start_date_str, end_date_str)
        self.__statement_manager = StatementManager(start_date_str, end_date_str)

        self.__merge_manager = MergeManager()
        self.__mapping_manager = MappingManager()

    def start(self):
        coupa_df = self.__coupa_manager.start(local=False)
        coupa_df.to_csv("data/bronze/coupa.csv")

        pnl_df = self.__pnl_manager.start(local=False)
        pnl_df.to_csv("data/bronze/pnl.csv")

        statement_df = self.__statement_manager.start(local=False)
        statement_df.to_csv("data/bronze/statement.csv")

        merge_pnl_coupa_df = self.__merge_manager.merge_pnl_coupa(pnl_df, coupa_df)
        merge_pnl_coupa_df.to_csv("data/silver/merge_pnl_coupa.csv")

        merged_df = self.__merge_manager.merge_statement_merged_data(merge_pnl_coupa_df, statement_df)
        merged_df.to_csv("data/silver/merge_statement_merged_data.csv")

        mapped_df = self.__mapping_manager.start(merged_df)
        mapped_df.to_csv("data/gold/final_nisan.csv")