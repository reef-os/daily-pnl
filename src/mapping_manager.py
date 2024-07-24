from src.mapping.labor_mapping import LaborMapping
from src.mapping.line_order_mapping import LineOrderMapping
from src.mapping.multiply_negative_mapping import MultiplyNegativeMapping


class MappingManager:
    def __init__(self):
        self.__labor_mapping = LaborMapping()
        self.__line_order_mapping = LineOrderMapping()
        self.__multiply_negative_mapping = MultiplyNegativeMapping()

    def start(self, df):
        print("--- MappingManager starting... ---")
        df = self.__labor_mapping.start(df)
        df = self.__line_order_mapping.map_line_order_and_line_item(df)
        df = self.__multiply_negative_mapping.eksi_bir_carp(df)
        print("--- MappingManager finished. ---")
        return df
