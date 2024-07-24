from src.ETL.pnl_orders.transform.pre_transform import PreTransformer
from src.ETL.pnl_orders.transform.mapping_transform import MappingTransformer
from src.ETL.pnl_orders.transform.post_transform import PostTransformer


class PnlTransformManager:
    def __init__(self):
        self.__pre_transformer = PreTransformer()
        self.__mapping_transformer = MappingTransformer()
        self.__post_transformer = PostTransformer()

    def start_transform(self, df):
        print("Starting PNL Orders transform...")
        pre_transformed_df = self.__pre_transformer.start_pre_transform(df)
        mapped_df = self.__mapping_transformer.start_mapping(pre_transformed_df)
        post_transformed_df = self.__post_transformer.start_post_transform(mapped_df)
        print("Finished PNL Orders transform!")
        return post_transformed_df
