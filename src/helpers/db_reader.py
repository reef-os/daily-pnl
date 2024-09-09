import pandas as pd
import awswrangler as wr
from helpers.aws_manager import AWSManager

class DbReader:
    def __init__(self):
        self.__aws_manager = AWSManager()
        self.AWS_SESSION = self.__aws_manager.get_aws_session()

    def __read_data(self, sql) -> pd.DataFrame:
        con = wr.redshift.connect("Admin-Redshift-Connection", boto3_session=self.AWS_SESSION)
        data = wr.redshift.read_sql_query(sql=sql, con=con)
        con.close()
        return data

    def get_data(self, filename: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        with open(f"sql/{filename}.sql", 'r', encoding='utf-8') as f:
            sql = f.read()
            if end_date is None:
                sql = sql.format(start_date=start_date)
            elif start_date is None and end_date is None:
                return self.__read_data(sql)
            else:
                sql = sql.format(start_date=start_date, end_date=end_date)
            data = self.__read_data(sql)
        return data


