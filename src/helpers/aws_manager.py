import boto3
import awswrangler as wr


class AWSManager:
    def __init__(self, profile_name='AdministratorAccess-815179467351', region_name='us-east-1'):
        self.__profile_name = profile_name
        self.__region_name = region_name
        self.__session = None

    def get_aws_session(self):
        if self.__session is None:
            #print(f"Getting AWS session with profile name: {self.__profile_name} and region name: {self.__region_name}")
            self.__session = boto3.session.Session(
                #profile_name=self.__profile_name,
                region_name=self.__region_name
            )
        return self.__session

    def insert_to_redshift(self, df):
        session = self.get_aws_session()
        secret_name = "/timemachine/dev/datawarehouse/credentials"
        schema = 'finance'
        table_name = 'daily_pnl'

        conn = wr.redshift.connect(
            secret_id=secret_name,
            boto3_session=session
        )

        wr.redshift.to_sql(
            df=df,
            con=conn,
            table=table_name,
            schema=schema,
            mode='append',
            index=False
        )
        print(f"Data inserted to Redshift. Schema: {schema}, Table: {table_name}")