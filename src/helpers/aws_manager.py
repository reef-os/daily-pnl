import boto3
import pandas as pd
from io import StringIO


class AWSManager:
    def __init__(self, profile_name='AdministratorAccess-815179467351', region_name='us-east-1'):
        self.__profile_name = profile_name
        self.__region_name = region_name
        self.__session = None

    def get_aws_session(self):
        if self.__session is None:
            #print(f"Getting AWS session with profile name: {self.__profile_name} and region name: {self.__region_name}")
            self.__session = boto3.session.Session(
                profile_name=self.__profile_name,
                region_name=self.__region_name
            )
        return self.__session