import pandas as pd

from src.etl_manager import ETLManager
import time



if __name__ == "__main__":
    start_time = time.time()

    #etl_manager = ETLManager('2024-04-01', '2024-04-30')
    #etl_manager.start()
    """
    df1 = pd.read_csv("data/gold/nisan_data.csv")
    df2 = pd.read_csv("data/gold/mayis_data.csv")
    df3 = pd.read_csv("data/gold/haziran_data.csv")

    df_merged = pd.concat([df1, df2, df3], ignore_index=True)
    df_merged.to_csv("data/gold/merged_data.csv")
    """

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Scriptin çalışma süresi: {execution_time} saniye")



# gross sales 6.3 milyon
#net sales 5.3