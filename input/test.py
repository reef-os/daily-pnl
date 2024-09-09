import pandas as pd
from src.transform.coupa.coupa import start_coupa
import warnings

warnings.filterwarnings('ignore')


def duplicate_day_to_month():
    df = pd.read_csv("/Users/mertcelikan/PycharmProjects/dail-pnl-v3/input/Input Jun'24.csv")
    df['Business Date Local'] = pd.to_datetime(df['Business Date Local'])

    date_range = pd.date_range(start='2024-06-01', end='2024-06-30')

    # Her tarih için dataframe'i çoğaltalım ve Business Date Local kolonunu güncelleyelim
    dfs = []
    for date in date_range:
        temp_df = df.copy()
        temp_df['Business Date Local'] = date
        dfs.append(temp_df)

    # Tüm dataframeleri birleştirelim
    final_df = pd.concat(dfs, ignore_index=True)
    final_df.to_csv("/Users/mertcelikan/PycharmProjects/dail-pnl-v3/input/coupa_output_jun.csv", index=False)
    return final_df



duplicate_day_to_month()