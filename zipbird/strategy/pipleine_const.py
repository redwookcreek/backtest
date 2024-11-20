import os
import sqlite3
import pandas as pd


TABLE_NAME = 'pipeline_data'

TRADE_DAY = 'trade_day'
TICKER = 'ticker'
SID = 'sid'
IND_NAME = 'ind_name'
IND_VALUE = 'ind_value'

KEY_COLMNS = [
    (TRADE_DAY, 'text'),
    (TICKER, 'text'),
    (IND_NAME, 'text'),
]
VALUE_COLUMNS = [
    (SID, 'INTEGER'),
    (IND_VALUE, 'REAL'),
]

def has_db_file(db_name:str):
    return os.path.isfile(db_name)

def format_trade_day(trade_day: pd.Timestamp) -> str:
    return trade_day.strftime('%Y-%m-%d')
