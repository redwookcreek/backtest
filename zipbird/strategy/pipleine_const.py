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

def get_db_conn(db_name) -> sqlite3.Connection:
    return sqlite3.connect(_get_db_file(db_name))

def _get_db_file(db_name:str) -> str:
    return f'results/{db_name}.db'

def has_db_file(db_name:str):
    file_path = _get_db_file(db_name)
    return os.path.isfile(file_path)

def format_trade_day(trade_day: pd.Timestamp) -> str:
    return trade_day.strftime('%Y-%m-%d')
