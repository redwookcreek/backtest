"""
The old db format is one table 'pipeline_data' with all indicators
trade_day, ticker, sid, ind_name, ind_value

Convert this to a different format:
Each indicator gets it's own table t_<ind_name>
with columns
trade_day, ticker, sid, ind_value
"""
import sqlite3
from collections import defaultdict

from zipbird.strategy import pipleine_const as const

READ_BATCH_SIZE = 100_000

class TransferDb:
    def __init__(self, old_db_conn: sqlite3.Connection, new_db_conn:sqlite3.Connection):
        self.old_db_conn = old_db_conn
        self.new_db_conn = new_db_conn

    def transfer_to_new_db(self):
        old_cursor = self.old_db_conn.cursor()
        columns = [
            const.IND_NAME,
            const.TRADE_DAY,
            const.TICKER,
            const.SID,
            const.IND_VALUE
        ]
        old_cursor.execute(
            f"""SELECT {','.join(columns)} from {const.TABLE_NAME}""")
        
        new_cursor = self.new_db_conn.cursor()
        while True:
            rows = old_cursor.fetchmany(READ_BATCH_SIZE)
            if not rows:
                break
            data_by_ind = defaultdict(list)
            for row in rows:
                ind_name = row[0]
                data_by_ind[ind_name].append(row[1:])
            for ind_name, ind_data in data_by_ind.items():
                self._insert_to_new_db(ind_name, ind_data)
            


