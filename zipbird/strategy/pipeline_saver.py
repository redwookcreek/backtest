import pandas as pd
import sqlite3

from zipbird.strategy import pipleine_const as const
from zipbird.strategy.strategy import BaseStrategy
from zipbird.strategy.pipeline_maker import PipelineMaker
from zipbird.utils import logger_util

UNIVERSE_MAX_RANK = 2000
UNIVERSE_MIN_PRICE = 1.0
UNIVERSE_WINDOW_LENGTH = 200

METADATA_TABLE = 'metadata'

class MismatchDateRangeException(Exception):
    pass

class PipelineSaver:

    def __init__(self,
                 strategies:list[BaseStrategy],
                 db_conn: sqlite3.Connection,
                 start_fresh=False):
        self.strategies = strategies

        self._init_db(db_conn, start_fresh)

        # Init pipeline
        self.pipeline_maker = PipelineMaker()
        self.pipeline_maker.add_dollar_volume_rank_universe(
            max_rank=UNIVERSE_MAX_RANK,
            min_close=UNIVERSE_MIN_PRICE,
            window_length=UNIVERSE_WINDOW_LENGTH)
        for strategy in self.strategies:
            strategy.prepare_pipeline_columns(self.pipeline_maker)

    def init(self, debug_logger:logger_util.DebugLogger, start_day:pd.Timestamp, end_day:pd.Timestamp):
        self.debug_logger = debug_logger

        # make sure existing data is compatiable with requested date range
        self._check_update_existing_data_compatability(start_day=start_day, end_day=end_day)

        # if there columns already exists, we don't need to calculate them again
        existing_columns = self._get_existing_columns()
        self.pipeline_maker.remove_columns(existing_columns)
        self.debug_logger.debug_print(
            1, 
            f'To store data from {start_day} to {end_day}. '
            f'Columns {",".join(self.pipeline_maker.get_columns())}. '
            f'Columns removed because they already exist: {",".join(existing_columns)}')

        # create metadata for this run
        self._upsert_metadata(start_day, end_day)

    def _init_db(self, db_conn:sqlite3.Connection, start_fresh:bool):
        # Init DB
        self.db_conn = db_conn
        cursor = self.db_conn.cursor()
        if start_fresh:
            cursor.execute(f'DROP TABLE IF EXISTS {const.TABLE_NAME}')
            cursor.execute(f'DROP TABLE IF EXISTS {METADATA_TABLE}')
        # Create main indicator data table
        columns = [f'{col_name} {col_type}'
                for col_name, col_type in 
                const.KEY_COLMNS + const.VALUE_COLUMNS]
        cursor.execute(f'CREATE TABLE IF NOT EXISTS {const.TABLE_NAME} ({",".join(columns)})')

        # create metadata table
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {METADATA_TABLE}
            (name text, value text)""")
        cursor.execute(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_metadata 
            ON {METADATA_TABLE} (name)""")

    def _check_update_existing_data_compatability(self, start_day:pd.Timestamp, end_day: pd.Timestamp):
        """Check if can update existing data without causing data inconsistency"""        
        db_start_day = self._get_metadata('start_day')
        if db_start_day and db_start_day != const.format_trade_day(start_day):
            raise MismatchDateRangeException(f'Start day does not match, in db: {db_start_day}, requested: {start_day}')
        
        db_end_day = self._get_metadata('end_day')
        if db_end_day and db_end_day !=const.format_trade_day(end_day):
            raise MismatchDateRangeException(f'end day does not match, in db: {db_end_day}, requested: {end_day}')

    def _get_metadata(self, key) -> str | None:
        cursor = self.db_conn.cursor()
        results = cursor.execute(f'select name, value from {METADATA_TABLE}').fetchall()
        for name, value in results:
            if key == name:
                return value
        return None
    
    def _get_existing_columns(self) -> list[str]:
        columns = self._get_metadata('columns')
        if not columns:
            return []
        else:
            return columns.split(',')
    
    def _upsert_metadata(self, start_day:pd.Timestamp, end_day: pd.Timestamp):
        # metadata table is a key value pair table that contains
        #   start_day: start day of the backtest
        #   end_day: end day of the backtest
        #   columns: comma separated indicator columns
        cursor = self.db_conn.cursor()
        
        to_update = {
            'start_day': const.format_trade_day(start_day),
            'end_day': const.format_trade_day(end_day),
            'columns': ','.join(self.pipeline_maker.get_columns())
        }
        for key, value in to_update.items():
            sql = f"""
            INSERT OR REPLACE INTO {METADATA_TABLE}
            (name, value)
            VALUES
            (?, ?)
            """
            cursor.execute(sql, (key, value))

    def make_pipeline(self):        
        return self.pipeline_maker.make_pipeline()

    def record_pipeline_data(self, trade_day:pd.Timestamp, pipeline_data:pd.DataFrame):
        self.debug_logger.debug_print(2, f'trade day {trade_day}, {len(pipeline_data)}')
        # trade day is the day to place trade, by the nature of zipline pipeline
        # the pipeline data provided for the trade day is calcuated from yesterday's 
        # price
        trade_day_str = const.format_trade_day(trade_day)
        data_to_insert = []
        for idx, row in pipeline_data.iterrows():
            ticker = idx.symbol
            sid = idx.sid
            for col in pipeline_data.columns:
                if pd.notna(row[col]):
                    data_to_insert.append(( 
                        trade_day_str,
                        ticker,
                        sid,
                        col,
                        row[col],
                    ))
        insert_sql = f"""
        INSERT OR REPLACE INTO {const.TABLE_NAME} (
          {const.TRADE_DAY},
          {const.TICKER},
          {const.SID},
          {const.IND_NAME},
          {const.IND_VALUE}
        )
        VALUES (?, ?, ?, ?, ?)
        """
        cursor = self.db_conn.cursor()
        cursor.executemany(insert_sql, data_to_insert)
        self.db_conn.commit()

    def create_index(self):
        cursor = self.db_conn.cursor()
        # Create index
        sql = f"""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_1 ON 
        {const.TABLE_NAME} ({",".join(col for col, _ in const.KEY_COLMNS)})
        """
        cursor.execute(sql)
        self.db_conn.commit()
