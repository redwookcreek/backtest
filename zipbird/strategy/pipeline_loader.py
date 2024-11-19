import pandas as pd
import sqlite3
import datetime

from zipbird.strategy.pipeline_maker import PipelineMaker
from zipbird.strategy import pipleine_const as const
from zipbird.strategy.strategy_executor import StrategyExecutor
from zipbird.utils.timer_context import TimerContext

class DBFileNotFoundException(Exception):
    pass

DEFAULT_CHUNK_DAYS = 2000

class PipelineLoader:
    def __init__(self, strategy:StrategyExecutor, db_conn:sqlite3.Connection, chunk_days=DEFAULT_CHUNK_DAYS):
        self.strategy = strategy
        pipeline_maker = PipelineMaker()
        strategy.prepare_pipeline_columns(pipeline_maker)
        
        self.columns = list(pipeline_maker.get_columns())
        self.db_conn = db_conn

        self.chunk_days = chunk_days
        self.chunk_end_day = None
        self.current_df = []

    def init(self, debug_logger, end_day:pd.Timestamp, timer_context:TimerContext):
        self.strategy.init(debug_logger=debug_logger)
        self.debug_logger = debug_logger
        self.end_day = end_day
        self.timer_context = timer_context
        
    def load_for_trade_day(self, trade_day: pd.Timestamp) -> pd.DataFrame:
        if self._is_loaded(trade_day):
            return self._get_for_trade_day(trade_day)
        with self.timer_context.timer('load chunk'):
          self.chunk_df = self._load_chunk(trade_day)
        return self._get_for_trade_day(trade_day)
    
    def _is_loaded(self, trade_day: pd.Timestamp):
        return self.chunk_end_day and self.chunk_end_day >= trade_day
  
    def _get_for_trade_day(self, trade_day: pd.Timestamp):
        return self.chunk_df.loc[const.format_trade_day(trade_day)]
    
    def _load_chunk(self, trade_day:pd.Timestamp):
        self.chunk_end_day = pd.Timestamp(trade_day + pd.Timedelta(days=self.chunk_days))
        self.chunk_end_day = min(self.chunk_end_day, self.end_day)
        self.debug_logger.debug_print(
            1,
            f'{datetime.datetime.now()}: Loading data from {trade_day} to {self.chunk_end_day}')
        dfs = []
        for column in self.columns:
            query = f"""
              SELECT
                {const.TRADE_DAY},
                {const.SID},
                {const.IND_VALUE}
              FROM
                {const.get_ind_table_name(column)}
              WHERE 
                {const.TRADE_DAY} >= ?
                AND
                {const.TRADE_DAY} <= ?
            """            
            with self.timer_context.timer('read sql query'):
              df = pd.read_sql_query(
                  query, self.db_conn,
                  params=[const.format_trade_day(trade_day),
                          const.format_trade_day(self.chunk_end_day)])
            with self.timer_context.timer('set index'):            
              df = (df
                .set_index([const.TRADE_DAY, const.SID])
                .rename(columns={const.IND_VALUE: column}))
            dfs.append(df)
        # Merge all dataframes on SID
        with self.timer_context.timer('concat dfs'):
          final_df = pd.concat(dfs, axis=1)
        return final_df
