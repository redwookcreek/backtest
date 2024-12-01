import pandas as pd
import sqlite3

from zipbird.strategy.pipeline_maker import PipelineMaker
from zipbird.strategy import pipleine_const as const
from zipbird.strategy.strategy_executor import StrategyExecutor
from zipbird.utils.timer_context import TimerContext

DEFAULT_CHUNK_DAYS = 2000

class DBFileNotFoundException(Exception):
    pass

class PipelineLoader:
    def __init__(self, strategy:StrategyExecutor, db_conn:sqlite3.Connection, chunk_days=DEFAULT_CHUNK_DAYS):
        self.strategy = strategy
        pipeline_maker = PipelineMaker()
        strategy.prepare_pipeline_columns(pipeline_maker)
        
        self.columns = list(pipeline_maker.get_columns())
        self.db_conn = db_conn
        self.chunk_days = chunk_days
        self.chunk_end_day = None
        self.chunk_df = None

    def init(self, debug_logger, timer_context:TimerContext):
        self.strategy.init(debug_logger=debug_logger)
        self.debug_logger = debug_logger
        self.timer_context = timer_context
        
    def load_for_trade_day(self, trade_day: pd.Timestamp) -> pd.DataFrame:
        if not self._is_trade_day_loaded(trade_day):
          self._load_chunk(trade_day)
        return self._get_for_trade_day(trade_day)
    
    def _is_trade_day_loaded(self, trade_day: pd.Timestamp) -> bool:
        return self.chunk_end_day and self.chunk_end_day >= trade_day
    
    def _get_for_trade_day(self, trade_day: pd.Timestamp) -> pd.DataFrame:
        return self.chunk_df.loc[const.format_trade_day(trade_day)]
    
    def _load_chunk(self, trade_day:pd.Timestamp):
        self.chunk_end_day = trade_day + pd.Timedelta(days=self.chunk_days)
        indicator_placeholder = ','.join(['?'] * len(self.columns))
        query = f"""
           SELECT
             {const.TRADE_DAY},
             {const.TICKER},
             {const.SID},
             {const.IND_NAME},
             {const.IND_VALUE}
           FROM
             {const.TABLE_NAME}
           WHERE 
             {const.TRADE_DAY} >= ?
             AND {const.TRADE_DAY} <= ?
             AND {const.IND_NAME} in ({indicator_placeholder})
        """
        with self.timer_context.timer('read sql'):
          df = pd.read_sql_query(
              query,
              self.db_conn,
              params=[
                 const.format_trade_day(trade_day),
                 const.format_trade_day(self.chunk_end_day)] + self.columns)
        with self.timer_context.timer('pivot'):
          df_wide = df.pivot(
              index=[const.TRADE_DAY, const.TICKER],
              columns=const.IND_NAME,
              values=const.IND_VALUE)
        self.chunk_df = df_wide