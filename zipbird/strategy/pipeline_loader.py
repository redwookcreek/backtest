import pandas as pd
import sqlite3

from zipbird.strategy.pipeline_maker import PipelineMaker
from zipbird.strategy import pipleine_const as const
from zipbird.strategy.strategy_executor import StrategyExecutor

class DBFileNotFoundException(Exception):
    pass

class PipelineLoader:
    def __init__(self, strategy:StrategyExecutor, db_conn:sqlite3.Connection):
        self.strategy = strategy
        pipeline_maker = PipelineMaker()
        strategy.prepare_pipeline_columns(pipeline_maker)
        
        self.columns = list(pipeline_maker.get_columns())
        self.db_conn = db_conn

    def init(self, debug_logger):
        self.strategy.init(debug_logger=debug_logger)
        self.debug_logger = debug_logger
        
    def load_for_trade_day(self, trade_day: pd.Timestamp) -> pd.DataFrame:
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
             {const.TRADE_DAY} = ?
             AND {const.IND_NAME} in ({indicator_placeholder})
        """
        df = pd.read_sql_query(query,
                               self.db_conn,
                               params=[const.format_trade_day(trade_day)] + self.columns)
        df_wide = df.pivot(index=const.SID,
                           columns=const.IND_NAME,
                           values=const.IND_VALUE)
        return df_wide