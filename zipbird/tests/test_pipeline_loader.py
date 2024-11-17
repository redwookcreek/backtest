import sqlite3
import unittest
import pandas as pd

from zipbird.strategy.pipeline_loader import PipelineLoader
from zipbird.utils import logger_util
from zipbird.strategy import pipleine_const as const
from zipbird.strategy.pipeline_maker import PipelineMaker
from zipbird.strategy.pipeline_saver import PipelineSaver
from zipbird.basic.types import Equity

class TestStrategy:   
    def prepare_pipeline_columns(self, pipeline_maker:PipelineMaker):
        pipeline_maker.add_adx(10)
        pipeline_maker.add_atrp(20)

class TestStrategy2:   
    def prepare_pipeline_columns(self, pipeline_maker:PipelineMaker):
        pipeline_maker.add_adx(10)

DEBUG_LOGGER = logger_util.DebugLogger()
DAY1 = '2012-01-01'
DAY2 = '2012-12-31'

TICKERS = [Equity('AAPL'), Equity('TSLA')]

DF1 = pd.DataFrame({
    'atr_10': [10., 29.],
    'close': [20., 39.],
    'adx_10': [1., 9.],
}, index=TICKERS)

DF2 = pd.DataFrame({
    'atr_10': [11., 30.],
    'close': [21., 40.],
    'adx_10': [10., 90.],
}, index=TICKERS)

class TestPipelineSaver(unittest.TestCase):
    
    def setUp(self):
        self.db_conn = sqlite3.connect('file:memdb1?mode=memory&cache=shared', uri=True)
    
        self.saver = PipelineSaver(strategies=[TestStrategy()], db_conn=self.db_conn)
        self.saver.init(DEBUG_LOGGER,
                   start_day=pd.Timestamp(DAY1),
                   end_day=pd.Timestamp(DAY2))
        
        self.saver.record_pipeline_data(
            trade_day=pd.Timestamp(DAY1),
            pipeline_data=DF1)
        self.saver.record_pipeline_data(
            trade_day=pd.Timestamp(DAY2),
            pipeline_data=DF2)
        self.saver.create_index()
        
        
    def tearDown(self) -> None:
        self.db_conn.close()

    def test_load_for_trade_day(self):
        loader = PipelineLoader(strategy=TestStrategy(), db_conn=self.db_conn)

        df1 = loader.load_for_trade_day(pd.Timestamp(DAY1))
        self.assertEqual(2, len(df1))
        self.assertEqual(20, df1.loc['AAPL']['close'])
        self.assertEqual(1, df1.loc['AAPL']['adx_10'])
        self.assertEqual(9, df1.loc['TSLA']['adx_10'])
        self.assertEqual(39, df1.loc['TSLA']['close'])

        df2 = loader.load_for_trade_day(pd.Timestamp(DAY2))
        self.assertEqual(2, len(df2))

    def test_load_for_trade_day2(self):
        """A strategy will less columns"""
        loader = PipelineLoader(strategy=TestStrategy2(), db_conn=self.db_conn)
        
        df1 = loader.load_for_trade_day(pd.Timestamp(DAY1))
        self.assertEqual({'close', 'adx_10'}, set(df1.columns))