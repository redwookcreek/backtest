import sqlite3
import unittest
import pandas as pd
from parameterized import parameterized

from zipbird.utils.timer_context import TimerContext
from zipbird.strategy.pipeline_loader import PipelineLoader
from zipbird.utils import logger_util
from zipbird.strategy.pipeline_maker import PipelineMaker
from zipbird.strategy.pipeline_saver import PipelineSaver
from zipbird.basic.types import Equity

class TestStrategy:   
    def init(self, *arg, **kwarg):
        pass
    def prepare_pipeline_columns(self, pipeline_maker:PipelineMaker):
        pipeline_maker.add_adx(10)
        pipeline_maker.add_atrp(20)

class TestStrategy2:
    def init(self, *arg, **kwarg):
        pass
    def prepare_pipeline_columns(self, pipeline_maker:PipelineMaker):
        pipeline_maker.add_adx(10)

DEBUG_LOGGER = logger_util.DebugLogger()
DAY1 = '2012-01-01'
DAY2 = '2012-12-31'

AAPL = Equity('AAPL')
AAPL.sid = 1
TSLA = Equity('TSLA')
TSLA.sid = 2

TICKERS = [AAPL, TSLA]

DF1 = pd.DataFrame({
    'atrp_20': [10., 29.],
    'close': [20., 39.],
    'adx_10': [1., 9.],
}, index=TICKERS)

DF2 = pd.DataFrame({
    'atrp_20': [11., 30.],
    'close': [21., 40.],
    'adx_10': [10., 90.],
}, index=TICKERS)

class TestPipelineLoader(unittest.TestCase):
    
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
        self.timer_context = TimerContext()
        
        
    def tearDown(self) -> None:
        try:
            self.db_conn.rollback()
        except:
            pass
        finally:
            self.db_conn.close()

    def list_all_tables(self) -> None:
        cursor = self.db_conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        for table in tables:
            print(f'------table {table} -----')
            cursor.execute("pragma table_info('{}')".format(table))
            columns = [tuple(row) for row in cursor.fetchall()]
            print(f'colums: {columns}')
    @parameterized.expand([1, 10000])
    def test_load_for_trade_day(self, chunk_days):
        loader = PipelineLoader(strategy=TestStrategy(), db_conn=self.db_conn, chunk_days=chunk_days)
        loader.init(debug_logger=DEBUG_LOGGER, timer_context=self.timer_context)
        df1 = loader.load_for_trade_day(pd.Timestamp(DAY1))
        self.assertEqual(2, len(df1))
        self.assertEqual(20, df1.loc[AAPL.sid]['close'])
        self.assertEqual(1, df1.loc[AAPL.sid]['adx_10'])
        self.assertEqual(10., df1.loc[AAPL.sid]['atrp_20'])
        self.assertEqual(9, df1.loc[TSLA.sid]['adx_10'])
        self.assertEqual(39, df1.loc[TSLA.sid]['close'])
        self.assertEqual(29., df1.loc[TSLA.sid]['atrp_20'])

        df1 = loader.load_for_trade_day(pd.Timestamp(DAY2))
        self.assertEqual(2, len(df1))
        self.assertEqual(21, df1.loc[AAPL.sid]['close'])
        self.assertEqual(10., df1.loc[AAPL.sid]['adx_10'])
        self.assertEqual(11., df1.loc[AAPL.sid]['atrp_20'])
        self.assertEqual(90., df1.loc[TSLA.sid]['adx_10'])
        self.assertEqual(40, df1.loc[TSLA.sid]['close'])
        self.assertEqual(30., df1.loc[TSLA.sid]['atrp_20'])

    def test_load_for_trade_day(self):
        loader = PipelineLoader(strategy=TestStrategy(), db_conn=self.db_conn, chunk_days=20000)
        loader.init(debug_logger=DEBUG_LOGGER, timer_context=self.timer_context)

        df1 = loader.load_for_trade_day(pd.Timestamp(DAY1))
        self.assertEqual(2, len(df1))
        self.assertEqual(20, df1.loc[AAPL.sid]['close'])
        self.assertEqual(1, df1.loc[AAPL.sid]['adx_10'])
        self.assertEqual(9, df1.loc[TSLA.sid]['adx_10'])
        self.assertEqual(39, df1.loc[TSLA.sid]['close'])

        df2 = loader.load_for_trade_day(pd.Timestamp(DAY2))
        self.assertEqual(2, len(df2))

    def test_load_for_trade_day2(self):
        """A strategy will less columns"""
        loader = PipelineLoader(strategy=TestStrategy2(), db_conn=self.db_conn)
        loader.init(debug_logger=DEBUG_LOGGER, timer_context=self.timer_context)

        df1 = loader.load_for_trade_day(pd.Timestamp(DAY1))
        self.assertEqual({'close', 'adx_10'}, set(df1.columns))
