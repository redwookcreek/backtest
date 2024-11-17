
import sqlite3
import unittest
import pandas as pd

from zipbird.utils import logger_util
from zipbird.strategy import pipleine_const as const
from zipbird.strategy.pipeline_maker import PipelineMaker
from zipbird.strategy.pipeline_saver import MismatchDateRangeException, PipelineSaver
from zipbird.basic.types import Equity

class TestStrategy:   
    def prepare_pipeline_columns(self, pipeline_maker:PipelineMaker):
        pipeline_maker.add_adx(10)
        pipeline_maker.add_atrp(20)

class TestStrategy2:   
    def prepare_pipeline_columns(self, pipeline_maker:PipelineMaker):
        pipeline_maker.add_adx(10)
        pipeline_maker.add_atrp(20)
        pipeline_maker.add_atrp(30)

DEBUG_LOGGER = logger_util.DebugLogger()
DAY1 = '2012-01-01'
DAY2 = '2012-12-31'
DAY3 = '2022-01-01'

TICKERS = [Equity('AAPL'), Equity('TSLA')]

DF = pd.DataFrame({
    'atr_10': [10, 29],
    'close': [20, 39],
    'adx_10': [1, 9],
}, index=TICKERS)

class TestPipelineSaver(unittest.TestCase):
    
    def setUp(self):
        self.db_conn = sqlite3.connect('file:memdb1?mode=memory&cache=shared', uri=True)
    
        self.saver = PipelineSaver(strategies=[TestStrategy()], db_conn=self.db_conn)
        self.saver.init(DEBUG_LOGGER,
                   start_day=pd.Timestamp(DAY1),
                   end_day=pd.Timestamp(DAY2))
        self.saver.create_index()
        
    def tearDown(self) -> None:
        self.db_conn.close()

    def test_init_metadata(self):
        self.assertEqual(DAY1, self.saver._get_metadata('start_day'))
        self.assertEqual(DAY2, self.saver._get_metadata('end_day'))
        self.assertEqual('close,adx_10,atrp_20', self.saver._get_metadata('columns'))

    def test_date_range_mismatch(self):        
        saver2 = PipelineSaver(strategies=[TestStrategy()],
                               db_conn=self.db_conn,
                               start_fresh=False)
        # end day mismatch
        with self.assertRaises(MismatchDateRangeException):
            saver2.init(DEBUG_LOGGER, pd.Timestamp(DAY1), pd.Timestamp(DAY3))
        
        # start day mismatch
        with self.assertRaises(MismatchDateRangeException):
            saver2.init(DEBUG_LOGGER, pd.Timestamp(DAY3), pd.Timestamp(DAY1))

    def test_column_remove(self):
        saver2 = PipelineSaver(strategies=[TestStrategy()],
                               db_conn=self.db_conn,
                               start_fresh=False)
        
        saver2.init(DEBUG_LOGGER, pd.Timestamp(DAY1), pd.Timestamp(DAY2))
        self.assertEqual('', saver2._get_metadata('columns'))

    def test_column_remove2(self):
        saver2 = PipelineSaver(strategies=[TestStrategy2()],
                               db_conn=self.db_conn,
                               start_fresh=False)
        
        saver2.init(DEBUG_LOGGER, pd.Timestamp(DAY1), pd.Timestamp(DAY2))
        self.assertEqual('atrp_30', saver2._get_metadata('columns'))

    def get_all_records(self):
        cursor = self.db_conn.cursor()
        return cursor.execute(f'SELECT * FROM {const.TABLE_NAME}').fetchall()
    
    def test_record_pipeline_data_first_time(self):
        self.saver.record_pipeline_data(
            pd.Timestamp(DAY1),
            DF)
        
        all_records = self.get_all_records()
        self.assertEqual(6, len(all_records))

    def test_record_pipeline_data_second_time(self):
        self.saver.record_pipeline_data(
            pd.Timestamp(DAY1),
            DF)
        
        all_records = self.get_all_records()
        self.assertEqual(6, len(all_records))
        
        self.saver.record_pipeline_data(pd.Timestamp(DAY1), DF)
        self.assertEqual(6, len(self.get_all_records()))
        
        self.saver.record_pipeline_data(pd.Timestamp(DAY2), DF)
        self.assertEqual(12, len(self.get_all_records()))