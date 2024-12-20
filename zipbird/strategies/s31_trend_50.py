"""
   Trending following 50
   Universe:
     sp500 components
     20 day dollar volume > 50M
   Enter:
     Close of SPY > SMA(50) + 2 * ATR(20)
     Close > highest of 50 day closes
   Exit:
     Stop loss 5 x ATR(20)
     Trailing stop 17.5%
   Ranking:
     ROC(50)
   Position Size:
     10% of total portfolio or 2% risk
"""
import pandas as pd

from zipbird.strategy.pipeline_maker import PipelineMaker, IndexNames
from zipbird.strategy.strategy_executor import BaseStrategy, Signal
from zipbird.strategy import pipeline_column_names as col_name
from zipbird.utils import factor_utils

from zipline.api import symbol
from zipline.protocol import Positions
from zipline.pipeline.data import USEquityPricing
from zipline_norgatedata.pipelines import NorgateDataIndexConstituent

SPX_TICKER = '$SPX'

class S31Trend50(BaseStrategy):
    
    def make_pipeline(self, pipeline_maker:PipelineMaker):        
        # indexconstituent = NorgateDataIndexConstituent('S&P 500')
        # universe_screen = factor_utils.get_universe_screen(
        #         min_price=self.params['min_price'],
        #         volume_window_length=self.params['avg_volume_days'],
        #         min_avg_dollar_volume=self.params['min_avg_dollar_volume']
        #     )
        # universe_screen = (universe_screen & indexconstituent)        
        # pipeline_maker.add_universe(universe_screen=universe_screen)
        pipeline_maker.add_dollar_volume_rank_universe(
            max_rank=1000, min_close=1, window_length=200)
        filter = self.prepare_pipeline_columns(pipeline_maker)
        pipeline_maker.add_filter(
            filter=filter,
            filter_name='50_high_filter',
        )

    def prepare_pipeline_columns(self, pipeline_maker:PipelineMaker):
        """Create zipline pipeline"""
        
        yesterday_close = USEquityPricing.close.latest

        pipeline_maker.add_sma(self.params['spx_sma_period'])
        pipeline_maker.add_roc(self.params['roc_period'])
        pipeline_maker.add_atr(self.params['atr_period'])
        pipeline_maker.add_index_consititue(IndexNames.SP500)
        pipeline_maker.add_dollar_volume_rank(period=200)
        high_in_window = pipeline_maker.add_max_in_window(self.params['roc_period'])
        
        return (yesterday_close >= high_in_window)
    
    def filter_pipeline_data(self, pipeline_data:pd.DataFrame) -> pd.DataFrame:
        high = col_name.max_in_window(self.params['roc_period'])
        dv_rank = col_name.dollar_volume_rank(200)
        d = pipeline_data
        return d[(d['close'] >= d[high]) & (d[dv_rank] <= 1000)]

    def generate_signals(self,
                         positions:Positions,
                         pipeline_data:pd.DataFrame,
                         filtered_pipeline_data:pd.DataFrame) -> list[Signal]:
        SPX = symbol(SPX_TICKER)
        spx_sma = col_name.sma_name(self.params['spx_sma_period'])
        spx_atr = col_name.atr_name(self.params['atr_period'])
        sma = pipeline_data[spx_sma][SPX]
        atr = self.params['spx_atr_threshold_multiple'] * pipeline_data[spx_atr][SPX]
        if pipeline_data['close'][SPX] < sma + atr:
            return []
        ROC = col_name.roc_name(self.params['roc_period'])
        buy_list = filtered_pipeline_data[ROC].sort_values(ascending=False).dropna()
        # filter out already exist positions
        buy_list = self.get_buy_list(buy_list,
                                     positions,
                                     self.params['max_positions'])
        return [Signal.make_open_long(stock) for stock in buy_list]
    