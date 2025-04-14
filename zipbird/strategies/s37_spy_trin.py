""" 
 TRIN Strategy
  page 100 of Short Term Tradning Strategies That Work
  
 - Buy
	- SPY > 200MA
	- RSI(SPY, 2) < 50
	- TRIN above 1 for 3 days
 - Sell
	- RSI(SPY, 2) > 65
"""
import pandas as pd
import numpy as np

from zipbird.strategy.pipeline_maker import PipelineMaker
from zipbird.strategy.strategy_executor import BaseStrategy, Signal
from zipbird.strategy import pipeline_column_names as col_name

import zipline
from zipline.api import symbol
from zipline.protocol import Positions
from zipline.assets import Equity
from zipline.pipeline.data import USEquityPricing

TRIN_TICKER = '#NYSETRIN'
ALLOWED_TICKERS = ['SPY']
class S37SpyTrin(BaseStrategy):    
    def prepare_pipeline_columns(self, pipeline_maker:PipelineMaker):
        """Create zipline pipeline"""
        yesterday_close = USEquityPricing.close.latest
        
        rsi = pipeline_maker.add_rsi(self.params['rsi_period'])
        sma = pipeline_maker.add_sma(self.params['sma_period'])
        pipeline_maker.add_consecutive_days_above_threshold(
            self.params['trin_days_above'],
            self.params['trin_threshold'])
        
        return (
            (yesterday_close > sma) & 
            (rsi < self.params['rsi_lower_limit'])
        )

    def generate_signals(self,
                         positions:Positions,
                         pipeline_data:pd.DataFrame,
                         filtered_pipeline_data:pd.DataFrame) -> list[Signal]:
        signals = []
        RSI = col_name.rsi_name(self.params['rsi_period'])
        print(f'------{zipline.api.get_datetime()}-------')
        print(pipeline_data)
        for pos in positions:
            if pipeline_data[RSI][pos] > self.params['rsi_upper_limit']:
                signals.append(Signal.make_close_long(pos))
    
        TRIN = symbol(TRIN_TICKER)
        TRIN_COL = col_name.consecutive_days_above_threshold(
            self.params['trin_days_above'],
            self.params['trin_threshold']
        )
        trin_above = pipeline_data[TRIN_COL][TRIN]
        if np.isnan(trin_above) or trin_above < 1:
            return signals
        allowed_equities = [symbol(s) for s in ALLOWED_TICKERS]
        buy_list = filtered_pipeline_data[
            ~filtered_pipeline_data.index.isin(positions.keys()) &
             filtered_pipeline_data.index.isin(allowed_equities)].index.tolist()
        signals.extend([Signal.make_open_long(stock) for stock in buy_list])
        return signals
    