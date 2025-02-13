"""
   Momentum Strategy with ADX and ROC
   Universe:
     Top 1000 stocks by dollar volume
     Close price > $1
   Enter:
     close above 100 day highest high
     ADX(14) > 45
     Rank by ROC(400) highest first
   Exit:
     Initial stop 20%
     Trailing stop 20%
"""
import pandas as pd

from zipbird.strategy.pipeline_maker import PipelineMaker
from zipbird.strategy.strategy_executor import BaseStrategy, Signal
from zipbird.strategy import pipeline_column_names as col_name
from zipbird.utils import factor_utils

from zipline.protocol import Positions

class S35ShortTrend(BaseStrategy):
    
    def make_pipeline(self, pipeline_maker:PipelineMaker):
        # Set up universe of top 1000 stocks by dollar volume
        pipeline_maker.add_dollar_volume_rank_universe(
            max_rank=1000, min_close=1, window_length=200)
        
        # Add our indicators
        self.prepare_pipeline_columns(pipeline_maker)

    def prepare_pipeline_columns(self, pipeline_maker:PipelineMaker):
        """Create zipline pipeline with ADX and ROC indicators"""
        
        # Add technical indicators
        pipeline_maker.add_adx(self.params['adx_period'])
        pipeline_maker.add_roc(self.params['roc_period'])
        pipeline_maker.add_atr(self.params['atr_period'])
        pipeline_maker.add_max_in_window(self.params['highest_high_period'])
        
    def filter_pipeline_data(self, pipeline_data:pd.DataFrame) -> pd.DataFrame:
        """Filter stocks based on ADX and ROC thresholds"""
        adx = col_name.adx_name(self.params['adx_period'])
        high = col_name.max_in_window(self.params['highest_high_period'])

        d = pipeline_data
        # Filter for stocks with high ADX (strong trend) and positive ROC (upward momentum)
        return d[(d[adx] > self.params['adx_threshold']) & 
                (d['close'] >= d[high])]

    def generate_signals(self,
                         positions:Positions,
                         pipeline_data:pd.DataFrame,
                         filtered_pipeline_data:pd.DataFrame) -> list[Signal]:
        """Generate trading signals based on our momentum strategy"""
        
        if filtered_pipeline_data.empty:
            return []
            
        # Combine ADX and ROC scores for ranking
        roc = col_name.roc_name(self.params['roc_period'])
        
        # Order by orc descending
        buy_list = filtered_pipeline_data[roc].sort_values(ascending=False)
        # Filter out existing positions and limit to max positions
        buy_list = self.get_buy_list(buy_list,
                                     positions,
                                     self.params['max_positions'])
        
        return [Signal.make_open_long(stock) for stock in buy_list]
