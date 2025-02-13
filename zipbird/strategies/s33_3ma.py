"""
Buy signal:
  Prices was consolidating in last N periods
    - 10 and 20 MA crossed 2 or more times
    - both 10 and 20 MA was above 50 MA
close signal
  Trailing stop at 10MA and 20MA (close half at each)
"""
import pandas as pd

from zipbird.strategy.pipeline_maker import PipelineMaker
from zipbird.strategy.strategy_executor import BaseStrategy, Signal
from zipbird.strategy import pipeline_column_names as col_name

from zipline.api import symbol
from zipline.protocol import Positions
from zipline.pipeline.data import USEquityPricing

SPX_TICKER = '$SPX'

class S33MAConsolidation(BaseStrategy):
    
    def make_pipeline(self, pipeline_maker:PipelineMaker):        
        pipeline_maker.add_dollar_volume_rank_universe(max_rank=1000, min_close=1, window_length=200)
        filter = self.prepare_pipeline_columns(pipeline_maker)
        pipeline_maker.add_filter(
            filter=filter,
            filter_name='200_sma_cross',
        )
        
    def prepare_pipeline_columns(self, pipeline_maker:PipelineMaker):
        """Create zipline pipeline"""
        yesterday_close = USEquityPricing.close.latest

        pipeline_maker.add_sma(self.params['slow_ma'])
        pipeline_maker.add_sma(self.params['fast_ma'])
        pipeline_maker.add_sma(self.params['master_ma'])
        pipeline_maker.add_roc(self.params['roc_period'])
        pipeline_maker.add_atr(self.params['atr_period'])

        cross_count = pipeline_maker.add_sma_cross_times(
            fast=self.params['fast_ma'],
            slow=self.params['slow_ma'],
            master=self.params['master_ma'],
            period=self.params['check_period'],
        )
        high_in_window = pipeline_maker.add_max_in_window(period=self.params['check_period'])

        return ((yesterday_close >= high_in_window) &
                (cross_count > 2))

    def generate_signals(self,
                         positions:Positions,
                         pipeline_data:pd.DataFrame,
                         filtered_pipeline_data:pd.DataFrame) -> list[Signal]:
        print('-----------pipeline_data-----------------')
        print(pipeline_data)
        print('-----------filtered_pipeline_data-----------------')
        print(filtered_pipeline_data)
        ROC = col_name.roc_name(self.params['roc_period'])
        buy_list = filtered_pipeline_data[ROC].sort_values(ascending=False).dropna()
        # filter out already exist positions
        buy_list = self.get_buy_list(buy_list,
                                     positions,
                                     self.params['max_positions'])
        sell_list = []
        for asset in positions:
            close = pipeline_data['close'][asset]
            master_ma = pipeline_data[col_name.sma_name(self.params['master_ma'])][asset]
            if  close < master_ma :
                sell_list.append(asset)
        return (
            [Signal.make_open_long(stock) for stock in buy_list] + 
            [Signal.make_close_long(stock) for stock in sell_list]
        )
    