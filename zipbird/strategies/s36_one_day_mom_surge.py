"""
   Momentum Strategy with ADX and ROC
   Universe:
     Top 1000 stocks by dollar volume
     Close price > $1
   Enter:
     one day mometum surge
     last few days are narrow range
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
from zipline.assets import Equity
from zipline.pipeline.data import USEquityPricing


class S36OneDayMOMSurge(BaseStrategy):
    
    def prepare_pipeline_columns(self, pipeline_maker:PipelineMaker):
        """Create zipline pipeline"""
        yesterday_close = USEquityPricing.close.latest
        one_day_mom = pipeline_maker.add_one_day_mom_surge(self.params['surge_percent'])
        roc = pipeline_maker.add_roc(self.params['roc_period'])
        pipeline_maker.add_atr(self.params['atr_period'])
        return (one_day_mom >= 1)

    def generate_signals(self,
                         positions:Positions,
                         pipeline_data:pd.DataFrame,
                         filtered_pipeline_data:pd.DataFrame) -> list[Signal]:
        roc = col_name.roc_name(self.params['roc_period'])
        buy_list = filtered_pipeline_data[roc].sort_values(ascending=True).dropna()
        # filter out already exist positions
        buy_list = self.get_buy_list(buy_list,
                                     positions,
                                     self.params['max_positions'],
                                     self.params['open_position_factor'])
        return [self._to_signal(stock, pipeline_data) for stock in buy_list]
    
    def _to_signal(self, stock:Equity, pipeline_data:pd.DataFrame) -> Signal:
        open_price = pipeline_data['close'][stock] * (1 - self.params['open_order_percent'] )
        return Signal.make_open_long(stock, limit_price=open_price)
