"""
Buy signal:
  cross over 200 MA
  200 MA must be pointing up
  Market filter spx > 200 MA
Close signal:
  close below 200MA for more than 2 ATR
Stop:
  5 ATR trailing stop
"""
import pandas as pd

from zipbird.strategy.pipeline_maker import PipelineMaker
from zipbird.strategy.strategy_executor import BaseStrategy, Signal
from zipbird.strategy import pipeline_column_names as col_name
from zipbird.utils import factor_utils

from zipline.api import symbol
from zipline.protocol import Positions
from zipline.pipeline.data import USEquityPricing
from zipline_norgatedata.pipelines import NorgateDataIndexConstituent

SPX_TICKER = '$SPX'

class S32Cross200MA(BaseStrategy):
    
    def prepare_pipeline_columns(self, pipeline_maker:PipelineMaker):
        """Create zipline pipeline"""
        # indexconstituent = NorgateDataIndexConstituent('S&P 500')
        # universe_screen = factor_utils.get_universe_screen(
        #         min_price=self.params['min_price'],
        #         volume_window_length=self.params['avg_volume_days'],
        #         min_avg_dollar_volume=self.params['min_avg_dollar_volume']
        #     )
        # universe_screen = (universe_screen & indexconstituent)
        # pipeline_maker.add_universe(universe_screen=universe_screen)
        pipeline_maker.add_dollar_volume_rank_universe(max_rank=1000, min_close=1, window_length=200)

        pipeline_maker.add_sma(self.params['spx_sma_period'])
        pipeline_maker.add_atr(self.params['atr_period'])
        pipeline_maker.add_roc(self.params['roc_period'])
        sma_cross = pipeline_maker.add_sma_cross(self.params['sma_period'])
        sma_trend = pipeline_maker.add_sma_trend(self.params['sma_period'])
        pipeline_maker.add_filter(
            filter=((sma_cross > 0) & (sma_trend > 0)),
            filter_name='200_sma_cross',
        )

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
    