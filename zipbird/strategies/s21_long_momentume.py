import pandas as pd

from zipbird.strategy.pipeline_maker import PipelineMaker
from zipbird.strategy.strategy_executor import BaseStrategy, Signal
from zipbird.strategy import pipeline_column_names as col_name

from zipline.api import symbol
from zipline.assets import Equity
from zipline.protocol import Positions
from zipline.pipeline.data import USEquityPricing

SPX_TICKER = '$SPX'

class S21LongMOM(BaseStrategy):
    def prepare_pipeline_columns(self, pipeline_maker:PipelineMaker):
        """Create zipline pipeline"""

        pipeline_maker.add_sma(self.params['spx_sma_period'])
        fast_sma = pipeline_maker.add_sma(self.params['fast_sma_period'])
        slow_sma = pipeline_maker.add_sma(self.params['slow_sma_period'])
        pipeline_maker.add_roc(self.params['roc_period'])
        pipeline_maker.add_atr(self.params['atr_period'])
        return (fast_sma > slow_sma)

    def generate_signals(self,
                         positions:Positions,
                         pipeline_data:pd.DataFrame,
                         filtered_pipeline_data:pd.DataFrame) -> list[Signal]:
        SPX = symbol(SPX_TICKER)
        spx_sma = col_name.sma_name(self.params['spx_sma_period'])
        if pipeline_data['close'][SPX] < pipeline_data[spx_sma][SPX]:
            return []
        ROC = col_name.roc_name(self.params['roc_period'])
        buy_list = filtered_pipeline_data[ROC].sort_values(ascending=False).dropna()
        # filter out already exist positions
        buy_list = self.get_buy_list(buy_list,
                                     positions,
                                     self.params['max_positions'])
        return [Signal.make_open_long(stock) for stock in buy_list]
    