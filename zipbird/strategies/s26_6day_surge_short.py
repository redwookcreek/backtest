import pandas as pd

from zipbird.strategy.pipeline_maker import PipelineMaker
from zipbird.strategy.strategy_executor import BaseStrategy, Signal
from zipbird.strategy import pipeline_column_names as col_name

from zipline.assets import Equity
from zipline.protocol import Positions


class S26SixDaySurgeShort(BaseStrategy):
    def prepare_pipeline_columns(self, pipeline_maker:PipelineMaker):
        """Create zipline pipeline"""
        roc = pipeline_maker.add_roc(self.params['roc_period'])
        pipeline_maker.add_atr(self.params['atr_period'])
        consecutive_up = pipeline_maker.add_consecutive_up(self.params['days_up'])

        return (
            (roc >= self.params['last_6day_min_up'] ) &
            (consecutive_up >= 1)
        )

    def generate_signals(self,
                         positions:Positions,
                         pipeline_data:pd.DataFrame,
                         filtered_pipeline_data:pd.DataFrame) -> list[Signal]:
        ROC = col_name.roc_name(self.params['roc_period'])
        buy_list = filtered_pipeline_data[ROC].sort_values(ascending=False).dropna()
        # filter out already exist positions
        buy_list = self.get_buy_list(buy_list,
                                     positions,
                                     self.params['max_positions'],
                                     self.params['open_position_factor'])
        return [self._to_signal(stock, pipeline_data) for stock in buy_list]
    
    def _to_signal(self, stock:Equity, pipeline_data:pd.DataFrame) -> Signal:
        open_price = pipeline_data['close'][stock] * (1 + self.params['open_order_percent'] )
        return Signal.make_open_short(stock, limit_price=open_price)