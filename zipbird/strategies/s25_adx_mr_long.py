import pandas as pd

from zipbird.strategy.pipeline_maker import PipelineMaker
from zipbird.strategy.strategy_executor import BaseStrategy, Signal
from zipbird.strategy import pipeline_column_names as col_name

from zipline.assets import Equity
from zipline.protocol import Positions
from zipline.pipeline.data import USEquityPricing


class S25ADXLongMR(BaseStrategy):
    def prepare_pipeline_columns(self, pipeline_maker:PipelineMaker):
        """Create zipline pipeline"""
        yesterday_close = USEquityPricing.close.latest

        atrp = pipeline_maker.add_atrp(self.params['atr_period'])
        sma = pipeline_maker.add_sma(self.params['sma_period'])
        adx = pipeline_maker.add_adx(self.params['adx_period'])
        rsi = pipeline_maker.add_rsi(self.params['rsi_period'])
        atr = pipeline_maker.add_atr(self.params['atr_period'])

        return (
            (yesterday_close > (sma + atr)) & 
            (atrp > self.params['atr_percent_limit']) &
            (adx > self.params['adx_lower_limit']) &
            (rsi < self.params['rsi_upper_limit'])
        )

    def generate_signals(self,
                         positions:Positions,
                         pipeline_data:pd.DataFrame,
                         filtered_pipeline_data:pd.DataFrame) -> list[Signal]:
        adx = col_name.adx_name(self.params['adx_period'])
        buy_list = filtered_pipeline_data[adx].sort_values(ascending=False).dropna()
        # filter out already exist positions
        buy_list = self.get_buy_list(buy_list,
                                     positions,
                                     self.params['max_positions'],
                                     self.params['open_position_factor'])
        return [self._to_signal(stock, pipeline_data) for stock in buy_list]
    
    def _to_signal(self, stock:Equity, pipeline_data:pd.DataFrame) -> Signal:
        open_price = pipeline_data['close'][stock] * (1 - self.params['open_order_percent'] )
        return Signal.make_open_long(stock, limit_price=open_price)