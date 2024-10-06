import pandas as pd

from zipbird.strategy.pipeline_maker import PipelineMaker
from zipbird.strategy.strategy_executor import BaseStrategy, Signal
from zipbird.strategy import pipeline_column_names as col_name

from zipline.assets import Equity
from zipline.protocol import Positions
from zipline.pipeline.data import USEquityPricing

class S2MRLong(BaseStrategy):
    
    def prepare_pipeline_columns(self, pipeline_maker:PipelineMaker):
        """Create zipline pipeline"""
        yesterday_close = USEquityPricing.close.latest

        pipeline_maker.add_dollar_volume_rank_universe(
            min_close=self.params['min_price'],
            max_rank=self.params['dollar_volume_rank_max'],
            window_length=self.params['dollar_volume_rank_window'],
        )
        sma = pipeline_maker.add_sma(self.params['sma_period'])
        rsi = pipeline_maker.add_rsi(self.params['rsi_period'])
        atr = pipeline_maker.add_atr(self.params['atr_period'])
        adx = pipeline_maker.add_adx(self.params['adx_period'])
        atrp = pipeline_maker.add_atrp(self.params['atr_period'])
        pipeline_maker.add_filter(
            filter=(
                (yesterday_close > sma) &
                (rsi >= self.params['rsi_lower_limit']) &
                (rsi <= self.params['rsi_upper_limit']) &
                (atrp >= self.params['atr_percent_limit']) &
                (adx >= self.params['adx_lower_limit']) &
                (adx <= self.params['adx_upper_limit'])
            ),
            filter_name='mr_filter',
        )

    def generate_signals(self,
                         positions:Positions,
                         pipeline_data:pd.DataFrame,
                         filtered_pipeline_data:pd.DataFrame) -> list[Signal]:
        RSI = col_name.rsi_name(self.params['rsi_period'])
        buy_list = filtered_pipeline_data[RSI].sort_values(ascending=True).dropna()
        # filter out already exist positions
        buy_list = self.get_buy_list(buy_list,
                                     positions,
                                     self.params['max_positions'],
                                     self.params['open_position_factor'])
        return [self._to_signal(stock, pipeline_data) for stock in buy_list]
    
    def _to_signal(self, stock:Equity, pipeline_data:pd.DataFrame) -> Signal:
        open_price = pipeline_data['close'][stock] * (1 - self.params['open_order_percent'] )
        return Signal.make_open_long(stock, limit_price=open_price)