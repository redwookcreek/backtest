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

import zipline
from zipline.protocol import Positions
from zipline.pipeline.data import USEquityPricing

SPX_TICKER = '$SPX'
class S34MOM(BaseStrategy):
    
    def prepare_pipeline_columns(self, pipeline_maker:PipelineMaker):
        """Create zipline pipeline with ADX and ROC indicators"""
        yesterday_close = USEquityPricing.close.latest
        pipeline_maker.add_sma(self.params['market_filter_sma_period'])
        sma = pipeline_maker.add_sma(self.params['sma_period'])
        pipeline_maker.add_roc(self.params['roc_period'])
        pipeline_maker.add_atr(self.params['atr_period'])
        crossed_high = pipeline_maker.add_cross_last_high_with_large_green_bar(
            period=self.params['highest_high_period'],
            close_percent=self.params['cross_high_close_percent'],
            green_bar_limit=self.params['green_bar_limit']
        )
        return ((crossed_high >= 0)
                & (crossed_high < 1) 
                #& (yesterday_close > sma)
                )

    def generate_signals(self,
                         positions:Positions,
                         pipeline_data:pd.DataFrame,
                         filtered_pipeline_data:pd.DataFrame) -> list[Signal]:
        """Generate trading signals based on our momentum strategy"""
        closing_signals = []
        # SMA = col_name.sma_name(self.params['sma_period'])

        # # close all positions if market is below 200MA
        # MARKET_SMA = col_name.sma_name(self.params['market_filter_sma_period'])
        # SPX_ASSET = zipline.api.symbol(SPX_TICKER)
        # if pipeline_data['close'][SPX_ASSET] < 0.98 * pipeline_data[MARKET_SMA][SPX_ASSET]:
        #     return [Signal.make_close_long(p) for p in positions]
        
        # for p in positions:
        #     if pipeline_data['close'][p] < pipeline_data[SMA][p]:
        #         closing_signals.append(Signal.make_close_long(p))

        if filtered_pipeline_data.empty:
            return closing_signals

        # Combine ADX and ROC scores for ranking
        roc = col_name.roc_name(self.params['roc_period'])
        
        # Order by orc descending
        buy_list = filtered_pipeline_data[roc].sort_values(ascending=False)
        # Filter out existing positions and limit to max positions
        buy_list = self.get_buy_list(buy_list,
                                     positions,
                                     self.params['max_positions'])
        signals = []
        for stock in buy_list:
            #stop_price = filtered_pipeline_data.at[stock, 'close'] * (1 + self.params['open_stop_percent'])
            #signals.append(Signal.make_open_long(stock, stop_price=stop_price))
            signals.append(Signal.make_open_long(stock))
        return signals + closing_signals
