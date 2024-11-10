import pandas as pd

from zipbird.strategy.pipeline_maker import PipelineMaker
from zipbird.strategy import pipeline_column_names
from zipbird.strategy.strategy import BaseStrategy, Signal
from zipbird.utils import factor_utils, utils

import zipline
from zipline.protocol import Positions
from zipline_norgatedata.pipelines import NorgateDataUnadjustedClose
from zipline_norgatedata.pipelines import NorgateDataIndexConstituent

SPX_TICKER = '$SPX'

class S1WeeklyRotationStrategy(BaseStrategy):
    
    def __init__(self, strategy_name, params):
        super().__init__(strategy_name, params)
        self.last_balance_day = None

    def prepare_pipeline_columns(self, pipeline_maker:PipelineMaker):
        """Create zipline pipeline"""
        unadjusted_close = NorgateDataUnadjustedClose()
        indexconstituent = NorgateDataIndexConstituent('S&P 500')
        dollar_volume_rank = factor_utils.DollarVolumeRankFactor(
            window_length=self.params['dollar_volume_rank_window'],
        )
        if self.params['use_spx']:
            universe_screen = factor_utils.get_universe_screen(
                min_price=self.params['min_price'],
                volume_window_length=self.params['avg_volume_days'],
                min_avg_dollar_volume=self.params['min_avg_dollar_volume']
            )
            universe_screen = (universe_screen & indexconstituent)
        else:
            universe_screen = (
                (unadjusted_close > self.params['min_price']) & 
                (dollar_volume_rank < 1000)
            )
        pipeline_maker.add_universe(universe_screen)

        pipeline_maker.add_roc(self.params['roc_len'])
        pipeline_maker.add_rsi(self.params['rsi_len'])
        pipeline_maker.add_sma(self.params['market_filter_sma_period'])
        pipeline_maker.add_vol(self.params['vol_window'])
    
    def generate_signals(self,
                         positions:Positions,
                         pipeline_data:pd.DataFrame,
                         filtered_pipeline_data:pd.DataFrame) -> list[Signal]:
        """Generate signals
        If spx is below 200SMA, no new signals, exit all current positions.
        Otherwise, for current positions, 
        if still in top 10 ROC, keep; if out of top 10 ROC, close.
        
        If total position is below 10 after this, choose the top N ROC with
        rsi < 50
        """
        SMA = pipeline_column_names.sma_name(self.params['market_filter_sma_period'])
        ROC = pipeline_column_names.roc_name(self.params['roc_len'])
        RSI = pipeline_column_names.rsi_name(self.params['rsi_len'])
    
        today = zipline.api.get_datetime().date()
        if self.params['balance_freq'] == 'weekly':
            if today.weekday() != 0:
                # not Monday, do nothing
                return []
        elif self.params['balance_freq'] == 'monthly':
            if self.last_balance_day is not None and self.last_balance_day.month == today.month:
                # balance at begining of month
                return []
        elif self.params['balance_freq'] == 'quarterly':
            if self.last_balance_day is not None and utils.get_quarter(today) == utils.get_quarter(self.last_balance_day):
                return []
        self.last_balance_day = today
        # If market is in down trend (spx < 200MA), close all positions
        SPX = zipline.api.symbol(SPX_TICKER)
        spx_close = pipeline_data['close'][SPX]
        spx_sma_with_tolerance = pipeline_data[SMA][SPX] * ( 1- self.params['market_filter_tolerance'])
        if  spx_close < spx_sma_with_tolerance:
            return [Signal.make_close_long(p) for p in positions]
        roc = filtered_pipeline_data[ROC].dropna().sort_values(ascending=False)
        top_roc = roc[:self.params['n_of_positions']]
        # close positions falling out of top 10 ROC
        to_close = [p for p in positions if p not in top_roc]
        to_keep = [p for p in positions if p in top_roc]

        # add new positions with rsi < 50, rank by roc
        rsi = filtered_pipeline_data[RSI]
        rsi = rsi[(rsi < self.params['max_rsi'])]
        n_of_new_positions = self.params['n_of_positions'] - len(to_keep)
        
        to_open = roc.loc[(~roc.index.isin(to_keep)) & roc.index.isin(rsi.index)]
        #to_open = roc.loc[(~roc.index.isin(to_keep))]
        to_open = to_open[:n_of_new_positions].index.tolist()

        to_open, to_close, to_keep = remove_duplicates_and_make_signals(to_open, to_close, to_keep)

        if self.params['rebalance_by_vol']:
            # If rebalance by vol, adjust all positions
            to_adjust = [Signal.make_adjust_long(s) for s in to_keep]
        else:
            # otherwise, do not touch existing positions (unless 
            # for closing)
            to_adjust = []
        return to_open + to_adjust + to_close
    

def remove_duplicates_and_make_signals(to_open, to_close, to_keep):
    # there are some tickers in to_open and to_close
    # this is because we use different method to decide close
    # and open.
    to_open_set = set(to_open)
    to_close_set = set(to_close)
    both_open_and_close = to_open_set & to_close_set
    to_open = to_open_set - both_open_and_close
    to_close = to_close_set - both_open_and_close
    to_keep = to_keep + list(both_open_and_close)
    return (
        [Signal.make_open_long(s) for s in to_open],
        [Signal.make_close_long(s) for s in to_close],
        to_keep)