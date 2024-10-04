from zipline.pipeline.filters import Filter, CustomFilter
from zipline.pipeline.factors import basic as basic_factors
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.factors import CustomFactor
from zipline.utils import math_utils as zipline_math_utils
from zipline.api import sid, symbol
from zipline_norgatedata.pipelines import NorgateDataUnadjustedClose

import talib
import numpy as np


def get_universe_screen(
        min_price:float,
        volume_window_length: int,
        min_avg_dollar_volume: int) -> Filter:
    """Returns a Filter to filter stock universe by price and volume"""
    dollar_volume = basic_factors.AverageDollarVolume(
        window_length=volume_window_length)
    unadjusted_close = NorgateDataUnadjustedClose()
    universe_screen = (
        (dollar_volume > min_avg_dollar_volume) &
        (unadjusted_close > min_price))
    return universe_screen

class DollarVolumeRankFactor(CustomFactor):
    inputs = (USEquityPricing.close, USEquityPricing.volume)
    window_length = 50
    
    def compute(self, today, assets, out, close, volume):
        dollar_volume = zipline_math_utils.nansum(close * volume, axis=0) / len(close)
        out[:] = len(dollar_volume) - dollar_volume.argsort().argsort() + 1


class RSIFactor(CustomFactor):
    inputs = (USEquityPricing.close,)  
    params = {'rsi_len' : 3,}
    window_length = 100

    def compute(self, today, assets, out, closes, rsi_len):
        def rsi_func(ts):
            return talib.RSI(ts, timeperiod=rsi_len)[-1]
        if closes.size > 0:
            rsi = np.apply_along_axis(func1d=rsi_func, axis=0, arr=closes)
            out[:] = rsi


def _high_low_close_loop(highs, lows, closes, timeperiod, func):
    result = []
    for h, l, c in zip(highs.T, lows.T, closes.T):
        try:
            result.append(func(h, l, c, timeperiod=timeperiod)[-1])
        except:
            result.append(np.nan)
    return result


def _close_loop(closes, timeperiod, func):
    result = []
    for c in closes.T:
        try:
            result.append(func(c, timeperiod=timeperiod)[-1])
        except:
            result.append(np.nan)
    return result

class ATRFactor(CustomFactor):
    inputs = (USEquityPricing.close, USEquityPricing.high, USEquityPricing.low,)  
    params = ('atr_len',)
    window_length = 100

    def compute(self, today, assets, out, closes, highs, lows, atr_len):
        out[:] = _high_low_close_loop(highs, lows, closes, atr_len, talib.ATR)



class ATRPFactor(CustomFactor):
    inputs = (USEquityPricing.close, USEquityPricing.high, USEquityPricing.low,)  
    params = ('atr_len',)
    window_length = 100

    def compute(self, today, assets, out, closes, highs, lows, atr_len):
        out[:] = _high_low_close_loop(highs, lows, closes, atr_len, talib.NATR)


class ADXFactor(CustomFactor):
    inputs = (USEquityPricing.close, USEquityPricing.high, USEquityPricing.low,)  
    params = ('adx_len',)
    window_length = 100

    def compute(self, today, assets, out, closes, highs, lows, adx_len):
        out[:] = _high_low_close_loop(highs, lows, closes, adx_len, talib.ADX)


class ROCFactor(CustomFactor):
    inputs = (USEquityPricing.close,)
    params = ('roc_len',)
    window_length = 100
    def compute(self, today, assets, out, closes, roc_len):
        out[:] = _close_loop(closes, timeperiod=roc_len, func=talib.ROCP)


class StdFactorPercent(CustomFactor):
    inputs = (USEquityPricing.close,)
    window_length = 50
    def compute(self, today, assets, out, closes):
        std = zipline_math_utils.nanstd(closes, axis=0)
        close_sma = zipline_math_utils.nanmean(closes, axis=0)
        out[:] = (std / close_sma) * 100


class StdFactorPercentileRank(CustomFactor):
    inputs = (USEquityPricing.close,)
    window_length = 50
    
    def compute(self, today, assets, out, closes):
        std = zipline_math_utils.nanstd(closes, axis=0)
        close_sma = zipline_math_utils.nanmean(closes, axis=0)
        volatility = (std / close_sma) * 100
        
        # Calculate percentile ranks
        # This gives a value between 0 and 1, where 1 is the highest volatility
        percentile_ranks = (volatility.argsort().argsort() + 1) / len(volatility)
        
        out[:] = percentile_ranks * 100

class ConsecutiveUpFactor(CustomFactor):
    """Consecutive up days.

    out[stock] = true if the stock has n consecutive up days.
    where n = window_length - 1
    """
    inputs = (USEquityPricing.open,
              USEquityPricing.close)
    window_length = 3

    def compute(self, today, assets, out, open, close):
        changes = np.diff(close, axis=0)
        green_days = (close > open)[1:]
        out[:] = (np.all(changes > 0, axis=0) &
                  np.all(green_days, axis=0))
        

class MaxInWindowFactor(CustomFactor):
    """Max price in last N days"""
    inputs = (USEquityPricing.close,)
    window_length = 50

    def compute(self, today, assets, out, closes):
        out[:] = np.max(closes, axis=0)

class TickerFilter(CustomFilter):
    """Factor that set to true only if the asset is one of the input tickers"""
    inputs = []
    window_length = 1
    params = ('tickers',)

    def compute(self, today, assets, out, tickers):
        sids = [symbol(t).sid for t in tickers]
        out[:] = np.isin(assets, sids)

