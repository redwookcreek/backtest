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


class StdPercentileFactor(CustomFactor):
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
        
class MomentumSurgeFactor(CustomFactor):
    """Single day momentum surge.

    1. closed more than x% comparing to last day
    2. a large white candle (close > open, close within 20% of high)
    """
    inputs = (USEquityPricing.open,
              USEquityPricing.high,
              USEquityPricing.low,
              USEquityPricing.close)
    window_length = 3
    params = ('surge_percent', )

    def compute(self, today, assets, out, open, high, low, close, surge_percent):
        daily_returns = close[-1] / close[-2] - 1
    
        # Condition 1: Check if the stock closed more than surge_percent% compared to last day
        surge_condition = daily_returns > surge_percent
    
        # Condition 2: Check for a large white candle
        # - Close > Open (white/bullish candle)
        # - Close within 20% of high (closed near the high of the day)
        white_candle = close[-1] > open[-1]
        close_near_high = (high[-1] - close[-1]) / (high[-1] - low[-1]) < 0.2
    
        # Combine conditions
        out[:] = surge_condition & white_candle & close_near_high


class MaxInWindowFactor(CustomFactor):
    """Max price in last N days"""
    inputs = (USEquityPricing.close,)
    window_length = 50

    def compute(self, today, assets, out, closes):
        out[:] = np.max(closes, axis=0)

def _get_last_cross_bar(assets, closes):
    """Calculate when the stock closed above last N period High"""
    # Calculate the cumulative maximum (expanding window) at each time step
    # This gives the highest high up to each point in time
    cumulative_max = np.maximum.accumulate(closes, axis=0)
    
    # Shift the cumulative max by 1 period to compare current price with previous high
    # First row will be zeros (we'll ignore it in the comparison)
    prev_cumulative_max = np.roll(cumulative_max, 1, axis=0)
    prev_cumulative_max[0] = 0
    
    # Find where closing price crossed above the previous highest high
    # True when current close > previous highest high
    crossings = closes > prev_cumulative_max
    result = np.full(len(assets), -1)

    # For each asset, find the index of the most recent crossing
    for i in range(len(assets)):
        # Find indices where crossings occurred for this asset
        cross_indices = np.where(crossings[:, i])[0]
        
        if len(cross_indices) > 0:
            # Get the most recent crossing index (highest index value)
            most_recent_cross = cross_indices[-1]
            # Convert to "bars ago" (0 = most recent bar)
            result[i] = closes.shape[0] - 1 - most_recent_cross
    return result

class LastCrossedHighestHighFactor(CustomFactor):
    """
    Find the last bar when price crossed above the highest high in last N periods
    Returns the bar count of how many bars ago this happened (0 = current bar)
    """
    inputs = (USEquityPricing.close,)
    window_length = 50

    def compute(self, today, assets, out, closes):
       out[:] = _get_last_cross_bar(assets, closes)


def _is_large_green_bar(opens, highs, lows, closes, close_percent, green_bar_limit):
    """Returns a np array to indicate when a stock is having a large green bar"""
    prev_closes = np.roll(closes, 1, axis=0)
    # First row should be zeroed to avoid false comparisons
    prev_closes[0] = 0
    
    # Calculate if the day is a large green bar
    # 1. Calculate close percent (current close vs previous close)
    close_percent_change = np.full_like(closes, np.nan, dtype=float)  # Initialize with NaN for clarity
    np.divide(closes - prev_closes, prev_closes, out=close_percent_change, where=prev_closes != 0)

    # For indices where prev_closes was zero, close_percent_change will remain NaN
    # 2. Calculate if bar has proper green bar characteristics
    bar_range = highs - lows
    top_wick = highs - closes
    bottom_wick = opens - lows
    
    # A proper green bar has small wicks relative to the bar range
    small_wicks = (top_wick <= green_bar_limit * bar_range) & (bottom_wick <= green_bar_limit * bar_range)
    
    # 3. Combine criteria for a large green bar
    large_green_bars = (close_percent_change >= close_percent) & small_wicks & (closes > opens)
    return large_green_bars

class LastCrossWithLargeGreenBar(LastCrossedHighestHighFactor):
    """
    Find the lart bar when price crossed above highest high in last N periods
    with a large green bar
    Returns the bar count of how may bars ago this happened (0= current bar, -1 for did not happen)
    """
    inputs = (USEquityPricing.open, USEquityPricing.high, USEquityPricing.low, USEquityPricing.close,)
    window_length = 50
    # close_percent = 0.04 the cross bar must be 4% higher than last close
    # green_bar_limit = 0.2, high - close and open - low >= 20% of the high - low
    params = ('close_percent', 'green_bar_limit')
    def compute(self, today, assets, out, opens, highs, lows, closes, close_percent, green_bar_limit):
        super().compute(today, assets, out, closes)

        # First get the last crossing bar for each asset
        last_crosses = out.copy()
        
        # Initialize output array with -1 (pattern not found)
        out[:] = -1
        
        # Calculate previous day's close
        large_green_bars = _is_large_green_bar(
            opens=opens,
            highs=highs,
            lows=lows,
            closes=closes,
            close_percent=close_percent,
            green_bar_limit=green_bar_limit
        )
        # For each asset, check if the crossing bar was also a large green bar
        for i in range(len(assets)):
            # If there was a crossing
            if last_crosses[i] >= 0:
                # Find the index of the crossing bar
                bar_index = int(closes.shape[0] - 1 - last_crosses[i])
                # Check if the crossing bar was a large green bar
                if large_green_bars[bar_index, i]:
                    out[i] = last_crosses[i]

class LastCrossWithLargeGreenBarAndThenConsolidation(LastCrossWithLargeGreenBar):
    """After a large green bar, the stock consolidate for few days
    
    consolidation means the stock never closed below breakout bar's after the breakout
    out == -1 mean it did not happen
    out == n means it conslidated for n days
    """
    window_length = 50
    # close_percent = 0.04 the cross bar must be 4% higher than last close
    # green_bar_limit = 0.2, high - close and open - low >= 20% of the high - low
    # breakout_day = 5 means the stock broke out 5 bars ago, and the stock has been
    # consolidated for 4 days
    params = ('close_percent', 'green_bar_limit', 'breakout_day')
    def compute(self, today, assets, out, opens, highs, lows, closes, close_percent, green_bar_limit, breakout_day):
        super().compute(
            today=today,
            assets=assets,
            out=out,
            opens=opens,
            highs=highs,
            lows=lows,
            closes=closes,
            close_percent=close_percent,
            green_bar_limit=green_bar_limit)
        breakout_on_green_bars = out.copy()
        bar_index = closes.shape[0] - breakout_day
        for i in range(len(assets)):
            # If there was a crossing
            if breakout_on_green_bars[bar_index] >= 0:
                # Find the index of the crossing bar
                bar_index = closes.shape[0] - 1 - last_crosses[i]
                
                # Check if the crossing bar was a large green bar
                if large_green_bars[bar_index, i]:
                    out[i] = last_crosses[i]

class TickerFilter(CustomFilter):
    """Factor that set to true only if the asset is one of the input tickers"""
    inputs = []
    window_length = 1
    params = ('tickers',)

    def compute(self, today, assets, out, tickers):
        sids = [symbol(t).sid for t in tickers]
        out[:] = np.isin(assets, sids)


class SMACrossOver(CustomFactor):
    """SMA cross over
    -1 for cross from above to below
    1  for cross from below to above
    0 for no cross event
    """
    inputs = [USEquityPricing.close]
    params = ('sma_len', )

    def compute(self, today, assets, out, close, sma_len):
        
        # Get SMA for current day and previous day
        sma_today = np.mean(close[-sma_len:], axis=0)
        sma_yesterday = np.mean(close[-(sma_len+1):-1], axis=0)
        
        # Get price for current day and previous day
        price_today = close[-1]
        price_yesterday = close[-2]
        
        # Check crossover conditions
        cross_above = (price_yesterday <= sma_yesterday) & (price_today > sma_today)
        cross_below = (price_yesterday >= sma_yesterday) & (price_today < sma_today)
        
        # Set output
        out[:] = 0  # Default no crossover
        out[cross_above] = 1  # Crossed above
        out[cross_below] = -1  # Crossed below

class SMATrend(CustomFactor):
    """
    1 if SMA is trending up (today > yesterday)
    -1 if SMA is trending down
    0 if no change
    """
    inputs = [USEquityPricing.close]
    params = ('sma_len', )

    def compute(self, today, assets, out, close, sma_len):
       
       # Get SMA for current day and previous day 
       sma_today = np.mean(close[-sma_len:], axis=0)
       sma_yesterday = np.mean(close[-(sma_len+1):-1], axis=0)
       
       # Check trend conditions
       trending_up = sma_today > sma_yesterday
       trending_down = sma_today < sma_yesterday
       
       # Set output
       out[:] = 0  # Default no change
       out[trending_up] = 1  # Trending up
       out[trending_down] = -1  # Trending down

class MACrossoverCountFactor(CustomFactor):
    """
    Counts the number of times two moving averages cross over 
    (either up or down) within the specified window length.
    
    Parameters:
    - window_length: Number of days to look back, this is used to fetch data
    - short_ma_length: Length of the shorter moving average
    - long_ma_length: Length of the longer moving average
    - master_ma_length: if set, 
    - check_period: number of days to look back for cross overs
    """
    inputs = [USEquityPricing.close]
    params = ('short_ma_len', 'long_ma_len', 'master_ma_len', 'check_period')    
    
    def compute(self, today, assets, out, closes, short_ma_len, long_ma_len, master_ma_len, check_period):
        # Efficient moving average calculation using cumulative sum
        def moving_average(x, w):
            cumsum = np.cumsum(x, axis=0)
            return (cumsum[w:] - cumsum[:-w]) / w
        
        # Compute short and long moving averages
        ma_short = np.zeros_like(closes)
        ma_long = np.zeros_like(closes)
        ma_master = np.zeros_like(closes)
        
        # Pad the arrays to ensure we can calculate full moving averages
        ma_short[short_ma_len:] = moving_average(closes, short_ma_len)
        ma_long[long_ma_len:] = moving_average(closes, long_ma_len)
        ma_master[master_ma_len:] = moving_average(closes, master_ma_len)
        # Initialize crossover count
        crossover_count = np.zeros(assets.shape[0])
        all_above_master = np.ones(assets.shape[0], dtype=bool)
        
        # Detect crossovers
        for i in range(check_period, closes.shape[0]):
            cross_below = (ma_short[i-1] >= ma_long[i-1]) & (ma_short[i] < ma_long[i])
            cross_above = (ma_short[i-1] <= ma_long[i-1]) & (ma_short[i] > ma_long[i])
            crossovers = cross_below | cross_above
            crossover_count += crossovers.astype(int)
            
            all_above_master &= ((ma_short[i] > ma_master[i]) & (ma_long[i] > ma_master[i]))
        # Set output: crossover count if above master, otherwise -1
        out[:] = np.where(all_above_master, crossover_count, -1)

class CloseAboveConsecutive(CustomFactor):
    """
    1 if close above the `threshold` for `window_length` days or more consecutively
    """
    inputs = [USEquityPricing.close]
    window_length = 10
    params = ('threshold',)

    def compute(self, today, assets, out, close, threshold):
        # Check if close price is above threshold for each asset on each day
        print('today', today)
        print(close)
        above_threshold = close > threshold
        # Use np.all to check if all days in the window are above threshold
        # axis=0 means we check along the time dimension for each asset
        all_above_threshold = np.all(above_threshold, axis=0)
        
        # Convert boolean results to int (1 for True, 0 for False)
        out[:] = all_above_threshold.astype(int)