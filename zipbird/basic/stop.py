from typing import Optional

from zipbird.basic.types import LongShort, StopOrderStatus


# ========================
# Expected data frame column names
# ========================
COL_CLOSE = 'close'
COL_ATR = 'atr'

class MismatchLongShortError(Exception):
    """Mismatch long short error"""

class ProfitTarget:

    def __init__(self, long_or_short):
        self.target_price = None
        self.long_or_short = long_or_short

    def get_target(self) -> float:
        """Returns the profit target of the order"""
        if not self.target_price:
            raise ValueError('Target price is not yet set')
        return self.target_price

    def update_target_with_open_price(self, open_price: float) -> None:
        """Update target based on open price"""

    def reached_target(self, data:float) -> bool:
        """Returns True if last close has reached target"""
        last_close = data[COL_CLOSE]
        if (self.long_or_short == LongShort.Long and
            last_close >= self.get_target()):
            return True
        if (self.long_or_short == LongShort.Short and
            last_close <= self.get_target()):
            return True
        return False
    
    def __str__(self):
        return f'ProfitTarget(long_or_short={self.long_or_short}, target_price={self.target_price})'

class PercentProfitTarget(ProfitTarget):
    """Profit target that is certain percentage away from opening price"""

    def __init__(self, long_or_short: LongShort, target_percent: float):
        super().__init__(long_or_short)
        self.target_percent = target_percent

    def update_target_with_open_price(self, open_price: float):
        sign = 1 if self.long_or_short == LongShort.Long else -1
        self.target_price = open_price * (1 + sign * self.target_percent)

    def __str__(self):
        return f'PercentProfitTarget(long_or_short={self.long_or_short}, target_percent={self.target_percent})'

class Stop:
    """A stop price"""

    def __init__(self, long_or_short):
        self.stop_price = None
        self.long_or_short = long_or_short

    def get_stop_price(self) -> float:
        """Return the current stop price"""
        if not self.stop_price:
            raise ValueError('Stop price is not yet set')
        return self.stop_price

    def is_triggered(self, data) -> bool:
        """Returns true if the stop has been triggered"""
        if self.long_or_short == LongShort.Long:
            return data[COL_CLOSE] <= self.get_stop_price() 
        else:
            return data[COL_CLOSE] >= self.get_stop_price() 

    @classmethod
    def get_stop_price_for_multiple(cls, stops) -> float:
        stops = [s for s in stops if s is not None]
        long_or_short = set(stop.long_or_short for stop in stops)
        if len(long_or_short) != 1:
            raise MismatchLongShortError('All stops must be of the same type')
        long_or_short = long_or_short.pop()
        if long_or_short == LongShort.Long:
            return max(stop.get_stop_price() for stop in stops)
        else:
            return min(stop.get_stop_price() for stop in stops)

# stop price should not exceed 50%
MAX_STOP_PERCENT = 0.5

class FixStop(Stop):
    """Fix point stop"""

    def __init__(self, long_or_short: LongShort, diff_price: float):
        super().__init__(long_or_short)
        self.diff_price = diff_price

    def update_with_open_price(self, open_price: float):
        if self.long_or_short == LongShort.Long:
            self.stop_price = max(open_price - self.diff_price, 
                                  open_price * (1 - MAX_STOP_PERCENT))
        else:
            self.stop_price = open_price + self.diff_price

    def __str__(self):
        return f'FixStop(long_or_short={self.long_or_short}, diff_price={self.diff_price}, stop_price={self.stop_price})'


class TrailingStop(Stop):
    """Used as price point for trailing stop"""

    def update_stop_price(self, data) -> None:
        """Update the stop price based on latest price info"""


class PercentTrailingStop(TrailingStop):

    def __init__(self, long_or_short: LongShort, trailing_percent: float):
        super().__init__(long_or_short)
        self.trailing_percent = trailing_percent

    def update_stop_price(self, data) -> None:
        last_close = data[COL_CLOSE]
        if self.long_or_short == LongShort.Long:
            cur_stop = self.stop_price if self.stop_price else 0
            self.stop_price = max(cur_stop,
                                  last_close * (1 - self.trailing_percent))
        else:
            cur_stop = self.stop_price if self.stop_price else float('inf')
            self.stop_price = min(cur_stop,
                                  last_close * (1 + self.trailing_percent))

    def __str__(self):
        return f'PercentTrailingStop(long_or_short={self.long_or_short}, trailing_percent={self.trailing_percent})'


class ATRTrailingStop(TrailingStop):

    def __init__(self, long_or_short: LongShort, trailing_atr_multiple: float):
        super().__init__(long_or_short)
        self.trailing_atr_multiple = trailing_atr_multiple

    def update_stop_price(self, data) -> None:
        last_close = data[COL_CLOSE]
        atr = data[COL_ATR]
        diff_price = self.trailing_atr_multiple * atr
        if self.long_or_short == LongShort.Long:
            self.stop_price = max(self.stop_price or 0,
                                  last_close - diff_price,
                                  last_close * (1 - MAX_STOP_PERCENT))
        else:
            self.stop_price = min(self.stop_price or float('inf'), last_close + diff_price)

    def __str__(self):
        return f'ATRTrailingStop(long_or_short={self.long_or_short}, trailing_atr_multiple={self.trailing_atr_multiple})'


class StopOrder:
    """Stop order

    Stop order is an order with both open order and the attached
    stop order if the open order is executed.

    Stop order has the following types:
    1. Fixed stop order, where the stop price is fixed at certain value
    2. Trailing stop order, where stop price is certain amount below or
       above the highest/lowest value since entry.
    3. Time stop order, where the position is exited after certain bars
       since entry
    4. Target stop order, where the position is exited after certain profit
       target is hit.

    It is possible for one stop order to hanle all 4 cases. E.g. we may need
    a stop order that:
    1. stop if fall 3 ATR since entry
    2. trailing at 2 ATR after entry
    3. exist N days after entry if did not exit by other checkes
    4. exist after reaching x% of profit

    Though 2 is usually for trend following strategies, 3 and 4 are for mean reversion
    strategies.
    """

    def __init__(
        self,
        initial_stop: Optional[FixStop] = None,
        time_stop: int = 0,  # number of bars
        profit_target: Optional[ProfitTarget] = None,
        trailing: Optional[TrailingStop] = None,
    ):
        self.initial_stop = initial_stop
        self.time_stop = time_stop
        self.profit_target = profit_target
        self.trailing_stop = trailing
        self.bar_count = 0

    def do_maintenance(self, open_price:float, data):
        """Do maintenance before each trading session"""
        self.update_with_open_price(open_price)
        self.update_stops(data)
        self.incr_bar_cnt()

    def get_day_count(self):
        return self.bar_count

    def get_target_price(self):
        if self.profit_target:
            return self.profit_target.get_target()
        return -1

    def get_stop_price(self):        
        return Stop.get_stop_price_for_multiple([self.trailing_stop, self.initial_stop])

    def update_with_open_price(self, open_price):
        if self.profit_target:
            self.profit_target.update_target_with_open_price(open_price)
        if self.initial_stop:
            self.initial_stop.update_with_open_price(open_price)

    def update_stops(self, data):
        if self.trailing_stop:
            self.trailing_stop.update_stop_price(data)

    def incr_bar_cnt(self):
        self.bar_count += 1

    def get_status(self, data) -> StopOrderStatus:
        if self.time_stop and self.bar_count >= self.time_stop:
            return StopOrderStatus.TIME_STOP

        if self.profit_target and self.profit_target.reached_target(data):
            return StopOrderStatus.TARGET_REACHED
        
        if self.initial_stop and self.initial_stop.is_triggered(data):
            return StopOrderStatus.INITIAL_STOP
        
        if self.trailing_stop and self.trailing_stop.is_triggered(data):
            return StopOrderStatus.TRAILING_STOP

        return StopOrderStatus.NOT_TRIGGER

    def __str__(self) -> str:
        return f'StopOrder(inital_stop={self.initial_stop},trailing_stop={self.trailing_stop},time_stop={self.time_stop},profit_target={self.profit_target})'