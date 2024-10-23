from datetime import date, datetime
from zipbird.basic.order import Order
from zipbird.basic.types import Equity, LongShort
from zipbird.utils import utils

class ReplayOrder:
    """Order to replay"""
    strategy_name: str
    symbol: str
    long_short: LongShort
    open_date: date
    open_price: float
    open_sizer_percent: float | None
    open_sizer_stop_diff: float | None
    close_date: date
    close_price: float

    # Derived fields for replay
    # Number of shares to open during replay
    replay_shares: int
    asset: Equity

    @classmethod
    def make_from_open_order(
        cls,
        strategy_name: str,
        open_date: date,
        open_price: float,
        open_order: Order,

    ):
        order = ReplayOrder()
        order.strategy_name = strategy_name
        order.long_short = open_order.long_short
        order.symbol = open_order.stock.symbol
        order.open_date = open_date
        order.open_price = open_price
        order.open_sizer_percent = open_order.get_percent_size()
        order.open_sizer_stop_diff = open_order.get_initial_stop_diff()
        order.close_date = None
        order.close_price = None
        order.replay_shares = 0
        return order
    
    def add_close_order(self, close_date:date, close_price: float):
        self.close_date = close_date
        self.close_price = close_price

    def as_csv(self):
        maybe_float = lambda a : a if a else ''
        return ','.join(str(a) for a in
                        [
                            self.strategy_name,
                            self.symbol,
                            1 if self.long_short == LongShort.Long else -1,
                            self.open_date,
                            self.open_price,
                            maybe_float(self.open_sizer_percent),
                            maybe_float(self.open_sizer_stop_diff),
                            self.close_date or '',
                            maybe_float(self.close_price),
                        ])
    
    @classmethod
    def from_csv(cls, line):
        parts = line.strip().split(',')
        order = ReplayOrder()
        order.strategy_name = parts[0]
        order.symbol = parts[1]
        order.long_short = LongShort.Long if parts[2] == '1' else LongShort.Short
        order.open_date = to_date(parts[3])
        order.open_price = to_float(parts[4])
        order.open_sizer_percent = to_float(parts[5])
        order.open_sizer_stop_diff = to_float(parts[6])
        order.close_date = to_date(parts[7])
        order.close_price = to_float(parts[8])
        return order
    
    def __eq__(self, value: object) -> bool:
        return (
            self.strategy_name == value.strategy_name and
            self.symbol == value.symbol and
            self.long_short == value.long_short and
            self.open_date == value.open_date and
            self.open_price == value.open_price and
            utils.compare_object(self.open_sizer_percent, value.open_sizer_percent) and
            utils.compare_object(self.open_sizer_stop_diff, value.open_sizer_stop_diff) and
            utils.compare_object(self.close_date, value.close_date) and
            utils.compare_object(self.close_price, value.close_price)
            )
    def __str__(self)-> str:
        return f'ReplayOrder({self.symbol},{self.long_short},{self.open_date},{self.open_price},{self.close_date},{self.close_price})'

def to_date(date_str: str) -> date:
    if not date_str:
        return None
    else:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    
def to_float(float_str: str) -> float:
    if not float_str:
        return None
    else:
        return float(float_str)