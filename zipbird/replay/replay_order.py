from datetime import date
import datetime
from zipbird.basic.order import Order
from zipbird.basic.types import Equity


class ReplayOrder:
    """Order to replay"""
    strategy_name: str
    asset: Equity
    open_date: date
    # price to open the position, if None, open at market open
    open_price: float
    open_sizer_percent: float | None
    open_sizer_stop_diff: float | None
    close_date: date
    # price to close the position, if None, close at market open
    close_price: float

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
        order.asset = open_order.stock
        order.open_date = open_date
        order.open_price = open_price
        order.open_sizer_percent = open_order.get_percent_size()
        order.open_sizer_stop_diff = open_order.get_initial_stop_diff()
        order.close_date = None
        order.close_price = None
        return order
    
    def add_close_order(self, close_date:date, close_price: float):
        self.close_date = close_date
        self.close_price = close_price

    def as_csv(self):
        maybe_float = lambda a : a if a else -1
        return ','.join(str(a) for a in
                        [
                            self.strategy_name,
                            self.asset.symbol,
                            self.open_date,
                            self.open_price,
                            maybe_float(self.open_sizer_percent),
                            maybe_float(self.open_sizer_stop_diff),
                            self.close_date or '',
                            maybe_float(self.close_price),
                        ])
    
    @classmethod
    def from_csv(cls, line):
        parts = line.split(',')
        order = ReplayOrder()
        order.strategy_name = parts[0]
        order.asset = parts[1]
        order.open_date = to_date(parts[2])
        order.open_price = to_float(parts[3])
        order.open_sizer_percent = to_float(parts[4])
        order.open_sizer_stop_diff = to_float(parts[5])
        order.close_date = to_date(parts[6])
        order.close_price = to_float(parts[7])

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