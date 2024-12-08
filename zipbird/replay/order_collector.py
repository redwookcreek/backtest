from datetime import date
import math
import pandas as pd
import uuid

from zipbird.basic.types import LongShort, OpenClose
from zipbird.basic.order import Order
from zipbird.replay.replay_order import ReplayOrder


class OrderCollector:
    def __init__(self, strategy_name):
        self.strategy_name = strategy_name
        self.orders:dict[ReplayOrder] = {}

    def add_open_order(self,
                       open_date: date,
                       open_price: float,
                       open_order: Order):
        self.orders[open_order.uuid] = ReplayOrder.make_from_open_order(
            strategy_name=self.strategy_name,
            open_date=open_date,
            open_price=round_price(open_price, OpenClose.Open, open_order.long_short),
            open_order=open_order)
    
    def add_close_order(self, order: Order, close_date: date, close_price:float):
        replay_order = self.orders[order.uuid]
        replay_order.add_close_order(
            close_date,
            round_price(close_price, OpenClose.Close, order.long_short))

    def write_orders(self, filename:str):
        orders = sorted(self.orders.values(), key=lambda o: o.open_date)
        with open(filename, 'w') as f:
            for order in orders:
                f.write(f'{order.as_csv()}\n')
    
    def add_round_trip(self, order: ReplayOrder):
        # orders are keyed on order's id, it is not needed
        # when we try to replay the orders, so create a random one
        order_id = uuid.uuid4()
        self.orders[order_id] = order

    def to_dataframe(self):
        result = []
        for order in self.orders.values():
            if order.close_price is not None:
                if order.long_short == LongShort.Long:
                    profit_pct = order.close_price / order.open_price - 1
                else:
                    profit_pct = order.open_price / order.close_price - 1
                result.append({
                    'symbol': order.symbol,
                    'year': order.close_date.year,
                    'open_date': order.open_date,
                    'open_price': order.open_price,
                    'close_date': order.close_date,
                    'close_price': order.close_price,
                    'long_short': order.long_short,

                    # dervied
                    'profit_pct': profit_pct,
                    'trade_days': (order.close_date - order.open_date).days,
                })

        return pd.DataFrame(result)
            

def round_price(price:float, close_or_open:OpenClose, long_or_short:LongShort):
    direction = None
    match (close_or_open, long_or_short):
        case (OpenClose.Open, LongShort.Long):
            direction = 'up'
        case (OpenClose.Close, LongShort.Short):
            direction = 'up'
        case (OpenClose.Open, LongShort.Long):
            direction = 'down'
        case (OpenClose.Close, LongShort.Short):
            direction = 'down'
    if direction == 'up':
        return math.ceil(price * 100) / 100.
    else:
        return math.floor(price * 100) / 100.

