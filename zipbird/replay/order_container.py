from datetime import date
from zipbird.basic.order import Order
from zipbird.replay.replay_order import ReplayOrder


class OrderContainer:
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
            open_price=open_price,
            open_order=open_order)
    
    def add_close_order(self, order: Order, close_date: date, close_price:float):
        replay_order = self.orders[order.uuid]
        replay_order.add_close_order(close_date, close_price)

    def write_orders(self, filename:str):
        orders = sorted(self.orders.values(), key=lambda o: o.open_date)
        with open(filename, 'w') as f:
            for order in orders:
                f.write(f'{order.as_csv()}\n')

    @classmethod
    def load_from_csv(cls, filename:str) -> list[ReplayOrder]:
        result = []
        with open(filename) as f:
            for line in f:
                result.append(ReplayOrder.from_csv(line))
        return result