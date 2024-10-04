"""Order to send to the broker"""

from zipbird.basic.stop import StopOrder
from zipbird.basic.types import OpenClose, LongShort

class NoOppositeOrderError(Exception):
    pass

class Order:
    stock: str
    open_close: OpenClose
    long_short: LongShort
    # Intended open price
    # There are signals that only open a position when
    # the price reaches the limit price on next session.
    limit_price: float | None
    stop: StopOrder | None
    
    def __init__(self, stock, open_close:OpenClose, long_short:LongShort, limit_price:float=None):
        self.stock = stock
        self.open_close = open_close
        self.long_short = long_short
        self.limit_price = limit_price
        self.stop = None
        # self.stop already has a bar_count, but not all orders have stop
        self.bar_count = 0

    def get_sign(self):
        """The amount or percentage are all positive, 
        The actual order amount depends on the if the
        order is open or close, long or short
        """
        s1 = 1 if self.open_close == OpenClose.Open else -1
        s2 = 1 if self.long_short == LongShort.Long else -1
        return s1 * s2

    def make_opposite_order(self, keep_stop:bool, keep_limit:bool):
        if self.open_close == OpenClose.Open:
            open_close = OpenClose.Close
        elif self.open_close == OpenClose.Close:
            open_close = OpenClose.Open
        else:
            raise NoOppositeOrderError(f'No opposite order for {self.open_close}')
        
        order = Order(
            self.stock, 
            open_close,
            self.long_short,
            self.limit_price)
        order.bar_count = self.bar_count
        if keep_stop:
            order.stop = self.stop
        if not keep_limit:
            order.limit_price = None
        
        return order

    def __eq__(self, value: object) -> bool:
        return (self.stock == value.stock and 
                self.open_close == value.open_close and
                self.long_short == value.long_short and
                self.limit_price == value.limit_price and
                self.stop == value.stop)

    def _order_type_str(self):
        if self.open_close == OpenClose.Open:
            return 'O'
        elif self.open_close == OpenClose.Adjust:
            return 'A'
        elif self.open_close == OpenClose.Close:
            return 'C'

    def add_stop(self, stop:StopOrder):
        self.stop = stop

    def get_bar_count(self):
        return self.bar_count

    def inc_bar_count(self):
        self.bar_count += 1

    def __str__(self):
        return f'{self.stock},{self.open_close},{self.long_short},stop={self.stop}'
    
    def __repr__(self):
        return f'Order({self.stock}, {self.open_close}, {self.long_short}, {self.limit_price}, stop={self.stop})'


class ShareOrder(Order):
    amount: int

    def __init__(self, stock, open_close:OpenClose, long_short:LongShort, limit_price:float=None, amount:int=None):
        super().__init__(stock, open_close, long_short, limit_price)
        self.amount = amount
    
    def amount_debug_str(self):
        return f'{self._order_type_str()} {self.amount or "---"}'

    def make_opposite_order(self, keep_stop:bool, keep_limit:bool):
        order = super().make_opposite_order(keep_stop, keep_limit)
        order.amount = self.amount
        return order
    
    def __eq__(self, value: object) -> bool:
        return super().__eq__(value) and self.amount == value.amount

    @staticmethod
    def make_close_long(stock, amount:int=None, limit_price:float=None):
        return ShareOrder(stock, OpenClose.Close, LongShort.Long, limit_price, amount)
    
    @staticmethod
    def make_close_short(stock, amount:int=None, limit_price:float=None):
        return ShareOrder(stock, OpenClose.Close, LongShort.Short, limit_price, amount)

    @staticmethod
    def make_open_long(stock, amount:int=None, limit_price:float=None):
        return ShareOrder(stock, OpenClose.Open, LongShort.Long, limit_price, amount)
    
    @staticmethod
    def make_open_short(stock, amount:int=None, limit_price:float=None):
        return ShareOrder(stock, OpenClose.Open, LongShort.Short, limit_price, amount)
    
    def __str__(self):
        return f'ShareOrder: {super().__str__()}, {self.amount or "---"}'
    
    def __eq__(self, value: object) -> bool:
        return super().__eq__(value) and self.amount == value.amount
    
    def __repr__(self):
        return f'ShareOrder({self.stock}, {self.open_close}, {self.long_short}, {self.limit_price}, {self.amount})'


class PercentOrder(Order):
    target_percent: float

    def __init__(self,
                 stock,
                 open_close:OpenClose,
                 long_short:LongShort, 
                 target_percent:float=None,
                 limit_price:float=None):
        super().__init__(stock, open_close, long_short, limit_price)
        self.target_percent = target_percent

    def _target_percent_str(self):
        if self.target_percent is None:
            return '---'
        return f'{self.target_percent*100:3.0f}%'
    
    def amount_debug_str(self):
        return self._order_type_str() + self._target_percent_str()

    def make_opposite_order(self, keep_stop:bool, keep_limit:bool):
        return NotImplemented('Cannot make opposite order for PercentOrder')

    @staticmethod
    def make_open_long(stock, percent, is_adjust):
        return PercentOrder(
            stock,
            OpenClose.Adjust if is_adjust else OpenClose.Open,
            LongShort.Long,
            target_percent=percent, 
            limit_price=None)
    
    def __str__(self):
        return f'PercentOrder:{super().__str__()}, {self._target_percent_str()}'