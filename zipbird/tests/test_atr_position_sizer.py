import unittest
import pandas as pd

from zipbird.basic.order import ShareOrder
from zipbird.basic.signal import Signal
from zipbird.basic.types import CloseStockNotInPortfolioException, ClosingInPositionSizerException, LongShort, OpenClose, Portfolio, Position
from zipbird.position_manager.atr_position_sizer import ATRPositionSizer

class TestATRPositionSizer(unittest.TestCase):
    def test_get_orders(self):
        params = {
            'atr_period': 10,
            'stop_loss_atr_multiple': 3,
            'stop_loss_days': 4,
            'max_equity_per_position': 0.1,
            'fraction_risk': 0.02
        }
        sizer = ATRPositionSizer(params)
        portfolio = Portfolio(portfolio_value=10000, positions={
            'AMZN': Position('AMZN', 300),
            'MSFT': Position('MFST', 400),
        })
        pipeline_data = pd.DataFrame({
            'close': {
                'AAPL': 100,
                'GOOG': 10,
                'AMZN': 300,
                'MSFT': 400,
            },
            'atr_10': {
                'AAPL': 3,
                'GOOG': 4,
                'AMZN': 5,
                'MSFT': 6,
            }
        })
        signals = [
            Signal.make_open_long('AAPL'),
            Signal.make_open_long('GOOG', 50.0),
        ]
        orders = sizer.get_orders(portfolio, signals, pipeline_data)
        self.assertEqual(len(orders), 2)
        orders.sort(key=lambda x: x.stock)
        self.assert_open_order(orders[0], 'AAPL', 10)
        self.assert_open_order(orders[1], 'GOOG', 16, 50.0)

    def assert_open_order(self, order, stock, amount, limit_price=None):
        self.assertEqual(order.stock, stock)
        self.assertEqual(order.amount, amount)
        self.assertEqual(order.open_close, OpenClose.Open)
        self.assertEqual(order.long_short, LongShort.Long)
        self.assertEqual(order.limit_price, limit_price)
        
    def test_assert_raise_on_closing_order(self):
        params = {
            'atr_period': 10,
            'stop_loss_atr_multiple': 3,
            'stop_loss_days': 4,
            'max_equity_per_position': 0.1,
            'fraction_risk': 0.02
        }
        sizer = ATRPositionSizer(params)
        portfolio = Portfolio(portfolio_value=10000, positions={
            'AMZN': Position('AMZN', 300),
            'MSFT': Position('MFST', 400),
        })
        with self.assertRaises(ClosingInPositionSizerException):
            sizer.get_orders( portfolio,
                             [Signal.make_close_long('AAPL')],
                             pd.DataFrame())

