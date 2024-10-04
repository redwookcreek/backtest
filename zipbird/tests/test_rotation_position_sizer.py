import datetime
import unittest
import pandas as pd

from zipbird.basic.order import PercentOrder
from zipbird.basic.signal import Signal
from zipbird.basic.types import ClosingInPositionSizerException, LongShort, OpenClose, Portfolio, Position
from zipbird.position_manager.rotation_position_sizer import RotationPositionSizer

class TestRotationPositionSizer(unittest.TestCase):

    def test_no_change_on_wrong_weekday(self):
        params = {
            'balance_weekday': 1 # Tuesday
        }
        sizer = RotationPositionSizer(params)
        portfolio = Portfolio(today=datetime.date(2024, 9, 15)) # Sunday
        # No balance on Sunday
        self.assertEqual(sizer.get_orders(portfolio, [], None), [])

    def test_close_and_open_with_signals(self):
        params = {
            'balance_weekday': 1,  # Tuesday
            'vol_window': 10,
            'rebalance_by_vol': False,
        }
        sizer = RotationPositionSizer(params)

        positions = {
            'MSFT': Position('MSFT', 100),
            'AMZN': Position('AMZN', 200),
            'META': Position('META', 300),
        }

        portfolio = Portfolio(
            today=datetime.date(2024, 9, 17),
            portfolio_cash=20000,
            portfolio_value=40000,
            positions=positions) # Tuesday

        signals = [
            Signal.make_open_long('AAPL'),
            Signal.make_open_long('GOOG'),
        ]

        pipeline_df = pd.DataFrame({
            'close': {
                'AAPL': 10,
                'GOOG': 20,
                'MSFT': 30,
                'AMZN': 40,
                'META': 50,
            },
            'vol': {
                'AAPL': 0.1,
                'GOOG': 0.2,
                'MSFT': 0.3,
                'AMZN': 0.4,
                'META': 0.5,
            }
        })
        # Create a dictionary where each key is a column, and the value is a Series
        data = {col: pipeline_df[col] for col in pipeline_df.columns}

        result = sizer.get_orders(portfolio, signals, data)
        self.assertEqual(len(result), 2)  # two close, two open
        result.sort(key=lambda x: x.stock)

        self.assertPercentOrder(result[0], PercentOrder.make_open_long('AAPL', 0.25, False))        
        self.assertPercentOrder(result[1], PercentOrder.make_open_long('GOOG', 0.25, False))
        

    def test_close_and_open_with_signals_by_vol(self):
        params = {
            'balance_weekday': 1,  # Tuesday
            'vol_window': 10,
            'rebalance_by_vol': True,
        }
        sizer = RotationPositionSizer(params)

        positions = {
            'META': Position('META', 300),
            'AAPL': Position('AAPL', 300),
        }

        portfolio = Portfolio(
            today=datetime.date(2024, 9, 17),
            portfolio_cash=1000,
            portfolio_value=30000,
            positions=positions) # Tuesday

        signals = [
            Signal.make_open_long('AAPL'),
            Signal.make_open_long('GOOG'),
            Signal('META', OpenClose.Adjust, LongShort.Long),
        ]

        pipeline_data = pd.DataFrame({
            'close': {
                'AAPL': 10,
                'GOOG': 20,
                'MSFT': 30,
                'AMZN': 40,
                'META': 50,
            },
            'vol_10': {
                'AAPL': 0.1,
                'GOOG': 0.2,
                'MSFT': 0.3,
                'AMZN': 0.4,
                'META': 0.2,
            }
        })

        result = sizer.get_orders(portfolio, signals, pipeline_data)        
        result.sort(key=lambda x: x.stock)
        self.assertEqual(len(result), 3)

        self.assertPercentOrder(result[0], PercentOrder.make_open_long('AAPL', 0.5, False))
        self.assertPercentOrder(result[1], PercentOrder.make_open_long('GOOG', 0.25, False))
        self.assertPercentOrder(result[2], PercentOrder.make_open_long('META', 0.25, True))

    def assertPercentOrder(self, order, expected):
        self.assertEqual(order.stock, expected.stock)
        self.assertEqual(order.open_close, expected.open_close)
        self.assertEqual(order.long_short, expected.long_short)
        self.assertAlmostEqual(order.target_percent, expected.target_percent)

    def assertAmountOrder(self, order, expected):
        self.assertEqual(order.stock, expected.stock)
        self.assertEqual(order.open_close, expected.open_close)
        self.assertEqual(order.long_short, expected.long_short)
        self.assertEqual(order.amount, expected.amount)
    
    def test_throw_on_closing_position(self):
        params = {
            'balance_weekday': 1,  # Tuesday

            'vol_window': 10,
            'rebalance_by_vol': False,
        }
        sizer = RotationPositionSizer(params)
        portfolio = Portfolio(
            today=datetime.date(2024, 9, 17),
            portfolio_cash=1000,
            portfolio_value=30000,
            positions={}) # Tuesday

        signals = [
            Signal.make_open_long('AAPL'),
            Signal.make_open_long('GOOG'),
            Signal.make_close_long('MSFT'),
        ]
        with self.assertRaises(ClosingInPositionSizerException):
            sizer.get_orders(portfolio, signals, None)
