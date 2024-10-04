from unittest.mock import Mock
import pandas as pd
import unittest

from zipbird.basic.order import ShareOrder
from zipbird.basic.signal import Signal
from zipbird.basic.types import CloseStockNotInPortfolioException, LongShort, OpenClose, Portfolio, Position
from zipbird.strategy.strategy_executor import StrategyExecutor, _create_closing_orders, _get_extra_cash_after_closing, _split_signals

class TestStrategyExecutor(unittest.TestCase):
    pipeline_data = pd.DataFrame({'close': {'AAPL': 100, 'GOOG': 200, 'MFST': 300}})

    def test_get_cash_after_closing(self):
        portfolio = Portfolio(portfolio_cash=10000, positions={
            'AAPL': Position('AAPL', 100),
            'GOOG': Position('GOOG', 200),
            'MFST': Position('MFST', -300),
        })
        
        to_close = [
            Signal.make_close_long('AAPL'),
            Signal.make_close_long('GOOG'),
            Signal.make_close_short('MFST'),
        ]
        expected_cash = _get_extra_cash_after_closing(portfolio.positions, to_close, self.pipeline_data)
        # 100 * 100 + 200 * 200  - 300 * 300 = -40000
        self.assertEqual(expected_cash, -40000)

    def test_get_cash_after_closing_with_no_positions(self):
        portfolio = Portfolio(portfolio_cash=10000, positions={})
        to_close = [
            Signal.make_close_long('AAPL'),
        ]
        with self.assertRaises(CloseStockNotInPortfolioException):
            _get_extra_cash_after_closing(portfolio.positions, to_close, self.pipeline_data)

    def test_get_cash_after_closing_with_no_close_signals(self):
        portfolio = Portfolio(portfolio_cash=10000, positions={
            'AAPL': Position('AAPL', 100),
            'GOOG': Position('GOOG', 200),
            'MFST': Position('MFST', -300),
        })
        to_close = []
        expected_cash = _get_extra_cash_after_closing(portfolio.positions, to_close, self.pipeline_data)
        self.assertEqual(expected_cash, 0)

    def test_split_signals(self):
        signals = [
            Signal.make_close_long('AAPL'),
            Signal.make_close_long('GOOG'),
            Signal.make_close_short('MFST'),
            Signal.make_open_long('AAPL1'),
            Signal.make_open_long('GOOG1'),
            Signal.make_open_short('MFST1'),
        ]
        to_open, to_close = _split_signals(signals)
        self.assertEqual(to_open, [
            Signal.make_open_long('AAPL1'),
            Signal.make_open_long('GOOG1'),
            Signal.make_open_short('MFST1'),
        ])
        self.assertEqual(to_close, [
            Signal.make_close_long('AAPL'),
            Signal.make_close_long('GOOG'),
            Signal.make_close_short('MFST'),
        ])

    def test_create_closing_orders(self):
        positions = {
            'AAPL': Position('AAPL', 100),
            'GOOG': Position('GOOG', 200),
            'MFST': Position('MFST', -300),
            'META': Position('GOOG', -200),
        }
        to_close = [
            Signal.make_close_long('AAPL'),
            Signal.make_close_long('GOOG'),
            Signal.make_close_short('META'),
        ]
        orders = _create_closing_orders(positions, to_close)
        self.assertEqual(orders, [
            ShareOrder('AAPL', OpenClose.Close, LongShort.Long, amount=100),
            ShareOrder('GOOG', OpenClose.Close, LongShort.Long, amount=200),
            ShareOrder('META', OpenClose.Close, LongShort.Short, amount=-200),
        ])

    def test_create_closing_orders_with_no_positions(self):
        positions = {}
        to_close = [
            Signal.make_close_long('AAPL'),
        ]
        with self.assertRaises(CloseStockNotInPortfolioException):
            _create_closing_orders(positions, to_close)

    def test_create_closing_orders_with_no_close_signals(self):
        positions = {
            'AAPL': Position('AAPL', 100),
            'GOOG': Position('GOOG', 200),
            'MFST': Position('MFST', -300),
            'META': Position('GOOG', -200),
        }
        to_close = []
        orders = _create_closing_orders(positions, to_close)
        self.assertEqual(orders, [])

    def test_run_strategy_executor(self):
        mock_strategy = Mock()
        mock_strategy.generate_signals.return_value = [
            Signal.make_close_long('AAPL'),
            Signal.make_close_long('GOOG'),
        ]
        mock_position_sizer = Mock()
        to_open_orders = [
            ShareOrder('MSFT', OpenClose.Open, LongShort.Long, amount=100),
            ShareOrder('META', OpenClose.Open, LongShort.Long, amount=200),
        ]
        mock_debug_logger = Mock()
        mock_position_sizer.get_orders.return_value = to_open_orders
        stratgy_executor = StrategyExecutor(mock_strategy, mock_position_sizer)
        stratgy_executor.init(mock_debug_logger)
        stratgy_executor.position_manager = Mock()
        pipeline_data = pd.DataFrame({'close': {'AAPL': 100, 'GOOG': 200}})
        portfolio = Portfolio(portfolio_cash=10000, positions={
            'AAPL': Position('AAPL', 100),
            'GOOG': Position('GOOG', 200),
        })
        stratgy_executor.run(portfolio, pipeline_data)
        mock_strategy.generate_signals.assert_called_once_with(
            positions=portfolio.positions,
            pipeline_data=pipeline_data,
            filtered_pipeline_data=pipeline_data)
        expected_orders = to_open_orders + [
            ShareOrder('AAPL', OpenClose.Close, LongShort.Long, amount=100),
            ShareOrder('GOOG', OpenClose.Close, LongShort.Long, amount=200),
        ]
        stratgy_executor.position_manager.do_maintenance.assert_called_once()
        stratgy_executor.position_manager.send_orders.assert_called_once_with(expected_orders)


if __name__ == '__main__':
    unittest.main()

