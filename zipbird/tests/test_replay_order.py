from datetime import date
import unittest

from zipbird.basic.order import ShareOrder
from zipbird.basic.stop import FixStop, StopOrder
from zipbird.basic.types import LongShort, Equity, OpenClose
from zipbird.replay.replay_order import ReplayOrder
from zipbird.replay.order_collector import round_price

DAY1 = date(2023, 1, 1)
DAY2 = date(2024, 12, 31)

class TestReplayOrder(unittest.TestCase):
    
    def test_make_order(self):
        order = ShareOrder.make_open_long(Equity('AAPL'), 100)
        order.add_stop(StopOrder(
            initial_stop=FixStop(long_or_short=LongShort.Long, diff_price=2.3)))
        replay_order = ReplayOrder.make_from_open_order(
            strategy_name='s1',
            open_date=DAY1,
            open_price=12.4,
            open_order=order
        )
        self.assertEqual(replay_order.strategy_name, 's1')
        self.assertEqual(replay_order.long_short, LongShort.Long)
        self.assertEqual(replay_order.symbol, 'AAPL')
        self.assertEqual(replay_order.open_date, DAY1)
        self.assertEqual(replay_order.open_price, 12.4)
        self.assertIsNone(replay_order.open_sizer_percent)
        self.assertEqual(replay_order.open_sizer_stop_diff, 2.3)
        self.assertIsNone(replay_order.close_date)
        self.assertIsNone(replay_order.close_price)
        self.assertEqual(replay_order.replay_shares, 0)

        replay_order.add_close_order(DAY2, 14.3)
        self.assertEqual(replay_order.close_date, DAY2)
        self.assertEqual(replay_order.close_price, 14.3)

    def test_convert_to_from_csv(self):
        order = ShareOrder.make_open_long(Equity('AAPL'), 100)
        order.add_stop(StopOrder(
            initial_stop=FixStop(long_or_short=LongShort.Long, diff_price=2.3)))
        replay_order = ReplayOrder.make_from_open_order(
            strategy_name='s1',
            open_date=DAY1,
            open_price=12.4,
            open_order=order
        )
        csv_line = replay_order.as_csv()
        self.assertTrue(len(csv_line) > 0)
        self.assertEqual(replay_order.as_csv(), csv_line)
        self.assertEqual(replay_order, ReplayOrder.from_csv(csv_line))

    def test_round_price(self):
        # input price, open_close, long_short, expected
        test_data = [
            (2.3435, OpenClose.Open, LongShort.Long, 2.35),
            (2.3432, OpenClose.Open, LongShort.Long, 2.35),
            (2.33, OpenClose.Open, LongShort.Long, 2.33),
            (12.334, OpenClose.Open, LongShort.Short, 12.33),
            (12.33, OpenClose.Open, LongShort.Short, 12.33),
            (12.335, OpenClose.Close, LongShort.Long, 12.33),
        ]
        for price, open_close, long_short, expected in test_data:
            self.assertEqual(
                round_price(price, open_close, long_short), expected)