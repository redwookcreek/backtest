import unittest

import pandas as pd

from zipbird.basic.types import LongShort
from zipbird.basic.stop import FixStop, MismatchLongShortError, PercentProfitTarget, PercentTrailingStop, ATRTrailingStop, Stop, StopOrder
from zipbird.basic.types import StopOrderStatus


class TestStopOrder(unittest.TestCase):

    def test_percent_profit_target_long(self):
        profit_target = PercentProfitTarget(LongShort.Long, 0.1)
        profit_target.update_target_with_open_price(90)
        self.assertAlmostEqual(profit_target.get_target(), 99.)
        self.assertFalse(profit_target.reached_target({'close': 98}))
        self.assertTrue(profit_target.reached_target({'close': 99.1}))
        self.assertTrue(profit_target.reached_target({'close': 100}))

    def test_percent_profit_target_short(self):
        profit_target = PercentProfitTarget(LongShort.Short, 0.1)
        profit_target.update_target_with_open_price(90)
        self.assertAlmostEqual(profit_target.get_target(), 81.)
        self.assertTrue(profit_target.reached_target({'close': 80}))
        self.assertFalse(profit_target.reached_target({'close': 91.1}))
        self.assertFalse(profit_target.reached_target({'close': 82}))

    def test_fix_stop_long(self):
        stop = FixStop(LongShort.Long, 2.0)
        stop.update_with_open_price(12)
        self.assertAlmostEqual(stop.get_stop_price(), 10)
        self.assertFalse(stop.is_triggered({'close': 11}))
        self.assertTrue(stop.is_triggered({'close': 9.5}))

    def test_fix_stop_short(self):
        stop = FixStop(LongShort.Short, 2.0)
        stop.update_with_open_price(12)
        self.assertAlmostEqual(stop.get_stop_price(), 14)
        self.assertFalse(stop.is_triggered({'close': 11}))
        self.assertTrue(stop.is_triggered({'close': 14.1}))

    def test_percent_trailing_stop(self):
        stop = PercentTrailingStop(LongShort.Long, 0.1)
        data = pd.DataFrame({'close': {'AMZN': 100}})
        stop.update_stop_price(data.loc['AMZN'])
        self.assertAlmostEqual(stop.get_stop_price(), 90)
        self.assertFalse(stop.is_triggered({'close': 100}))
        self.assertTrue(stop.is_triggered({'close': 89}))

        # price did not move higher, stop should remain the same
        stop.update_stop_price({'close': 90})
        self.assertAlmostEqual(stop.get_stop_price(), 90)

        # price moved higher, stop should move up too
        stop.update_stop_price({'close': 110})
        self.assertAlmostEqual(stop.get_stop_price(), 99)

    def test_percent_trailing_short(self):
        stop = PercentTrailingStop(LongShort.Short, 0.1)
        stop.update_stop_price({'close': 100})
        self.assertAlmostEqual(stop.get_stop_price(), 110)
        self.assertFalse(stop.is_triggered({'close': 100}))
        self.assertTrue(stop.is_triggered({'close': 111}))

        # price did not move lower, stop should remain the same
        stop.update_stop_price({'close': 110})
        self.assertAlmostEqual(stop.get_stop_price(), 110)

        # price moved lower, stop should move down too
        stop.update_stop_price({'close': 90})
        self.assertAlmostEqual(stop.get_stop_price(), 99)

    def test_atr_trailing_stop_long(self):
        stop = ATRTrailingStop(LongShort.Long, 2)
        stop.update_stop_price({'close': 100, 'atr': 3})
        self.assertAlmostEqual(stop.get_stop_price(), 94)

        self.assertFalse(stop.is_triggered({'close': 95}))
        self.assertTrue(stop.is_triggered({'close': 93}))

        stop.update_stop_price({'close': 102, 'atr': 2})
        self.assertAlmostEqual(stop.get_stop_price(), 98)

        # vol increased, resulting lower stop than before
        # stop should not move
        stop.update_stop_price({'close': 104, 'atr': 6})
        self.assertAlmostEqual(stop.get_stop_price(), 98)

    def test_atr_trailing_stop_short(self):
        stop = ATRTrailingStop(LongShort.Short, 2)
        stop.update_stop_price({'close': 100, 'atr': 3})
        self.assertAlmostEqual(stop.get_stop_price(), 106)

        self.assertFalse(stop.is_triggered({'close': 100}))
        self.assertTrue(stop.is_triggered({'close': 107}))

        stop.update_stop_price({'close': 100, 'atr': 2})
        self.assertAlmostEqual(stop.get_stop_price(), 104)

        # vol increased, resulting higher stop than before
        # stop should not move
        stop.update_stop_price({'close': 100, 'atr': 6})
        self.assertAlmostEqual(stop.get_stop_price(), 104)

    def test_stop_order_time_stopped(self):
        stop = StopOrder(time_stop=2)
        data = {'close': 100}
        self.assertEqual(stop.get_status(data), StopOrderStatus.NOT_TRIGGER)
        stop.incr_bar_cnt()
        self.assertEqual(stop.get_status(data), StopOrderStatus.NOT_TRIGGER)
        stop.incr_bar_cnt()
        self.assertEqual(stop.get_status(data), StopOrderStatus.TIME_STOP)

    def test_stop_order_initial_stopped(self):
        fix_stop = FixStop(LongShort.Long, 2)
        stop = StopOrder(initial_stop=fix_stop)
        stop.incr_bar_cnt()
        stop.update_with_open_price(100)
        self.assertEqual(stop.get_status({'close': 99}), StopOrderStatus.NOT_TRIGGER)
        self.assertEqual(stop.get_status({'close': 97}), StopOrderStatus.INITIAL_STOP)

    def test_stop_order_target_reached(self):
        profit_target = PercentProfitTarget(LongShort.Short, 0.1)
        stop = StopOrder(profit_target=profit_target)
        stop.update_with_open_price(100)
        self.assertEqual(stop.get_status({'close': 95}), StopOrderStatus.NOT_TRIGGER)
        self.assertEqual(stop.get_status({'close': 89}), StopOrderStatus.TARGET_REACHED)

    def test_stop_order_trailing_stop(self):
        trailing = ATRTrailingStop(LongShort.Long, 2)
        stop = StopOrder(trailing=trailing)
        stop.update_stops({'close': 100, 'atr': 3})
        self.assertEqual(stop.get_status({'close': 100}), StopOrderStatus.NOT_TRIGGER)
        self.assertEqual(stop.get_status({'close': 93}), StopOrderStatus.TRAILING_STOP)

    def test_stop_order_multiple_stops(self):
        trailing = ATRTrailingStop(LongShort.Long, 2)
        initial = FixStop(LongShort.Long, 6)
        trailing.update_stop_price({'close': 100, 'atr': 2})
        initial.update_with_open_price(100)
        stop_price = Stop.get_stop_price_for_multiple([trailing, initial])
        self.assertEqual(stop_price, 96)

    def test_stop_order_multiple_stops1(self):
        trailing = ATRTrailingStop(LongShort.Long, 4)
        initial = FixStop(LongShort.Long, 2)
        trailing.update_stop_price({'close': 100, 'atr': 1})
        initial.update_with_open_price(100)
        stop_price = Stop.get_stop_price_for_multiple([trailing, initial, None])
        self.assertEqual(stop_price, 98)

    def test_stop_order_multiple_stops_short(self):
        trailing = ATRTrailingStop(LongShort.Short, 4)
        initial = FixStop(LongShort.Short, 2)
        trailing.update_stop_price({'close': 100, 'atr': 1})
        initial.update_with_open_price(100)
        stop_price = Stop.get_stop_price_for_multiple([trailing, initial, None])
        self.assertEqual(stop_price, 102)

    def test_stop_order_multiple_stops_mismatch_long_short(self):
        trailing = ATRTrailingStop(LongShort.Long, 4)
        initial = FixStop(LongShort.Short, 2)
        trailing.update_stop_price({'close': 100, 'atr': 1})
        initial.update_with_open_price(100)
        with self.assertRaises(MismatchLongShortError):
            Stop.get_stop_price_for_multiple([trailing, initial])

if __name__ == '__main__':
    unittest.main()