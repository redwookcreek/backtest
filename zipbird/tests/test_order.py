import unittest

from zipbird.basic.types import LongShort, OpenClose
from zipbird.basic.order import ShareOrder, PercentOrder

class TestOrder(unittest.TestCase):
    def test_create_order_long(self):
        order = ShareOrder.make_open_long('AAPL', 100)
        self.assertEqual(order.stock, 'AAPL')
        self.assertEqual(order.open_close, OpenClose.Open)
        self.assertEqual(order.long_short, LongShort.Long)
        self.assertEqual(order.amount, 100)

    def test_create_order_long_limit_price(self):
        order = ShareOrder.make_open_long('AAPL', 100, 12.)
        self.assertEqual(order.limit_price, 12.)

    def test_create_order_short(self):
        order = ShareOrder.make_open_short('AAPL', 100)
        self.assertEqual(order.stock, 'AAPL')
        self.assertEqual(order.open_close, OpenClose.Open)
        self.assertEqual(order.long_short, LongShort.Short)
        self.assertEqual(order.amount, 100)

    def test_create_percent_order_long(self):
        order = PercentOrder('AAPL', OpenClose.Open, LongShort.Long, target_percent=0.1)
        self.assertEqual(order.stock, 'AAPL')
        self.assertEqual(order.open_close, OpenClose.Open)
        self.assertEqual(order.long_short, LongShort.Long)
        self.assertEqual(order.target_percent, 0.1)

    def test_create_percent_order_long_limit_price(self):
        order = PercentOrder('AAPL', OpenClose.Open, LongShort.Long, target_percent=0.1, limit_price=12.)
        self.assertEqual(order.limit_price, 12.)

    def test_create_percent_order_short(self):
        order = PercentOrder('AAPL', OpenClose.Open, LongShort.Short, target_percent=0.1)
        self.assertEqual(order.stock, 'AAPL')
        self.assertEqual(order.open_close, OpenClose.Open)
        self.assertEqual(order.long_short, LongShort.Short)
        self.assertEqual(order.target_percent, 0.1)