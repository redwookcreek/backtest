import unittest

from zipbird.basic.signal import Signal
from zipbird.basic.types import LongShort, OpenClose

class TestSignal(unittest.TestCase):
    def test_create_signal_long(self):
        signal = Signal.make_open_long('AAPL')
        self.assertEqual(signal.stock, 'AAPL')
        self.assertEqual(signal.open_close, OpenClose.Open)
        self.assertEqual(signal.long_short, LongShort.Long)

    def test_create_signal_long_limit_price(self):
        signal = Signal.make_open_long('AAPL', 12.)
        self.assertEqual(signal.limit_price, 12.)

    def test_create_signal_short(self):
        signal = Signal.make_open_short('AAPL')
        self.assertEqual(signal.stock, 'AAPL')
        self.assertEqual(signal.open_close, OpenClose.Open)
        self.assertEqual(signal.long_short, LongShort.Short)


if __name__ == '__main__':
    unittest.main()