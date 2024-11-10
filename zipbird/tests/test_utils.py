import unittest
from datetime import date
from zipbird.utils import utils

class TestUtils(unittest.TestCase):
    def test_get_quarter(self):
        self.assertEqual(1, utils.get_quarter(date(2021, 1, 2)))
        self.assertEqual(1, utils.get_quarter(date(2021, 2, 2)))
        self.assertEqual(1, utils.get_quarter(date(2021, 3, 2)))
        self.assertEqual(2, utils.get_quarter(date(2021, 4, 2)))
        self.assertEqual(2, utils.get_quarter(date(2021, 5, 2)))
        self.assertEqual(2, utils.get_quarter(date(2021, 6, 2)))
        self.assertEqual(3, utils.get_quarter(date(2021, 7, 2)))
        self.assertEqual(3, utils.get_quarter(date(2021, 8, 2)))
        self.assertEqual(3, utils.get_quarter(date(2021, 9, 2)))
        self.assertEqual(4, utils.get_quarter(date(2021, 10, 2)))
        self.assertEqual(4, utils.get_quarter(date(2021, 11, 2)))
        self.assertEqual(4, utils.get_quarter(date(2021, 12, 2)))


if __name__ == '__main__':
    unittest.main()