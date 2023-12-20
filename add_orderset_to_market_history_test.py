import csv
import io
import sqlite3
import unittest

import add_orderset_to_market_history as add_o
import trade_lib
import lib

DUMMY_ITEM = trade_lib.ItemSummary(1, "", 1, 1, 1, 1, 1)

class TestLoad(unittest.TestCase):
    def testMultipleOrdersSamePrice(self):
        i = next(add_o.load("testdata/orderset3.csv.gz", set([60003760])))
        self.assertEqual(i.TypeID, 182)
        self.assertEqual(i.StationID, 60003760)
        self.assertEqual(i.Sell, 10.1)
        self.assertEqual(i.SellVolume, 893181)

unittest.main()
