import csv
import io
import sqlite3
import unittest

import calc_market_quality as calc
import lib

DUMMY_ITEM = calc.ItemSummary(1, "", 1, 1, "mgroup", 1)

class TestWeightedMean(unittest.TestCase):
    def testSimple(self):
        self.assertEqual(calc.weighted_mean([(1, 1), (1, 2), (1, 3)]), 2)

    def testWeighted(self):
        self.assertEqual(calc.weighted_mean([(1, 1), (2, 2), (5, 3)]), 2.5)

class TestGetStationStats(unittest.TestCase):
    def testSimple(self):
        items = {
            12608: calc.ItemSummary(ID=12608, Name="A", GroupID=1, CategoryID=1, ValueTraded=1000, MarketGroup='Foo>Bar'),
            47900: DUMMY_ITEM,  # only buy orders
            48121: DUMMY_ITEM,  # only buy orders
        }
        prices = { 12608: 70 }
        s, e = next(calc.get_station_stats("testdata/orderset4.csv.gz", items, prices, lib.OrdersetInfo(None, None)))
        self.assertEqual(60003760, s)
        self.assertEqual(1, len(e))
        self.assertEqual((12608, 1000, 1.0427142857142857), e[0])

class TestEmitStationStats(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        c = self.conn.cursor()
        c.execute("""
        CREATE TABLE Stations(
          ID       INT PRIMARY KEY NOT NULL,
          Name     TEXT NOT NULL,
          SystemID INT NOT NULL,
          RegionID INT NOT NULL
        );""")
        c.execute("""INSERT INTO Stations VALUES(?,?,?,?);""", [1, "Amo - Minmatar Fleet Market", 123, 100])

    def testWriteRecord(self):
        f = io.StringIO()
        w = csv.writer(f)
        calc.emit_station_stats(w, 1, [(11, 5, 1.1), (12, 1, 1.3), (13, 2, 1.2)], self.conn.cursor(), {11: DUMMY_ITEM, 12: DUMMY_ITEM, 13: DUMMY_ITEM, 14: DUMMY_ITEM}, None)
        f.seek(0)
        self.assertEqual(f.read(), """1,Amo - Minmatar Fleet Market,75.0,15.0\r
""")

unittest.main()
