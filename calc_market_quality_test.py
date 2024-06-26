import csv
import io
import sqlite3
import unittest

import calc_market_quality as calc
import lib

DUMMY_ITEM = calc.ItemSummary(1, "", 1, 1, "mgroup", 1, 1, 1)

class TestWeightedMean(unittest.TestCase):
    def testSimple(self):
        self.assertEqual(calc.weighted_mean([(1, 1), (1, 2), (1, 3)]), 2)

    def testWeighted(self):
        self.assertEqual(calc.weighted_mean([(1, 1), (2, 2), (5, 3)]), 2.5)

#class TestGetStationStats(unittest.TestCase):
#    def testSimple(self):
#        items = {
#            47821: calc.ItemSummary(ID=47821, Name="A", GroupID=1, CategoryID=1, ValueTraded=1000, Buy=300000, Sell=400000),
#            47900: DUMMY_ITEM,  # only buy orders
#            48121: DUMMY_ITEM,  # only buy orders
#            48416: calc.ItemSummary(ID=48416, Name="B", GroupID=2, CategoryID=1, ValueTraded=2000, Buy=200000, Sell=300000),
#            56134: calc.ItemSummary(ID=56134, Name="C", GroupID=3, CategoryID=2, ValueTraded=1000, Buy=10000, Sell=10010),
#            60338: calc.ItemSummary(ID=60338, Name="D", GroupID=4, CategoryID=3, ValueTraded=1000, Buy=20000000, Sell=30000000),
#            73248: DUMMY_ITEM,  # only buy orders
#            73840: DUMMY_ITEM,  # only buy orders
#            79211: calc.ItemSummary(ID=79211, Name="E", GroupID=5, CategoryID=5, ValueTraded=1000, Buy=400000, Sell=500000),
#        }
#        s, e = next(calc.get_station_stats("testdata/orderset2.csv.gz", items, lib.OrdersetInfo(None, None)))
#        self.assertEqual(60000004, s)
#        self.assertEqual(5, len(e))
#        self.assertEqual((47821, 1000, 1.125), e[0])
#        self.assertEqual((48416, 2000, 1.5), e[1])
#        self.assertEqual((56134, 1000, 1), e[2])
#        self.assertEqual((60338, 1000, 1), e[3])
#        self.assertEqual((79211, 1000, 1.6), e[4])

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
