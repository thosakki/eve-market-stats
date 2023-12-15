import calc_market_quality as calc
import csv
import io
import sqlite3
import unittest

class TestWeightedMean(unittest.TestCase):
    def testSimple(self):
        self.assertEqual(calc.weighted_mean([(1, 1), (1, 2), (1, 3)]), 2)

    def testWeighted(self):
        self.assertEqual(calc.weighted_mean([(1, 1), (2, 2), (5, 3)]), 2.5)

class TestGetMostTradedItems(unittest.TestCase):
    def testBasic(self):
        d = io.StringIO("""ID,Name,GroupID,CategoryID,Value Traded,Buy,Sell
2679,Scourge Rage Heavy Assault Missile,654,8,961403854,79.1,90.0
""")
        r = [x for x in calc.get_most_traded_items(d)]
        self.assertEqual(r, [calc.ItemSummary(2679, "Scourge Rage Heavy Assault Missile", 654, 8, 961403854, 79.1, 90.0)])

    def testParseFailure(self):
        d = io.StringIO("""ID   Name    GroupID CategoryID
2679    Scourge Rage Heavy Assault Missile  654 8
""")
        with self.assertRaises(RuntimeError) as ctx:
            [x for x in calc.get_most_traded_items(d)]

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
        dummy_item = calc.ItemSummary(1, "", 1, 1, 1, 1, 1)
        calc.emit_station_stats(w, 1, [(11, 5, 1.1), (12, 1, 1.3), (13, 2, 1.2)], self.conn.cursor(), {11: dummy_item, 12: dummy_item, 13: dummy_item, 14: dummy_item}, None)
        f.seek(0)
        self.assertEqual(f.read(), """1,Amo - Minmatar Fleet Market,75.0,15.0\r
""")

unittest.main()
