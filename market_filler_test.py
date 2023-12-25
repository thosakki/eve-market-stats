#!/usr/bin/python3

import csv
import datetime
import io
import sqlite3
from typing import List
import unittest

import market_filler as m
import trade_lib

class TestPickPrices(unittest.TestCase):
    TODAY = datetime.datetime(year=2021, month=1, day=1)
    JITA = 60003760

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        c = self.conn.cursor()
        c.execute("""
            CREATE TABLE PriceHistory(
            TypeID INTEGER,
            Date DATE,
            StationID INTEGER,
            Buy FLOAT,
            Sell FLOAT,
            SellVolume INTEGER
            );""")
        c.execute("""
            CREATE INDEX PriceHistory_ByItemDate ON PriceHistory(TypeID, Date);
            """)

    def AddPrices(self, typeID: int, curDate: datetime.date, stationID: int, buy: List[float], sell: List[float], sellVolume: List[int]):
        assert len(buy) == len(sell)
        assert len(sell) == len(sellVolume)
        date = curDate - datetime.timedelta(days=len(buy)-1)
        c = self.conn.cursor()
        for i in range(0, len(buy)):
            c.execute("INSERT INTO PriceHistory VALUES(?,?,?,?,?,?)", [typeID, date.isoformat(), stationID, buy[i], sell[i], sellVolume[i]])
            date += datetime.timedelta(days=1)
        self.conn.commit()

    @staticmethod
    def ts(i: int):
        return trade_lib.ItemSummary(i, "", 1, 1, 1, 1, 1)

    def testSimple(self):
        self.AddPrices(1, self.TODAY, self.JITA, [100]*3, [110]*3, [1000]*3)

        p = m.pick_prices(self.conn, self.ts(1), self.TODAY)
        self.assertEqual(p.trade.ID, 1)
        self.assertEqual(p.buy, 111.1)
        self.assertEqual(p.sell, 134.2)
        self.assertEqual(p.newSell, 132)

    def testMovingPrices(self):
        self.AddPrices(1, self.TODAY, self.JITA, [90, 100, 100, 100], [100, 100, 110, 130], [1000]*4)

        p = m.pick_prices(self.conn, self.ts(1), self.TODAY)
        self.assertEqual(p.trade.ID, 1)
        self.assertEqual(p.buy, 111.1)
        self.assertEqual(p.sell, 134.2)
        self.assertEqual(p.newSell, 132)

class TestProcessOrderset(unittest.TestCase):
    JITA = 60003760
    AMARR = 60008494

    @staticmethod
    def ts(i: int):
        return trade_lib.ItemSummary(i, "", 1, 1, 1, 1, 1)

    def testStocks(self):
        im = m.ItemModel(self.ts(12608), buy=80, sell=90, newSell=90, notes=[])
        stock, _ = m.process_orderset("testdata/orderset4.csv.gz", {im.trade.ID: im}, set([self.JITA]))
        self.assertEqual(stock[im.trade.ID][self.JITA][0], 12066461)
        self.assertEqual(stock[im.trade.ID][self.JITA][1], 21482637)

    def testLowest(self):
        im = m.ItemModel(self.ts(12608), buy=80, sell=90, newSell=90, notes=[])
        _, lowest = m.process_orderset("testdata/orderset4.csv.gz", {im.trade.ID: im}, set([self.JITA, self.AMARR]))
        self.assertEqual(lowest[im.trade.ID][1], self.JITA)
        self.assertEqual(lowest[im.trade.ID][0], 72.99)

class TestSuggestStock(unittest.TestCase):
    ALLOW = [60003760, 60008494]
    DEST = 60005686

    def setUp(self):
        self.sde_conn = sqlite3.connect(":memory:")
        c = self.sde_conn.cursor()
        c.execute("""
        CREATE TABLE Stations(
          ID       INT PRIMARY KEY NOT NULL,
          Name     TEXT NOT NULL,
          SystemID INT NOT NULL,
          RegionID INT NOT NULL
        );""")
        c.execute("""INSERT INTO Stations VALUES(?,?,?,?);""", [self.ALLOW[0], "Jita 4-4", 123, 100])
        c.execute("""INSERT INTO Stations VALUES(?,?,?,?);""", [self.ALLOW[1], "Amarr EFA", 134, 101])
        c.execute("""INSERT INTO Stations VALUES(?,?,?,?);""", [self.DEST, "Hek BC", 145, 102])
        self.sde_conn.commit()

    @staticmethod
    def ts(i: int):
        return trade_lib.ItemSummary(i, "Item{}".format(i), 1, 1, 1, 1, 100)

    def testAlreadyInStock(self):
        im = m.ItemModel(self.ts(1), buy=80, sell=90, newSell=90, notes=[])
        r = m.suggest_stock(self.sde_conn, self.DEST, im, {
            self.ALLOW[0]: [2000, 0],
            self.ALLOW[1]: [1000, 0],
            self.DEST: [0, 1000],
            }, (78.4, self.ALLOW[0]), set(self.ALLOW), set())
        self.assertEqual(r.ID, 1)
        self.assertEqual(r.Name, "Item1")
        self.assertEqual(r.Quantity, 0)
        self.assertIn("already in stock", r.Notes)

    def testNoneAvailable(self):
        im = m.ItemModel(self.ts(1), buy=80, sell=90, newSell=90, notes=[])
        r = m.suggest_stock(self.sde_conn, self.DEST, im, {
            self.ALLOW[0]: [0, 10000],
            self.ALLOW[1]: [0, 1000],
            self.DEST: [0, 0],
            }, (78.4, self.ALLOW[0]), set(self.ALLOW), set())
        self.assertEqual(r.ID, 1)
        self.assertEqual(r.Name, "Item1")
        self.assertEqual(r.Quantity, 0)
        self.assertIn("not available", r.Notes)



unittest.main()
