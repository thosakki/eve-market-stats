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


unittest.main()
