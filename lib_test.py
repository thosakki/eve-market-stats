from datetime import datetime
import sqlite3
import unittest

import lib
Order = lib.Order

# Show full diff in unittest
unittest.util._MAX_LENGTH=2000

class TestGetTypeInfo(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        c = self.conn.cursor()
        c.execute("""
        CREATE TABLE Types(
          ID      INT PRIMARY KEY NOT NULL,
          Name    TEXT NOT NULL,
          GroupID INT NOT NULL
        );""")
        c.execute("""
        CREATE UNIQUE INDEX Types_ByName ON Types(Name);
        """)
        c.execute("""
        CREATE TABLE Groups(
          ID      INT PRIMARY KEY NOT NULL,
          Name    TEXT NOT NULL,
          CategoryID INT NOT NULL
        );""")
        c.execute("""
        CREATE UNIQUE INDEX Groups_ByName ON Groups(Name);
        """)
        c.execute("""
        CREATE TABLE Categories(
          ID      INT PRIMARY KEY NOT NULL,
          Name    TEXT NOT NULL
        );""")
        c.execute("""
        CREATE UNIQUE INDEX Categories_ByName ON Categories(Name);
        """)
        c.execute("""INSERT INTO Types VALUES(?,?,?)""", [1, "Multispectrum Energized Membrane I", 123])
        c.execute("""INSERT INTO Types VALUES(?,?,?)""", [2, "Multispectrum Energized Membrane II", 123])
        c.execute("""INSERT INTO Groups VALUES(?,?,?)""", [123, "Modules", 1234])
        c.execute("""INSERT INTO Categories VALUES(?,?)""", [1234, "Cat"])

    def tearDown(self):
        self.conn.close()

    def testGetSuccessful(self):
        t = lib.get_type_info(self.conn.cursor(), 1)
        self.assertEqual(t.ID, 1)
        self.assertEqual(t.Name, "Multispectrum Energized Membrane I")
        self.assertEqual(t.GroupID, 123)

    def testGetFail(self):
        self.assertIsNone(lib.get_type_info(self.conn.cursor(), 99))

class TestGetTypeInfo(unittest.TestCase):
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
        c.execute("""INSERT INTO Stations VALUES(?,?,?,?);""", [1234, "Amo - Minmatar Fleet Market", 123, 100])
        c.execute("""INSERT INTO Stations VALUES(?,?,?,?);""", [4321, "Jita IV - Moon 4 - Caldari Navy Assembly Plant", 12, 20])

    def tearDown(self):
        self.conn.close()

    def testGetStationInfo(self):
        s = lib.get_station_info(self.conn.cursor(), 1234)
        self.assertEqual(s.ID, 1234)
        self.assertEqual(s.Name, "Amo - Minmatar Fleet Market")
        self.assertEqual(s.SystemID, 123)
        self.assertEqual(s.RegionID, 100)

    def testGetStationInfoFailed(self):
        self.assertIsNone(lib.get_station_info(self.conn.cursor(), 9999))

    def testGetStationInfo(self):
        s = lib.get_station_info_byname(self.conn.cursor(), "Jita IV - Moon 4 - Caldari Navy Assembly Plant")
        self.assertEqual(s.ID, 4321)
        self.assertEqual(s.Name, "Jita IV - Moon 4 - Caldari Navy Assembly Plant")
        self.assertEqual(s.SystemID, 12)
        self.assertEqual(s.RegionID, 20)

    def testGetStationInfoFailed(self):
        self.assertIsNone(lib.get_station_info_byname(self.conn.cursor(), "Jita IV - Moon 3 - Not Here"))

class TestReadOrderset(unittest.TestCase):
    def runTest(self):
        d = [x for x in lib.read_orderset("testdata/orderset.csv.gz")]
        orders, ordersets = list(zip(*d))
        for o in ordersets[:-2]: self.assertEqual(o, 100177)
        self.assertEqual(orders[0], Order(1109, 60013330, False, 199680.0, 1, datetime.fromisoformat("2022-02-21T11:02:58")))
        self.assertEqual(orders[1], Order(1109, 60013333, False, 199680.0, 1, datetime.fromisoformat("2022-03-29T11:07:07")))
        self.assertEqual(orders[2], Order(1109, 60013336, False, 199680.0, 1, datetime.fromisoformat("2022-02-15T11:06:35")))
        self.assertEqual(orders[3], Order(1109, 60013339, False, 199680.0, 1, datetime.fromisoformat("2022-04-28T11:02:59")))


unittest.main()
