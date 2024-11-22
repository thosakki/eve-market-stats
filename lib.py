from collections import namedtuple
import csv
from dataclasses import dataclass
from datetime import datetime
import gzip
import sqlite3
from typing import Iterator, Optional, Tuple

Order = namedtuple('Order', ['TypeID', 'StationID', 'IsBuy', 'Price', 'Volume', 'Date'])
StationInfo = namedtuple('StationInfo', ['ID', 'Name', 'SystemID', 'RegionID'])
TypeInfo = namedtuple('TypeInfo', ['ID', 'Name', 'GroupID', 'GroupName', 'CategoryID', 'CategoryName', 'MarketGroup'])

@dataclass
class OrdersetInfo:
    Orderset: Optional[int]
    Date: Optional[datetime.date]

def get_type_info(cur: sqlite3.Cursor, type_id: int) -> TypeInfo:
    res = cur.execute("""
    SELECT Types.ID, Types.name, Groups.ID, Groups.Name, Categories.ID, Categories.Name, MarketGroups.Path
    FROM Types JOIN Groups ON (Types.GroupID = Groups.ID)
        JOIN Categories ON (Categories.ID = Groups.CategoryID)
    LEFT JOIN MarketGroups ON (MarketGroups.ID = Types.MarketGroupID)
    WHERE Types.ID = ?
    """, [type_id])
    r = res.fetchall()
    if len(r) == 0:
        return None
    if len(r) > 1:
        raise RuntimeError("multiple values for a name from SDE")
    row = r[0]
    return TypeInfo(row[0], row[1], row[2], row[3], row[4], row[5], row[6])

def get_type_info_byname(cur: sqlite3.Cursor, name: str) -> TypeInfo:
    res = cur.execute("""
    SELECT Types.ID, Types.name, Groups.ID, Groups.Name, Categories.ID, Categories.Name, MarketGroups.Path
    FROM Types JOIN Groups ON (Types.GroupID = Groups.ID)
        JOIN Categories ON (Categories.ID = Groups.CategoryID)
    LEFT JOIN MarketGroups ON (MarketGroups.ID = Types.MarketGroupID)
    WHERE Types.Name = ?
    """, [name])
    r = res.fetchall()
    if len(r) == 0:
        #log.info("Could not find type '{}'".format(name))
        return None
    if len(r) > 1:
        raise RuntimeError("multiple values for a name from SDE")
    row = r[0]
    return TypeInfo(row[0], row[1], row[2], row[3], row[4], row[5], row[6])

def get_station_info(cur: sqlite3.Cursor, stationID: int) -> StationInfo:
    res = cur.execute("""
    SELECT ID, Name, SystemID, RegionID
    FROM Stations
    WHERE ID = ?
    """, [stationID])
    r = res.fetchall()
    if len(r) == 0:
        return None
    if len(r) > 1:
        raise RuntimeError("multiple values for a name from SDE")
    row = r[0]
    return StationInfo(row[0], row[1], row[2], row[3])

def get_station_info_byname(cur: sqlite3.Cursor, station: str) -> StationInfo:
    res = cur.execute("""
    SELECT ID, Name, SystemID, RegionID
    FROM Stations
    WHERE Name = ?
    """, [station])
    r = res.fetchall()
    if len(r) == 0:
        return None
    if len(r) > 1:
        raise RuntimeError("multiple values for a name from SDE")
    row = r[0]
    return StationInfo(row[0], row[1], row[2], row[3])

def get_system_info(cur: sqlite3.Cursor, systemID: int) -> (str, float):
    res = cur.execute("""
        SELECT Name, Security
        FROM Systems
        WHERE ID = ?
        """, [systemID])
    r = res.fetchall()
    if len(r) == 0:
        return None
    if len(r) > 1:
        raise RuntimeError("multiple values for a name from SDE")
    row = r[0]
    return (row[0], row[1])

def read_orderset(orderset_file: str) -> Iterator[Tuple[Order, int]]:
    with gzip.open(orderset_file, "rt") as ofh:
        r = csv.reader(ofh, delimiter="\t")
        for row in r:
            # 911190994	41	2023-11-26T06:52:24Z	False	23572	23572	1	17.86	60000004	region	365	10000033	126876
            _, typeID, date, is_buy, volume, _, _, price, stationID, _, _, _, orderset = row
            date = date.rstrip('Z')  # python <3.11 doesn't know Z.
            yield Order(TypeID=int(typeID), StationID=int(stationID), IsBuy=(is_buy=='True'), Price=float(price), Volume=int(volume), Date=datetime.fromisoformat(date)), int(orderset)
        yield Order(TypeID=0, StationID=0, IsBuy=False, Price=0, Volume=0, Date=None), 0

def read_orderset_filter(orderset_file: str, oinfo: OrdersetInfo) -> Iterator[Order]:
    for x, item_orderset in read_orderset(orderset_file):
        yield x

        assert oinfo.Orderset is None or oinfo.Orderset == item_orderset or item_orderset == 0
        if item_orderset > 0: oinfo.Orderset = item_orderset
        if oinfo.Date is None or (x.Date is not None and oinfo.Date < x.Date): oinfo.Date = x.Date
