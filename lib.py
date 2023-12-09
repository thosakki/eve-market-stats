from collections import namedtuple
import csv
import gzip
import sqlite3

Order = namedtuple('Order', ['TypeID', 'StationID', 'IsBuy', 'Price', 'Volume'])
StationInfo = namedtuple('StationInfo', ['ID', 'Name', 'SystemID', 'RegionID'])
TypeInfo = namedtuple('TypeInfo', ['ID', 'Name', 'GroupID', 'GroupName', 'CategoryID', 'CategoryName'])

def get_type_info(cur: sqlite3.Cursor, type_id: int) -> TypeInfo:
    res = cur.execute("""
    SELECT Types.ID, Types.name, Groups.ID, Groups.Name, Categories.ID, Categories.Name
    FROM Types JOIN Groups ON (Types.GroupID = Groups.ID)
        JOIN Categories ON (Categories.ID = Groups.CategoryID)
    WHERE Types.ID = ?
    """, [name])
    r = res.fetchall()
    if len(r) == 0:
        log.info("Could not find type '{}'".format(name))
        return None
    if len(r) > 1:
        raise RuntimeError("multiple values for a name from SDE")
    row = r[0]
    return TypeInfo(row[0], row[1], row[2], row[3], row[4], row[5])

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

def read_orderset(orderset_file: str):
    with gzip.open(orderset_file, "rt") as ofh:
        r = csv.reader(ofh, delimiter="\t")
        for row in r:
            # 911190994	41	2023-11-26T06:52:24Z	False	23572	23572	1	17.86	60000004	region	365	10000033	126876
            _, typeID, _, is_buy, volume, _, _, price, stationID, _, _, _, _ = row
            yield Order(TypeID=int(typeID), StationID=int(stationID), IsBuy=(is_buy=='True'), Price=float(price), Volume=int(volume))
        yield Order(TypeID=0, StationID=0, IsBuy=False, Price=0, Volume=0)

