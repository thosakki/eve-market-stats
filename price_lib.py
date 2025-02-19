from dataclasses import dataclass
import datetime
import sqlite3
from typing import Dict, Tuple

@dataclass
class ItemPricing():
    other_stations: Dict[int, Tuple[float, float]]
    fair_price: float

def get_pricing(conn: sqlite3.Connection, type_id: int, date: datetime.date) -> ItemPricing:
    current_prices = dict()
    res = conn.execute("""
    SELECT StationID, Buy, Sell, SellVolume FROM PriceHistory
    WHERE TypeID=? AND Date=?""", [type_id, date.isoformat()])
    current_prices = {
            r[0]: (r[2], r[3]) for r in res.fetchall()
            }
    res = conn.execute("""
    SELECT AVG(daily_price) FROM (
      SELECT Date,MIN(Sell) AS daily_price FROM PriceHistory
      WHERE TypeID=? AND Date > date(?, "-3 months")
        AND StationID IN (60003760, 60011866, 60008494) -- Jita 4-4, Dodixie FNAP, Amarr EFA
      GROUP BY Date)""", [type_id, date.isoformat()])
    fair_price = res.fetchall()[0][0]
    return ItemPricing(other_stations=current_prices, fair_price=fair_price)

