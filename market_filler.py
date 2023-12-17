#!/usr/bin/python3

from argparse import ArgumentParser
import csv
from dataclasses import dataclass
import datetime
import logging
import math
import sqlite3
import sys
from typing import Dict, Iterator, Set, Tuple

import lib
import trade_lib

logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
log = logging.getLogger(__name__)

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
      GROUP BY Date)""", [type_id, date.isoformat()])
    fair_price = res.fetchall()[0][0]
    return ItemPricing(other_stations=current_prices, fair_price=fair_price)

def process_orderset(ofile: str, station: int, oinfo: lib.OrdersetInfo) -> Iterator[Tuple[int, float]]:
    current_type = None
    current_type_best_sell = None
    log.info("orderset file '{}'".format(ofile))
    for x in lib.read_orderset_filter(ofile, oinfo):

        if current_type is not None and current_type != x.TypeID:
            if current_type_best_sell is not None:
                yield (current_type, current_type_best_sell)
            current_type_best_sell = None
            current_type = None

        if station != x.StationID: continue
        if x.IsBuy: continue

        current_type = x.TypeID
        if current_type_best_sell is None or current_type_best_sell > x.Price:
            current_type_best_sell = x.Price
    # read_orderset() always pads the end with one bogus order line with a dummy item & station,
    # so we never have to emit the final item or station returned.

def guess_min_order(i: trade_lib.ItemSummary):
    if i.CategoryID == 8:  # Charges
        if i.GroupID in (86, 374, 375):  # Crystals
            return 20
        else:
            return 2000
    if i.CategoryID == 87:  # Fighter
        return 10
    return 1

def suggest_stock(sde_conn: sqlite3.Connection, prices_conn: sqlite3.Connection, items: Dict[int, trade_lib.ItemSummary], station_prices: Dict[int, float], allowed_sources: Set[int], date: datetime.date, w):
    w.writerow(["TypeID", "Item Name", "StationID", "Station Name", "Price", "Quantity", "Value"])
    for type_id, info in items.items():
        type_info = lib.get_type_info(sde_conn, type_id)
        min_order = guess_min_order(type_info)
        availability = get_pricing(prices_conn, type_id, date)

        if type_id in station_prices and station_prices[type_id] < availability.fair_price * 1.2:
            log.debug("{}({}): already available at {} (vs {})".format(type_info.Name, type_id, station_prices[type_id], availability.fair_price))
            continue

        market_quantity = math.floor(info.ValueTraded / availability.fair_price)
        stock_quantity = math.floor(market_quantity / 50)
        if stock_quantity < min_order:
            log.debug("{}({}): market_quantity={} min_order={} - not ordering".format(type_info.Name, type_id, market_quantity, min_order))
            continue
        else:
            stock_quantity = min_order * math.ceil(stock_quantity/min_order)

        considered = 0
        sources = [(s, p) for s, p in availability.other_stations.items()
                # Must have a sell price and some stock and be an allowed source for shippping.
                if p[0] is not None and p[1] is not None and s in allowed_sources]
        for s, p in sorted(sources, key=lambda i: i[1][0]):
            station_info = lib.get_station_info(sde_conn, s)
            considered += 1
            if p[0] < availability.fair_price and p[1] >= stock_quantity/2:
                buy_quantity = min(p[1], stock_quantity)
                w.writerow([type_id, type_info.Name, s, station_info.Name, p[0], buy_quantity, p[0]*buy_quantity])
                # Found best station to source this item - don't say any others.
                break
            else:
                log.debug("{}({}) not available in quantity at station {} (price {} quantity {})".format(type_info.Name, type_id, station_info.Name, p[0], p[1]))
        if considered == 0:
            log.debug("{}({}): wanted to get but no sources considered. other_stations={} vs allowed={}".format(type_info.Name, type_id, availability.other_stations, allowed_sources))

def main():
    arg_parser = ArgumentParser(prog='calc-market-quality.py')
    arg_parser.add_argument('--orderset', type=str)
    arg_parser.add_argument('--limit-top-traded-items', type=int)
    arg_parser.add_argument('--from-stations', nargs='*', type=int)
    arg_parser.add_argument('--station', type=int)
    args = arg_parser.parse_args()

    sde_conn = sqlite3.connect("sde.db")
    prices_conn = sqlite3.connect("market-prices.db")

    with open("top-traded.csv","rt") as tt_fh:
        items = {s.ID: s for s in trade_lib.get_most_traded_items(tt_fh, args.limit_top_traded_items)}
        log.info("Basket of items loaded, {} items".format(len(items)))

    oinfo = lib.OrdersetInfo(None, None)
    prices_at_station = {
        i: p for i, p in process_orderset(args.orderset, args.station, oinfo)
    }

    log.info("Prices at station {} retrieved: {} items available".format(args.station, len(prices_at_station)))
    w = csv.writer(sys.stdout)
    suggest_stock(sde_conn, prices_conn, items, prices_at_station, set(args.from_stations), oinfo.Date.date(), w)


if __name__ == "__main__":
    main()
