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
        AND StationID IN (60003760, 60011866, 60008494) -- Jita 4-4, Dodixie FNAP, Amarr EFA
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

def suggest_stock(sde_conn: sqlite3.Connection, prices_conn: sqlite3.Connection, items: Dict[int, trade_lib.ItemSummary], station_prices: Dict[int, float], allowed_sources: Set[int], industry_items: Set[int], date: datetime.date, w):
    w.writerow(["TypeID", "Item Name", "Quantity", "Max Buy", "Value", "My Sell Price", "StationID", "Station Name", "Industry", "Current Price", "Notes"])
    for type_id, info in items.items():
        type_info = lib.get_type_info(sde_conn, type_id)
        availability = get_pricing(prices_conn, type_id, date)
        min_order = guess_min_order(info)
        current_price = station_prices.get(type_id)
        notes = []
        buy_quantity = buy_price = sell_price = station_id = station_name = None
        industry = False

        if availability.fair_price is None:
            notes.append("no fair price available")
        elif current_price is not None and current_price < availability.fair_price * 1.22:
            notes.append("already available at {} (vs {})".format(station_prices[type_id], availability.fair_price))
        else:
            market_quantity = math.floor(info.ValueTraded / availability.fair_price)
            stock_quantity = math.floor(market_quantity / 25)

            # Only do not order if the stock quantity is much less than our minimum order size.
            if 2*stock_quantity < min_order:
                notes.append("order size {} too small".format(min_order))
            else:
                stock_quantity = min_order * math.ceil(stock_quantity/min_order)

                considered = 0
                sources = [(s, p) for s, p in availability.other_stations.items()
                    # Must have a sell price and some stock and be an allowed source for shippping.
                    if p[0] is not None and p[1] is not None and s in allowed_sources]
                for s, p in sorted(sources, key=lambda i: i[1][0]):
                    station_info = lib.get_station_info(sde_conn, s)
                    if station_info is None:
                        log.info("station {} unknown", s)
                    considered += 1
                    if p[1] < math.ceil(stock_quantity/5):
                        notes.append("not available in quantity at station {} (price {} quantity {} want quantity {})".format(station_info.Name, p[0], p[1], stock_quantity))
                    elif p[0] > availability.fair_price*0.98:
                        notes.append("not available at good price at station {} (price {} quantity {} want price {})".format(station_info.Name, p[0], p[1], availability.fair_price))
                    else:
                        buy_quantity = min(p[1], stock_quantity)
                        sell_price = availability.fair_price * 1.18
                        buy_price = p[0]*1.01 if p[0] < sell_price/1.25 else sell_price/1.25
                        station_id = s
                        station_name = station_info.Name
                        # Found best station to source this item - don't say any others.
                        break

                if buy_quantity is None and type_id in industry_items:
                    industry = True
                    sell_price = availability.fair_price*1.2
                    buy_price = availability.fair_price
                    buy_quantity = availability.fair_price*stock_quantity
                elif considered == 0:
                    notes.append("wanted to get but no sources considered. other_stations={} vs allowed={}".format(availability.other_stations, allowed_sources))

        w.writerow([type_id, type_info.Name,
            buy_quantity, buy_price, buy_price*buy_quantity if buy_quantity else None,
            sell_price, station_id, station_name, "Y" if industry else "N", ",".join(notes)])

def read_industry_items(conn: sqlite3.Connection, filename: str) -> Set[int]:
    res = set()
    with open(filename, "rt") as f:
        for i in f:
            name = i.rstrip()
            item = lib.get_type_info_byname(conn.cursor(), name)
            if item is None:
                log.warning("unrecognised item {}".format(name))
                continue
            res.add(item.ID)
    return res

def main():
    arg_parser = ArgumentParser(prog='calc-market-quality.py')
    arg_parser.add_argument('--orderset', type=str)
    arg_parser.add_argument('--industry', type=str)
    arg_parser.add_argument('--limit-top-traded-items', type=int)
    arg_parser.add_argument('--from-stations', nargs='*', type=int)
    arg_parser.add_argument('--station', type=int)
    args = arg_parser.parse_args()

    sde_conn = sqlite3.connect("sde.db")
    prices_conn = sqlite3.connect("market-prices.db")

    with open("top-traded.csv","rt") as tt_fh:
        items = {s.ID: s for s in trade_lib.get_most_traded_items(tt_fh, args.limit_top_traded_items)}
        log.info("Basket of items loaded, {} items".format(len(items)))

    industry_items = read_industry_items(sde_conn, args.industry) if args.industry else set()

    oinfo = lib.OrdersetInfo(None, None)
    prices_at_station = {
        i: p for i, p in process_orderset(args.orderset, args.station, oinfo)
    }

    log.info("Prices at station {} retrieved: {} items available".format(args.station, len(prices_at_station)))
    w = csv.writer(sys.stdout)
    suggest_stock(sde_conn, prices_conn, items, prices_at_station, set(args.from_stations), industry_items, oinfo.Date.date(), w)


if __name__ == "__main__":
    main()
