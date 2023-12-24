#!/usr/bin/python3

from argparse import ArgumentParser
from collections import defaultdict
import csv
from dataclasses import dataclass
import datetime
import logging
import math
import sqlite3
import sys
from typing import Dict, Iterator, List, Optional, Set, Tuple

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

@dataclass
class ItemModel:
    trade : trade_lib.ItemSummary
    buy: Optional[float]  # the price at which we are willing to buy
    sell: Optional[float] # the price at which we are happy to see this selling
    newSell: Optional[float] # the price we would list new sales at
    notes: List[str]

def pick_prices(prices_conn: sqlite3.Connection, trade_summary: trade_lib.ItemSummary, date) -> trade_lib.ItemSummary:
    type_id = trade_summary.ID
    availability = get_pricing(prices_conn, type_id, date)
    imodel = ItemModel(trade=trade_summary, notes = [], buy=None, sell=None, newSell=None)

    if availability.fair_price is None:
        imodel.notes.append("no fair price available")
        return None

    # allow buying slightly over the fair price.
    imodel.buy = availability.fair_price*1.01
    imodel.newSell = availability.fair_price*1.2
    # Acceptable sell price is slightly higher, we need some hysteresis so that we don't
    # keep adjusting orders constantly.
    imodel.sell = availability.fair_price*1.22
    return imodel

def get_orderset_info(ofile: str) -> lib.OrdersetInfo:
    oinfo = lib.OrdersetInfo(None, None)
    for _ in lib.read_orderset_filter(ofile, oinfo):
        pass
    return oinfo

def process_orderset(ofile: str, market_model: Dict[int, ItemModel], stations: Set[int]) -> Tuple[Dict[int, Dict[int, List]], Dict[int, Tuple[float, int]]]:
    # Per item, per station, stocks below buy and sell prices
    stock_per_station = defaultdict(lambda: defaultdict(lambda: [0,0]))
    lowest_sell = defaultdict(lambda: (1e99,0))
    log.info("reading orderset file '{}'".format(ofile))
    for x,_ in lib.read_orderset(ofile):
        if x.StationID not in stations: continue
        if x.TypeID not in market_model: continue
        if x.IsBuy: continue

        if x.Price < market_model[x.TypeID].buy:
            stock_per_station[x.TypeID][x.StationID][0] += x.Volume
        if x.Price < market_model[x.TypeID].sell:
            stock_per_station[x.TypeID][x.StationID][1] += x.Volume
        if x.Price < lowest_sell[x.TypeID][0]:
            lowest_sell[x.TypeID] = (x.Price, x.StationID)

    # Convert to a regular dict, to avoid exposing implementation.
    return ({i: dict(v) for i,v in stock_per_station.items()},
            dict(lowest_sell))

def guess_min_order(i: trade_lib.ItemSummary):
    if i.CategoryID == 8:  # Charges
        if i.GroupID in (86, 374, 375):  # Crystals
            return 20
        else:
            return 2000
    if i.CategoryID == 87:  # Fighter
        return 10
    return 1

def suggest_stock(sde_conn: sqlite3.Connection, prices_conn: sqlite3.Connection, station: int, item: ItemModel, station_stocks: Dict[int, int], lowest_sell: Tuple[float, int], allowed_stations: Set[int], industry_items: Set[int], date: datetime.date):
    min_order = guess_min_order(item.trade)
    notes = item.notes
    industry = item.trade.ID in industry_items

    market_quantity = math.floor(item.trade.ValueTraded / item.buy)
    stock_quantity = math.floor(market_quantity / 25)

    # Reduce potential order by the amount of existing stock below the target sale price.
    existing_stock = station_stocks.get(station, [0,0])[1]
    stock_quantity = max(0, stock_quantity-existing_stock)
    from_station = None
    station_info = None

    if stock_quantity < min_order/2:
        notes.append("already in stock below target price, volume={}".format(existing_stock))
    else:
        if existing_stock > 0:
            notes.append("some stock below target price, volume={}".format(existing_stock))
        stock_quantity = min_order * math.ceil(stock_quantity/min_order)

        # We favour the lowest-price station if it has sufficient stock and is allowed.
        if lowest_sell[1] in allowed_stations and lowest_sell[0] > min_order and lowest_sell[0] > stock_quantity / 2:
            from_station=lowest_sell[1]
            station_info = lib.get_station_info(sde_conn, from_station)
            stock_quantity = min([lowest_sell[0], stock_quantity])
        else:
            # Prefer stations with most stock in the target price range.
            for station, stock in sorted(station_stocks.items(), key=lambda x: x[1][0], reverse=True):
                if station not in allowed_stations: continue
                station_info = lib.get_station_info(sde_conn, station)
                if stock[0] == 0:
                    notes.append("not available at station {} (quantity {})".format(station_info.Name, stock_quantity))
                elif stock[0] < min_order or stock[0] < stock_quantity / 2:
                    notes.append("not available in quantity at station {} for target price (want {} available {})".format(station_info.Name, stock_quantity, stock[0]))
                else:
                   stock_quantity = min([stock[0], stock_quantity])
                   from_station = station
                   break

    # "TypeID", "Item Name", "Quantity", "Max Buy", "Value", "My Sell Price", "StationIDs", "Station Names", "Industry", "Current Price", "Notes"])
    return (
            item.trade.ID, item.trade.Name,stock_quantity,item.buy,item.newSell * stock_quantity if from_station else 0,item.newSell,from_station,station_info.Name if from_station else "-",
            "Y" if industry else "N", ",".join(notes)
            )

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

def item_order_key(s: trade_lib.ItemSummary):
    return (s.CategoryID, s.GroupID)

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

    oinfo = get_orderset_info(args.orderset)
    log.info("orderset {}: #{}, {}".format(args.orderset, oinfo.Orderset, oinfo.Date))
    market_model = {
            i: pick_prices(prices_conn, item, oinfo.Date) for i, item in items.items()}
    market_model = {i: m for i, m in market_model.items() if m is not None}

    industry_items = read_industry_items(sde_conn, args.industry) if args.industry else set()

    all_stations = set(args.from_stations)
    all_stations.add(args.station)
    item_stocks, lowest_sell = process_orderset(args.orderset, market_model, all_stations)

    trade_suggestions = [
        suggest_stock(sde_conn, prices_conn, args.station, market_model[i], s, lowest_sell[i], set(args.from_stations), industry_items, oinfo.Date.date()) for i, s in item_stocks.items()]

    w = csv.writer(sys.stdout)
    w.writerow(["TypeID", "Item Name", "Quantity", "Max Buy", "Value", "My Sell Price", "StationIDs", "Station Names", "Industry", "Current Price", "Notes"])
    for s in sorted(trade_suggestions, key=lambda x: item_order_key(market_model[x[0]].trade)):
        w.writerow(s)


if __name__ == "__main__":
    main()
