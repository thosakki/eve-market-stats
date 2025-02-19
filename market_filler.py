#!/usr/bin/python3

from argparse import ArgumentParser
from collections import defaultdict, namedtuple
import csv
from dataclasses import dataclass
import datetime
from fnmatch import fnmatch
import industry
import logging
import math
import sqlite3
import sys
from typing import Dict, List, Optional, Set, Tuple
import yaml

import lib
from price_lib import get_pricing
import trade_lib

log = logging.getLogger(__name__)

def get_sources(conn: sqlite3.Connection, to: str, fname: str) -> (int, Dict[int, dict]):
    with open(fname, "rt") as fh:
        d = yaml.safe_load(fh)
        for x in d:
            if x['id'] == to:
                to_id = lib.get_station_info_byname(conn, x['to']).ID
                from_stations = {
                        lib.get_station_info_byname(conn, y['name']).ID: {
                            'isk_cost': y.get('isk_cost', 0),
                            'vol_cost': y.get('isk_cost', 0)
                            }
                        for y in x['from']
                        }
                return to_id, from_stations

        raise RuntimeError('failed to find source {}'.format(to))

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
        return imodel

    # allow buying slightly over the fair price.
    imodel.buy = availability.fair_price*1.01
    imodel.newSell = availability.fair_price*1.24
    # Acceptable sell price is slightly higher, we need some hysteresis so that we don't
    # keep adjusting orders constantly.
    imodel.sell = availability.fair_price*1.26
    return imodel

def get_orderset_info(ofile: str) -> lib.OrdersetInfo:
    oinfo = lib.OrdersetInfo(None, None)
    for _ in lib.read_orderset_filter(ofile, oinfo):
        pass
    return oinfo

def process_orderset(ofile: str, market_model: Dict[int, ItemModel], stations: Set[int]) -> Tuple[Dict[int, Dict[int, List]], Dict[int, Tuple[float, int]]]:
    # Per item, per station, stocks below buy and sell prices
    stock_per_station = {i: defaultdict(lambda: [0,0]) for i in market_model.keys()}
    lowest_sell = defaultdict(lambda: (1e99,0))
    log.info("reading orderset file '{}'".format(ofile))
    for x,_ in lib.read_orderset(ofile):
        if x.StationID not in stations: continue
        if x.TypeID not in market_model: continue
        if x.IsBuy: continue

        try:
            if market_model[x.TypeID].buy is None: continue
            if x.Price < market_model[x.TypeID].buy:
                stock_per_station[x.TypeID][x.StationID][0] += x.Volume
            if x.Price < market_model[x.TypeID].sell:
                stock_per_station[x.TypeID][x.StationID][1] += x.Volume
            if x.Price < lowest_sell[x.TypeID][0]:
                lowest_sell[x.TypeID] = (x.Price, x.StationID)
        except TypeError as e:
            raise RuntimeError("failed to parse {} (mm {}): {}".format(x, market_model[x.TypeID], e))

    # Convert to a regular dict, to avoid exposing implementation.
    return ({i: dict(v) for i,v in stock_per_station.items()},
            dict(lowest_sell))

Result = namedtuple('Result', ['ID', 'Name', 'BuyQuantity', 'MaxBuy', 'MyAssets', 'MyCurrentSell', 'SellQuantity', 'MySell', 'StockQuantity', 'FromStationID', 'FromStationName', 'ToStationID', 'ToStationName', 'IndustryCost', 'BuildQuantity', 'AdjustOrder', 'Notes'])

def bool_to_str(b: bool) -> str:
    return "Y" if b else "N"

def suggest_stock(station: int, item: ItemModel, station_stocks: Dict[int, int], current_assets: int, current_order: Optional[Tuple[int, float]], industry_items: Set[int], stock_fraction: float) -> Result:
    min_order = trade_lib.get_order_size(item.trade).MinOrderSize
    notes = item.notes

    market_quantity = math.floor(item.trade.ValueTraded / item.buy)
    # original_stock_quantity how much supply of this item we want to be available on the market.
    original_stock_quantity = min_order * math.floor(
            0.5 + math.floor(market_quantity * stock_fraction)/min_order)

    # Reduce potential order by the amount of existing stock below the target sale price.
    existing_stock = station_stocks.get(station, [0,0])[1]
    competitor_stock = existing_stock
    if current_order is not None and current_order[1] <= item.newSell:
        competitor_stock -= current_order[0]
    # Intentionally reduce the *total* target stock level (including competitors) by the amount
    # of any competitor's stocks.
    # So if a competitor is stocking half of our target level, we halve the target stock here and then
    # their stock covers that amount (subtracted again below). If they stock 1/4 of the target, we
    # reduce our purchase by 1/2 in total.
    # Basically we assume that if a competitor is stocking a substantial amount now, they will stock more
    # later and we should greatly reduce or even not bother trying to supply it ourselves.
    original_stock_quantity = max(0, original_stock_quantity-competitor_stock)

    # stock_quantity is how much more *we* want to supply to the market (not including any stock that
    # we already listed) - before considering availability.
    if existing_stock > 0:
        # We reduce our potential stocking even more aggressively if we have an order up already
        # note that existing stock may include an existing order, so this is really:
        # competitor stock*2 + my_stock_below_target*2 + my_stock_total*2
        if existing_stock*2 + (current_order[0]*2 if current_order is not None else 0) > original_stock_quantity:
            stock_quantity = 0
            notes.append("already in stock below target price, volume={}".format(existing_stock))
        else:
            stock_quantity = max(0, original_stock_quantity - existing_stock)
            notes.append("some stock below target price, volume={}".format(existing_stock))
    else:
        stock_quantity = original_stock_quantity

    # buy_quantity is how much we therefore want to buy or build.
    adjust_order = False
    buy_quantity = max(0, stock_quantity - current_assets)
    if current_order is not None:
        notes.append("already listed for sale, volume={}".format(current_order[0]))
        buy_quantity = max(0, buy_quantity - current_order[0])

        if item.newSell * 1.1 < current_order[1]:
            adjust_order=True

    return Result(ID=item.trade.ID, Name=item.trade.Name,
                  BuyQuantity=buy_quantity, MaxBuy=item.buy,
                  MyAssets=current_assets,
                  MyCurrentSell=(current_order[0] if current_order else 0),
                  StockQuantity=original_stock_quantity,
                  IndustryCost=industry_items.get(item.trade.ID),
                  AdjustOrder=bool_to_str(adjust_order),
                  MySell=item.newSell,
                  Notes=notes,
                  # Before going to SuggestBuys, SellQuantity is the amount we
                  # want additionally to sell if we buy/build *nothing*.
                  SellQuantity=stock_quantity - buy_quantity,
                  FromStationID=None,
                  FromStationName=None,
                  ToStationID=None,
                  ToStationName=None,
                  BuildQuantity=None)

def suggest_buys(sde_conn: sqlite3.Connection, r: Result, item: ItemModel, station_stocks: Dict[int, int], lowest_sell: Tuple[float, int], allowed_stations: Set[int]) -> Result:
    min_order = trade_lib.get_order_size(item.trade).MinOrderSize
    from_station = None
    station_info = None
    notes = []
    build_quantity = 0

    if r.BuyQuantity < min_order/2:
        if r.MyCurrentSell == 0 and r.MyAssets == 0:
            notes.append("target stock quantity too low, original_stock_quantity={}, buy_quantity={}, min_order={}".format(r.StockQuantity, r.BuyQuantity, min_order))
        buy_quantity = 0
    elif r.IndustryCost and lowest_sell[0] > r.IndustryCost*1.1:
        buy_quantity = 0
        build_quantity = r.BuyQuantity
    else:
        buy_quantity = min_order * math.ceil(r.BuyQuantity/min_order)

        # Prefer station with lowest price, then stations with most stock in the target price range.
        for station, stock in sorted(station_stocks.items(), key=lambda x: (x[0] == lowest_sell[1],x[1][0]), reverse=True):
            if station not in allowed_stations: continue
            station_info = lib.get_station_info(sde_conn, station)
            if stock[0] == 0:
                notes.append("not available at station {} (quantity {})".format(station_info.Name, buy_quantity))
            elif stock[0] < min_order or stock[0] < buy_quantity / 2:
                notes.append("not available in quantity at station {} for target price (want {} available {})".format(station_info.Name, buy_quantity, stock[0]))
            else:
               buy_quantity = min([stock[0], buy_quantity])
               from_station = station
               break

        if from_station is None:
            buy_quantity = 0

    return r._replace(
            Notes=r.Notes + notes,
            FromStationID=from_station,
            BuyQuantity=buy_quantity,
            SellQuantity=min(buy_quantity+r.SellQuantity, max(r.StockQuantity - r.MyCurrentSell, 0)),
            BuildQuantity=build_quantity,
            FromStationName=station_info.Name if from_station else "-"
            )

def read_assets(files: str) -> Dict[int, int]:
    res = defaultdict(int)
    for filename in files:
        i = 0
        with open(filename, "rt") as f:
            reader = csv.DictReader(f)
            for r in reader:
                i += 1
                if r['Singleton'] == 'True': continue
                res[int(r['TypeID'])] += int(r['Quantity'])
        log.info("Read asset file {}: {} assets.".format(filename, i))
    return dict(res)

def read_orders(station: int, files: str) -> Dict[int, int]:
    res = defaultdict(lambda: [0, None])
    for filename in files:
        with open(filename, "rt") as f:
            i = 0
            # TypeID,Quantity,Original Quantity,Price,LocationID
            reader = csv.DictReader(f)
            for r in reader:
                i += 1
                if int(r['LocationID']) != station: continue
                typeID = int(r['TypeID'])
                res[typeID][0] += int(r['Quantity'])
                if res[typeID][1] is not None:
                    res[typeID][1] = min(res[typeID][1], float(r['Price']))
                else:
                    res[typeID][1] = float(r['Price'])
        log.info("Read order file {}: {} orders.".format(filename, i))
    return dict(res)

def read_market_paths(filename: str) -> List[str]:
    res = []
    with open(filename, "rt") as f:
        for l in f:
            l = l.rstrip()
            # could do validation here
            res.append(l)
    return res

def decide_actions(sde_conn: sqlite3.Connection, station: int, item, s,lowest_sell, from_stations, assets, orders, industry, stock_fraction: float):
    r = suggest_stock(station, item, s, assets, orders, industry, stock_fraction)
    r = suggest_buys(sde_conn, r, item, s, lowest_sell, from_stations)
    return r

def item_order_key(conn: sqlite3.Connection, s: trade_lib.ItemSummary):
    info = lib.get_type_info(conn.cursor(), s.ID)
    return (info.CategoryName, info.GroupName, s.Name)

def main():
    logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
    arg_parser = ArgumentParser(prog='market_filler.py')
    arg_parser.add_argument('--orderset', type=str)
    arg_parser.add_argument('--limit-top-traded-items', type=int)
    arg_parser.add_argument('--top-traded-items', type=str)
    arg_parser.add_argument('--station', type=str)
    arg_parser.add_argument('--stock_fraction', type=float, default=0.02)
    arg_parser.add_argument('--sources', type=str)
    arg_parser.add_argument('--assets', nargs='*', type=str)
    arg_parser.add_argument('--orders', nargs='*', type=str)
    arg_parser.add_argument('--exclude_market_paths', type=str)
    arg_parser.add_argument('--exclude_industry', type=str)
    args = arg_parser.parse_args()

    sde_conn = sqlite3.connect("sde.db")
    prices_conn = sqlite3.connect("market-prices.db")
    industry_conn = sqlite3.connect("industry.db")
    excluded_mpaths = read_market_paths(args.exclude_market_paths) if args.exclude_market_paths is not None else []

    to_station, from_stations = get_sources(sde_conn, args.station, args.sources)
    logging.info("assessing market needs for {}".format(to_station))
    logging.info("source stations {}".format(','.join([str(x) for x in from_stations.keys()])))
    with open(args.top_traded_items, "rt") as tt_fh:
        items = {}
        for s in trade_lib.get_most_traded_items(tt_fh, args.limit_top_traded_items):
            excluded = False
            for x in excluded_mpaths:
                if fnmatch(s.MarketGroup, x):
                    excluded = True
                    break
            if excluded: continue
            items[s.ID] = s 
        log.info("Basket of items loaded, {} items".format(len(items)))

    oinfo = get_orderset_info(args.orderset)
    log.info("orderset {}: #{}, {}".format(args.orderset, oinfo.Orderset, oinfo.Date))
    market_model = {
            i: pick_prices(prices_conn, item, oinfo.Date) for i, item in items.items()}
    market_model = {i: m for i, m in market_model.items() if m is not None}

    industry_items = industry.read_items(sde_conn, prices_conn, industry_conn, args.exclude_industry, oinfo.Date)
    assets = read_assets(args.assets) if args.assets else {}
    orders = read_orders(to_station, args.orders) if args.orders else {}

    all_stations = set(from_stations.keys())
    all_stations.add(to_station)
    item_stocks, lowest_sell = process_orderset(args.orderset, market_model, all_stations)

    trade_suggestions = [
            decide_actions(sde_conn, to_station, market_model[i], s, lowest_sell[i], from_stations, assets.get(i, 0), orders.get(i), industry_items, args.stock_fraction)
            for i, s in item_stocks.items() if i in lowest_sell]

    w = csv.writer(sys.stdout)
    w.writerow(["TypeID", "Item Name", "Buy Quantity", "Max Buy", "My Quantity", "Sell Quantity", "My Sell Price", "Stock Quantity", "From StationIDs", "From Station Names", "IndustryCost", "Build Quantity", "Adjust Order?", "Notes"])
    for s in sorted(trade_suggestions, key=lambda x: item_order_key(sde_conn, market_model[x[0]].trade)):
        w.writerow([s.ID, s.Name, s.BuyQuantity, '{:.2f}'.format(s.MaxBuy), s.MyAssets + s.MyCurrentSell, s.SellQuantity, "{:.2f}".format(s.MySell), s.StockQuantity, s.FromStationID, s.FromStationName, "{:.2f}".format(s.IndustryCost) if s.IndustryCost else '', s.BuildQuantity, s.AdjustOrder, ",".join(s.Notes)])


if __name__ == "__main__":
    main()
