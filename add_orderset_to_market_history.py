#!/usr/bin/python3

from argparse import ArgumentParser
import csv
from dataclasses import dataclass
import datetime
import gzip
import logging
import sqlite3
from typing import IO, Iterator, Set, Tuple

import lib

logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)

# Jita 4-4, Dodixie FNAP, Amarr EFA
BEST_STATIONS = (60003760, 60011866, 60008494)

@dataclass
class ItemMarket():
    TypeID: int
    StationID: int
    Buy: float
    Sell: float
    SellVolume: int

def load(orderset_fname: str, stations: Set[int]) -> Iterator[ItemMarket]:
    current_station = None
    current_item = None
    current_buy = None
    current_sell = None
    current_sell_vol = None

    for o, _ in lib.read_orderset(orderset_fname):
        if ((current_station is not None and o.StationID != current_station) or
                (current_item is not None and o.TypeID != current_item)):
            yield ItemMarket(TypeID=current_item, StationID=current_station, Buy=current_buy, Sell=current_sell, SellVolume=current_sell_vol)
            current_buy = current_sell = current_sell_vol = None

        if o.StationID not in stations: continue
        current_station = o.StationID
        current_item = o.TypeID

        if o.IsBuy:
            if current_buy is None or current_buy < o.Price:
                current_buy = o.Price
        else:
            if current_sell is None or current_sell > o.Price:
                if current_sell is None or current_sell > o.Price*1.01: current_sell_vol = 0
                current_sell = o.Price
                current_sell_vol += o.Volume

def emit_item(conn: sqlite3.Connection, date: datetime.date, im: ItemMarket):
    conn.execute("""
    INSERT INTO PriceHistory VALUES(?,?,?,?,?,?)
    """, [im.TypeID, date.isoformat(), im.StationID, im.Buy, im.Sell, im.SellVolume])

def init_db(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE PriceHistory(
    TypeID INTEGER,
    Date DATE,
    StationID INTEGER,
    Buy FLOAT,
    Sell FLOAT,
    SellVolume INTEGER
    );""")
    conn.execute("""
    CREATE INDEX PriceHistory_ByItemDate ON PriceHistory(TypeID, Date);
    """)


def read_filter_file(fh: IO) -> Set[int]:
    """Returns the IDs from the given CSV."""
    x = set()
    r = csv.DictReader(fh)
    for row in r:
        x.add(int(row["ID"]))
    return x


def main():
    arg_parser = ArgumentParser(prog='add_orderset_to_market_history')
    arg_parser.add_argument('--initial', action='store_true')
    arg_parser.add_argument('--orderset', type=str)
    arg_parser.add_argument('--filter_items', type=str)
    arg_parser.add_argument('--extra_stations', nargs='*', type=int)
    args = arg_parser.parse_args()

    conn = sqlite3.connect("market-prices.db")
    if args.initial: init_db(conn)
    stations = set(BEST_STATIONS)
    stations.update(args.extra_stations)
    log.info("Querying for stations {}".format(stations))

    oinfo = lib.OrdersetInfo(None, None)
    for o in lib.read_orderset_filter(args.orderset, oinfo):
        pass
    log.info("Orderset identified as {}, {}".format(oinfo.Orderset, oinfo.Date.date().isoformat()))

    filter_items = None
    if args.filter_items:
        with open(args.filter_items) as fh:
            filter_items = read_filter_file(fh)
        log.info("Filtering based on {}: {} items selected".format(args.filter_items, len(filter_items)))

    count = 0
    skipped = 0
    for im in load(args.orderset, stations):
        if filter_items is not None and im.TypeID not in filter_items:
            skipped += 1
            continue
        emit_item(conn, oinfo.Date.date(), im)
        count += 1
        if count % 1000 == 0: conn.commit()
    conn.commit()
    log.info("Wrote {} items, skipped {} items".format(count, skipped))


if __name__ == "__main__":
    main()
