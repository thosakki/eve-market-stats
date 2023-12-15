#!/usr/bin/python3

from argparse import ArgumentParser
from collections import namedtuple
import csv
import datetime
import logging
import tempfile
from typing import Dict, IO, Iterator, List, NamedTuple, Optional, Tuple
import sqlite3
import sys

import lib

logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)

ItemSummary = namedtuple('ItemSummary', ['ID', 'Name', 'GroupID', 'CategoryID', 'ValueTraded', 'Buy', 'Sell'])

def weighted_mean(d: List[Tuple[float, float]]) -> float:
    return(sum(w*v for w,v in d)/sum(w for w,_ in d))

def parse_float(x: str) -> Optional[float]:
    if x == '-':
        return None
    return float(x)

def get_most_traded_items(fh: IO) -> Iterator[ItemSummary]:
    r = csv.DictReader(fh, delimiter=',')
    for row in r:
        try:
            yield ItemSummary(ID=int(row['ID']), Name=row['Name'], GroupID=int(row['GroupID']), CategoryID=int(row['CategoryID']), ValueTraded=float(row['Value Traded']), Buy=parse_float(row['Buy']), Sell=parse_float(row['Sell']))
        except (ValueError, KeyError) as e:
            raise RuntimeError("Failed to parse line {}: {}".format(row, e))

def emit_station_stats(w, stationID: int, efficiencies: List[Tuple[int, float, float]], c: sqlite3.Cursor, items: Dict[int, ItemSummary], dump_detail_for: int):
    coverage = len(efficiencies) / len(items)

    if dump_detail_for == stationID:
        log.info("Dumping for station {}".format(stationID))
        with open("dump.csv", "wt") as f:
            d = csv.writer(f)
            d.writerow(['TypeID', 'Name', 'Value Traded (universal)', 'Efficiency'])
            for i, v, e in efficiencies:
                ti = lib.get_type_info(c, i)
                d.writerow([i, ti.Name, v, e])

    if len(efficiencies)>0:
        mean_efficiency = weighted_mean([(w,e) for i, w, e in efficiencies])
        eff_str = '{:.1f}'.format((mean_efficiency-1)*100)
    else:
        eff_str = '-'
    station_info = lib.get_station_info(c, stationID)
    if station_info is None:
        log.info("Could not find station {}".format(stationID))
    w.writerow([str(stationID), station_info.Name if station_info is not None else "-", '{:.1f}'.format(coverage*100), eff_str])


def get_station_stats(ofile: str, items: Dict[int, ItemSummary], oinfo: lib.OrdersetInfo) -> Iterator[Tuple[int, List[Tuple[int, float, float]]]]:
    current_station = None
    current_type = None
    current_station_price_efficiencies = None
    current_type_best_sell = None
    log.info("orderset file '{}'".format(ofile))
    for x in lib.read_orderset_filter(ofile, oinfo):

        if current_type is not None and current_type != x.TypeID:
            if current_type_best_sell is not None:
                all_best_sell = items[current_type].Sell
                if all_best_sell is None:
                    log.info("no best sell price for common item? {}".format(current_type))
                    efficiency = 1.0
                else:
                    efficiency = current_type_best_sell / all_best_sell

                # Treat stupidly overpriced stuff as unavailable.
                if efficiency <= 100:
                    current_station_price_efficiencies.append((current_type, items[current_type].ValueTraded, efficiency))
            current_type_best_sell = None
            current_type = None

        if current_station is None or current_station != x.StationID:
            if current_station is not None:
                yield(current_station, current_station_price_efficiencies)
            current_station_price_efficiencies = []
            current_station = x.StationID

        if x.TypeID in items and not x.IsBuy:
            current_type = x.TypeID
            if current_type_best_sell is None or current_type_best_sell > x.Price:
                current_type_best_sell = x.Price
    # read_orderset() always pads the end with one bogus order line with a dummy item & station,
    # so we never have to emit the final item or station returned.

def output(csv_fh: IO, oinfo: lib.OrdersetInfo):
    r = csv.DictReader(csv_fh)
    fieldnames = r.fieldnames
    fieldnames.extend(['Orderset', 'Date'])
    w = csv.DictWriter(sys.stdout, fieldnames)
    w.writeheader()
    for row in r:
        # Add global variables now to every row.
        row['Orderset'] = oinfo.Orderset
        row['Date'] = oinfo.Date.date().isoformat()
        w.writerow(row)

def main():
    arg_parser = ArgumentParser(prog='calc-market-quality.py')
    arg_parser.add_argument('--orderset', type=str)
    arg_parser.add_argument('--dump-detail-for', type=int)
    args = arg_parser.parse_args()

    conn = sqlite3.connect("sde.db")
    c = conn.cursor()

    with open("top-traded.csv","rt") as tt_fh:
        items = {s.ID: s for s in get_most_traded_items(tt_fh)}

    temp_station_stats = tempfile.TemporaryFile(mode='w+t')
    w = csv.writer(temp_station_stats)
    w.writerow(['StationID', 'Station Name', 'Coverage %', 'Inefficiency %'])

    oinfo = lib.OrdersetInfo(None, None)
    for s, e in get_station_stats(args.orderset, items, oinfo):
        emit_station_stats(w, s, e, c, items, args.dump_detail_for)

    temp_station_stats.seek(0)
    output(temp_station_stats, oinfo)

if __name__ == "__main__":
    main()
