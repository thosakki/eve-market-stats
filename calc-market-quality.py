#!/usr/bin/python3

from argparse import ArgumentParser
from collections import namedtuple
import csv
import logging
from typing import List, Optional
import sqlite3
import sys

import lib

logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)

ItemSummary = namedtuple('ItemSummary', ['ID', 'Name', 'GroupID', 'CategoryID', 'ValueTraded', 'Buy', 'Sell'])
items = dict()

arg_parser = ArgumentParser(prog='calc-market-quality.py')
arg_parser.add_argument('--orderset', type=str)
args = arg_parser.parse_args()

con = sqlite3.connect("sde.db")
cur = con.cursor()

def mean(d: List[float]) -> float:
    return(sum(d)/len(d))

def parse_float(x: str) -> Optional[float]:
    if x == '-':
        return None
    return float(x)

# Read in most traded items.
with open("top-traded.csv", "rt") as fh:
    r = csv.DictReader(fh, delimiter=',')
    for row in r:
        try:
            items[int(row['ID'])] = ItemSummary(ID=int(row['ID']), Name=row['Name'], GroupID=int(row['GroupID']), CategoryID=int(row['CategoryID']), ValueTraded=float(row['Value Traded']), Buy=parse_float(row['Buy']), Sell=parse_float(row['Sell']))
        except ValueError as e:
            raise RuntimeError("Failed to parse line {}: {}".format(row, e))

def emit_station_stats(w, stationID: int, efficiencies: List[float]):
    coverage = len(efficiencies) / len(items)
    log.info('efficiencies: {}'.format([(i, x) for i, x in efficiencies if x > 2]))
    if len(efficiencies)>0:
        mean_efficiency = mean([e for i, e in efficiencies])
        eff_str = '{:.1f}%'.format((mean_efficiency-1)*100)
    else:
        eff_str = '-'
    station_info = lib.get_station_info(cur, stationID)
    if station_info is None:
        log.info("Could not find station {}".format(stationID))
    w.writerow([str(stationID), station_info.Name if station_info is not None else "-", '{:.1f}%'.format(coverage*100), eff_str])


w = csv.writer(sys.stdout)
w.writerow(['StationID', 'Station Name', 'Coverage %', 'Inefficiency %'])
current_station = None
current_type = None
current_station_price_efficiencies = None
current_type_best_sell = None
log.info("orderset file '{}'".format(args.orderset))
for x in lib.read_orderset(args.orderset):
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
                current_station_price_efficiencies.append((current_type, efficiency))
        current_type_best_sell = None
        current_type = None

    if current_station is None or current_station != x.StationID:
        if current_station is not None:
            emit_station_stats(w, current_station, current_station_price_efficiencies)
        current_station_price_efficiencies = []
        current_station = x.StationID

    if x.TypeID in items and not x.IsBuy:
        current_type = x.TypeID
        if current_type_best_sell is None or current_type_best_sell > x.Price:
            current_type_best_sell = x.Price

# read_orderset() always pads the end with one bogus order line with a dummy item & station,
# so we never have to emit the final item or station returned.

