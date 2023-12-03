#!/usr/bin/python3

from argparse import ArgumentParser
from collections import defaultdict, namedtuple
import csv
import gzip
import logging
import math
import operator
import sqlite3
import sys
import yaml

logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)
arg_parser = ArgumentParser(prog='top-market-items')
arg_parser.add_argument('--include_group', nargs='*', type=int)
arg_parser.add_argument('--exclude_group', nargs='*', type=int)
arg_parser.add_argument('--include_category', nargs='*', type=int)
arg_parser.add_argument('--exclude_category', nargs='*', type=int)
args = arg_parser.parse_args()

items = {}
TypeInfo = namedtuple('TypeInfo', ['ID', 'Name', 'GroupID', 'GroupName', 'CategoryID', 'CategoryName'])

con = sqlite3.connect("sde.db")
cur = con.cursor()

def get_type_info(cur: sqlite3.Cursor, name: str) -> TypeInfo:
    res = cur.execute("""
    SELECT Types.ID, Types.name, Groups.ID, Groups.Name, Categories.ID, Categories.Name
    FROM Types JOIN Groups ON (Types.GroupID = Groups.ID)
        JOIN Categories ON (Categories.ID = Groups.CategoryID)
    WHERE Types.Name = ?
    """, [name])
    r = res.fetchall()
    if len(r) == 0:
        log.info("Could not find type '{}'".format(name))
        return None
    if len(r) > 1:
        raise RuntimeError("multiple values for a name from SDE")
    row = r[0]
    return TypeInfo(row[0], row[1], row[2], row[3], row[4], row[5])

log.info('Reading trade volumes...')
with open('popular.csv') as market_data_csv:
    reader = csv.DictReader(market_data_csv)
    for r in reader:
        t = r['Commodity']

        ti = get_type_info(con, t)
        if ti is None:
            log.warning('Unknown type {}'.format(t))
            continue
        if args.include_group is not None and ti.GroupID not in args.include_group: continue
        if args.exclude_group is not None and ti.GroupID in args.exclude_group: continue
        if args.include_category is not None and ti.CategoryID not in args.include_category: continue
        if args.exclude_category is not None and ti.CategoryID in args.exclude_category: continue

        value_traded = int(r['Value of trades'].replace('.', ''))
        traded_items = int(r['Traded items'].replace('.', ''))
        items[ti.ID] = { 'ID': ti.ID, 'Name': ti.Name, 'CategoryID': ti.CategoryID, 'GroupID': ti.GroupID, 'num': traded_items, 'value': value_traded, 'score': value_traded / math.pow(value_traded/traded_items, 0.6)}
log.info('...read trade volumes')

def parse_agg_what(s: str) -> (int, int, bool):
    region,  type_id, is_buy = s.split('|')
    return (int(region), int(type_id), is_buy=='true')

log.info('Reading buy & sell prices')
with gzip.open('aggregatecsv.csv.gz', 'rt') as agg_fh:
    #  what,weightedaverage,maxval,minval,stddev,median,volume,numorders,fivepercent,orderSet
    reader = csv.DictReader(agg_fh)
    for r in reader:
        region, type_id, is_buy = parse_agg_what(r['what'])
        if type_id not in items: continue

        # The Forge, Sinq Liason, Domain
        if region not in (10000002, 10000032, 10000043): continue
        if is_buy:
            best_price = float(r['maxval'])
            if 'buy' not in items[type_id] or items[type_id]['buy'] < best_price:
                items[type_id]['buy'] = best_price
        else:
            best_price = float(r['minval'])
            if 'sell' not in items[type_id] or items[type_id]['sell'] > best_price:
                items[type_id]['sell'] = best_price
log.info('...read buy & sell prices')

log.info('Producing outout...')
count = 0

w = csv.writer(sys.stdout, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
w.writerow(['ID', 'Name', 'GroupID', 'CategoryID', 'Value Traded', 'Buy', 'Sell'])
for id, d in sorted(items.items(), key=lambda x: x[1]['score'], reverse=True):
    w.writerow([d['ID'], d['Name'], d['GroupID'], d['CategoryID'], d['value'], d.get('buy', '-'), d.get('sell', '-')])
    count += 1
    if count > 10000:
        break
log.info('...done')
