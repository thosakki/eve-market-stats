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

import lib

logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)
arg_parser = ArgumentParser(prog='top-market-items')
arg_parser.add_argument('--orderset', type=str)
arg_parser.add_argument('--include_group', nargs='*', type=int)
arg_parser.add_argument('--exclude_group', nargs='*', type=int)
arg_parser.add_argument('--include_category', nargs='*', type=int)
arg_parser.add_argument('--exclude_category', nargs='*', type=int)
arg_parser.add_argument('--popular', nargs='*', type=str)
args = arg_parser.parse_args()

items = {}
TypeInfo = lib.TypeInfo

con = sqlite3.connect("sde.db")
cur = con.cursor()

months = len(args.popular)
for name in args.popular:
  log.info('Reading trade volumes {}...'.format(name))
  with open(name) as market_data_csv:
    reader = csv.DictReader(market_data_csv)
    for r in reader:
        t = r['Commodity']

        ti = lib.get_type_info_byname(con, t)
        if ti is None:
            #log.warning('Unknown type {}'.format(t))
            continue
        if args.include_group is not None and ti.GroupID not in args.include_group: continue
        if args.exclude_group is not None and ti.GroupID in args.exclude_group: continue
        if args.include_category is not None and ti.CategoryID not in args.include_category: continue
        if args.exclude_category is not None and ti.CategoryID in args.exclude_category: continue

        value_traded = int(r['Value of trades'].replace('.', ''))
        traded_items = int(r['Traded items'].replace('.', ''))
        if ti.ID not in items:
            items[ti.ID] = { 'ID': ti.ID, 'Name': ti.Name, 'CategoryID': ti.CategoryID, 'GroupID': ti.GroupID, 'MarketGroup': ti.MarketGroup, 'by_month': []}
        items[ti.ID]['by_month'].append({'num': traded_items, 'value': value_traded, 'score': value_traded / math.pow(value_traded/traded_items, 0.6)})
  log.info('...read trade volumes {}'.format(name))

for _, r in items.items():
    # Unweighted averages.
    r['num'] = sum(x['num'] for x in r['by_month'])/months
    r['value'] = sum(x['value'] for x in r['by_month'])/months

    if len(r['by_month']) < months:
        # Anything seasonal, I don't want.
        r['score'] = 0
    else:
        # Lowest score across all months considered - we're looking for consistently heavily traded items.
        r['score'] = min(x['value']/math.pow(x['value']/x['num'], 0.6) for x in r['by_month'])

def parse_agg_what(s: str) -> (int, int, bool):
    region,  type_id, is_buy = s.split('|')
    return (int(region), int(type_id), is_buy=='true')

log.info('Reading buy & sell prices')
for o, _ in lib.read_orderset(args.orderset):
    # Jita 4-4, Dodixie FNAP, Amarr EFA
    if o.StationID not in (60003760, 60011866, 60008494): continue
    if o.TypeID not in items: continue
    if o.IsBuy:
        if items[o.TypeID].get('buy',0) < o.Price:
            items[o.TypeID]['buy'] = o.Price
    else:
        if 'sell' not in items[o.TypeID] or items[o.TypeID]['sell'] > o.Price:
            items[o.TypeID]['sell'] = o.Price
log.info('...read buy & sell prices')

log.info('Producing output...')
count = 0

w = csv.writer(sys.stdout, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
w.writerow(['ID', 'Name', 'GroupID', 'CategoryID', 'MarketGroup', 'Value Traded', 'Buy', 'Sell'])
for id, d in sorted(items.items(), key=lambda x: x[1]['score'], reverse=True):
    w.writerow([d['ID'], d['Name'], d['GroupID'], d['CategoryID'], d['MarketGroup'], d['value'], d.get('buy', '-'), d.get('sell', '-')])
    count += 1
    if count > 10000:
        break
log.info('...done')
