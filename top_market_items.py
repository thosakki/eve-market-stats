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
import trade_lib

logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)
arg_parser = ArgumentParser(prog='top-market-items')
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

        if ti.MarketGroup is None:
            log.debug("non-market item {}".format(t))
            continue

        value_traded = int(r['Value of trades'].replace('.', ''))
        traded_items = int(r['Traded items'].replace('.', ''))
        if ti.ID not in items:
            items[ti.ID] = { 'ID': ti.ID, 'Name': ti.Name, 'CategoryID': ti.CategoryID, 'GroupID': ti.GroupID, 'MarketGroup': ti.MarketGroup, 'NormalMarketSize': trade_lib.get_order_size(ti).NormalMarketSize, 'by_month': []}
        items[ti.ID]['by_month'].append({'num': traded_items, 'value': value_traded})
  log.info('...read trade volumes {}'.format(name))

for _, r in items.items():
    r['num'] = min(x['num'] for x in r['by_month'])
    r['value'] = min(x['value'] for x in r['by_month'])

    if len(r['by_month']) < months:
        # Anything seasonal, I don't want.
        r['score'] = 0
    else:
        value_per_item = r['value']/r['num']
        value_per_trade = value_per_item * r['NormalMarketSize']
        # Lowest score across all months considered - we're looking for consistently heavily traded items.
        r['score'] = min(x['value']/math.pow(value_per_trade, 0.6) for x in r['by_month'])

def parse_agg_what(s: str) -> (int, int, bool):
    region,  type_id, is_buy = s.split('|')
    return (int(region), int(type_id), is_buy=='true')

log.info('Producing output...')
count = 0

w = csv.writer(sys.stdout, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
w.writerow(['ID', 'Name', 'GroupID', 'CategoryID', 'MarketGroup', 'Value Traded'])
for id, d in sorted(items.items(), key=lambda x: x[1]['score'], reverse=True):
    w.writerow([d['ID'], d['Name'], d['GroupID'], d['CategoryID'], d['MarketGroup'], d['value']])
    count += 1
    if count > 10000:
        break
log.info('...done')
