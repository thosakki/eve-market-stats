import datetime
import logging
import math
import sqlite3
from typing import Dict, Optional

import lib
from price_lib import get_pricing

log = logging.getLogger(__name__)

def _get_buildable_items(sde_conn: sqlite3.Connection, industry_conn: sqlite3.Connection, exclude_industry: str):
    exclude = set()
    with open(exclude_industry, "rt") as f:
        for line in f:
            item = lib.get_type_info_byname(sde_conn, line.rstrip())
            exclude.add(item.ID)

    res = {}
    r = industry_conn.execute("""
            SELECT ID,Name,QuantityBuilt FROM BuildItems;
            """)
    for x in r.fetchall():
        type_id = x[0]
        item = lib.get_type_info(sde_conn.cursor(), type_id)
        if item is None:
            log.warning("unrecognised item {}".format(x[1]))
        if item.ID not in exclude:
            res[item.ID] = {'Name': item.Name, 'QuantityBuilt': x[2]}
    return res

def _get_item_build_costs(items: Dict[int, dict], prices_conn: sqlite3.Connection, industry_conn: sqlite3.Connection, date):
    # Greedy algorithm here - we do a pass over the data structure filling in
    # costs for everything that we know all the input costs for. Then we
    # repeat, filling in on later passes items that can now be built from items
    # that we just filled in the costs for.
    done = True
    while True:
        for bi in items.keys():
            if 'BuildCost' in items[bi]: continue

            inputs = industry_conn.execute("""
                SELECT ID,QuantityRequired FROM BuildItemInputs WHERE OutputID=?;
                """, [bi])
            build_cost = 0
            for i in inputs:
                price = None
                if i[0] in items:
                  if 'BuildCost' in items[i[0]]:
                    price = items[i[0]]['BuildCost']
                  else:
                    done = False
                    break
                else:
                  p = get_pricing(prices_conn, i[0], date)
                  if p.fair_price is None:
                    input_type = lib.get_type_info(sde_conn.cursor(), i[0])
                    log.warning("No fair price for {}: {} {}".format(input_type.Name, i[0], date))
                    break
                  price = p.fair_price
                build_cost += price * i[1]
            else:
                logging.info('setting build cost for {}, currently {}'.format(bi, items.get(bi, None)))
                items[bi]['BuildCost'] = build_cost / items[bi]['QuantityBuilt']

        if done:
            return
        done = True

def get_reprocess_value(sde_conn: sqlite3.Connection, prices_conn: sqlite3.Connection, type_id: int, date: datetime.date) -> Optional[float]:
    outputs = sde_conn.execute("""
        SELECT OutputID,QuantityYielded FROM ReprocessItems WHERE ID=?;
        """, [type_id])
    reprocess_value = 0.0
    for o in outputs:
        price = None
        p = get_pricing(prices_conn, o[0], date)
        if p.fair_price is None:
          input_type = lib.get_type_info(sde_conn.cursor(), o[0])
          log.warning("No fair price for {}: {} {}".format(input_type.Name, o[0], date))
          return None
        reprocess_value += p.fair_price * math.floor(o[1] * 0.5)
    return reprocess_value if reprocess_value > 0.0 else None

def read_items(sde_conn: sqlite3.Connection, prices_conn: sqlite3.Connection, industry_conn: sqlite3.Connection, exclude_industry: str, date) -> Dict[int, float]:
    items = _get_buildable_items(sde_conn, industry_conn, exclude_industry)
    log.info("Loaded list of {} buildable items".format(len(items)))

    _get_item_build_costs(items, prices_conn, industry_conn, date)
    log.info("Computed build costs for {} items".format(len(items)))

    return {x: items[x]['BuildCost'] for x in items if 'BuildCost' in items[x]}

