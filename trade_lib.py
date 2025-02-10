from collections import namedtuple
from functools import cache
from typing import IO, Iterator, List, Optional
import csv

ItemSummary = namedtuple('ItemSummary', ['ID', 'Name', 'GroupID', 'CategoryID', 'MarketGroup', 'ValueTraded'])
OrderSizeRule = namedtuple('OrderSizeRule', ['Prefix', 'NormalMarketSize', 'MinOrderSize'])

def parse_float(x: str) -> Optional[float]:
    if x == '-':
        return None
    return float(x)

def get_most_traded_items(fh: IO, max_items: Optional[int]) -> Iterator[ItemSummary]:
    r = csv.DictReader(fh, delimiter=',')
    count = 0
    for row in r:
        try:
            yield ItemSummary(ID=int(row['ID']), Name=row['Name'], GroupID=int(row['GroupID']), CategoryID=int(row['CategoryID']), MarketGroup=row['MarketGroup'], ValueTraded=float(row['Value Traded']))
        except (ValueError, KeyError) as e:
            raise RuntimeError("Failed to parse line {}: {}".format(row, e))
        count += 1
        if max_items and count >= max_items: return

@cache
def _get_min_order_rules() -> List[OrderSizeRule]:
    rules = []
    with open("order-sizes.txt", "rt") as f:
        r = csv.reader(f, delimiter="\t")
        for row in r:
            try:
                rules.append(OrderSizeRule(Prefix=row[0], NormalMarketSize=int(row[1]), MinOrderSize=int(row[2])))
            except (ValueError, KeyError, IndexError) as e:
                raise RuntimeError("Failed to parse line {}: {}".format(row, e))
    return rules

def get_order_size(i: ItemSummary) -> (int, int):
    rules = _get_min_order_rules()

    for r in rules:
        if i.MarketGroup.startswith(r.Prefix):
            return r
    raise RuntimeError("No rule matches {} ({})", i.Name, i.MarketGroup) 
