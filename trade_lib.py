from collections import namedtuple
from typing import IO, Iterator, Optional
import csv

ItemSummary = namedtuple('ItemSummary', ['ID', 'Name', 'GroupID', 'CategoryID', 'MarketGroup', 'ValueTraded', 'Buy', 'Sell'])

def parse_float(x: str) -> Optional[float]:
    if x == '-':
        return None
    return float(x)

def get_most_traded_items(fh: IO, max_items: Optional[int]) -> Iterator[ItemSummary]:
    r = csv.DictReader(fh, delimiter=',')
    count = 0
    for row in r:
        try:
            yield ItemSummary(ID=int(row['ID']), Name=row['Name'], GroupID=int(row['GroupID']), CategoryID=int(row['CategoryID']), MarketGroup=row['MarketGroup'], ValueTraded=float(row['Value Traded']), Buy=parse_float(row['Buy']), Sell=parse_float(row['Sell']))
        except (ValueError, KeyError) as e:
            raise RuntimeError("Failed to parse line {}: {}".format(row, e))
        count += 1
        if max_items and count >= max_items: return

