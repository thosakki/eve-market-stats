#!/usr/bin/python3

import datetime
import fileinput
import logging
import sqlite3

import lib
from price_lib import get_pricing
from industry import get_reprocess_value

log = logging.getLogger(__name__)

def main():
    logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
    sde_conn = sqlite3.connect("sde.db")
    prices_conn = sqlite3.connect("market-prices.db")

    for x in fileinput.input():
        x = x.rstrip()
        try:
            ti = lib.get_type_info(sde_conn, int(x))
        except ValueError:
            ti = lib.get_type_info_byname(sde_conn, x)
        v = get_reprocess_value(sde_conn, prices_conn, ti.ID, datetime.date.today())
        print(v)

if __name__ == "__main__":
    main()
