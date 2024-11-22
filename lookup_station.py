#!/usr/bin/python3

import fileinput
import lib
import sqlite3

sde_conn = sqlite3.connect("sde.db")

for x in fileinput.input():
    x = x.rstrip()
    try:
        ti = lib.get_station_info(sde_conn, int(x))
    except ValueError:
        ti = lib.get_station_info_byname(sde_conn, x)
    print(ti)

