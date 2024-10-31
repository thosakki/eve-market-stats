#!/usr/bin/python3

from argparse import ArgumentParser
import csv
import lib
import locale
import logging
import sqlite3

logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)

sde = sqlite3.connect("sde.db")
industryDB = sqlite3.connect("industry.db")
locale.setlocale(locale.LC_NUMERIC, 'en_GB.UTF-8')

seen = set()

res = industryDB.execute("""
    SELECT Name
    FROM BuildItems
""")
for r in res.fetchall():
    print(r[0])
    seen.add(r[0])

res = industryDB.execute("""
    SELECT ID
    FROM BuildItemInputs
""")
for r in res.fetchall():
    inputInfo = lib.get_type_info(sde, r[0])
    if inputInfo.Name in seen: continue
    print(inputInfo.Name)
    seen.add(inputInfo.Name)
    assert inputInfo.Name in seen
