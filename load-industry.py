#!/usr/bin/python3

from argparse import ArgumentParser
import csv
import lib
import locale
import logging
import sqlite3

logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)

arg_parser = ArgumentParser(prog='load-industry')
arg_parser.add_argument('--industry', type=str)
arg_parser.add_argument('--initial', action='store_true')
args = arg_parser.parse_args()

sde = sqlite3.connect("sde.db")
industryDB = sqlite3.connect("industry.db")
locale.setlocale(locale.LC_NUMERIC, 'en_GB.UTF-8')

if args.initial:
    industryDB.execute("""
    CREATE TABLE BuildItems(
      ID  INT PRIMARY KEY NOT NULL,
      Name TEXT NOT NULL,
      QuantityBuilt INT
    );
    """)
    industryDB.execute("""
    CREATE TABLE BuildItemInputs(
      ID INT NOT NULL,
      OutputID INT NOT NULL,
      QuantityRequired INT
    );
    """)
    industryDB.execute("""
    CREATE UNIQUE INDEX BuildItemInputs_Key ON BuildItemInputs(OutputID, ID);
    """)

with open(args.industry) as fh:
    cur = industryDB.cursor()
    reader = csv.DictReader(fh)
    for r in reader:
        outputName = r["Thing to make"]
        outputInfo = lib.get_type_info_byname(sde, outputName)
        if outputInfo is None:
            log.warn("output {} not recognised".format(outputName))
            continue
        inputName = r["Input"]
        inputInfo = lib.get_type_info_byname(sde, inputName)
        if inputInfo is None:
            log.warn("input {} not recognised".format(inputName))
            continue
        cur.execute("""INSERT OR REPLACE INTO BuildItems VALUES(?,?,?)""",[outputInfo.ID, outputInfo.Name, locale.atoi(r["Quantity made"])])
        cur.execute("""INSERT OR REPLACE INTO BuildItemInputs VALUES(?,?,?)""",[inputInfo.ID, outputInfo.ID, locale.atof(r["Quantity for N"])])
    industryDB.commit()
