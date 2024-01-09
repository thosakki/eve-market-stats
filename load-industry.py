#!/usr/bin/python3

from argparse import ArgumentParser
import csv
import lib
import sqlite3
import logging

logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)

arg_parser = ArgumentParser(prog='load-industry')
arg_parser.add_argument('--industry', type=str)
arg_parser.add_argument('--initial', action='store_true')
args = arg_parser.parse_args()

sde = sqlite3.connect("sde.db")
industryDB = sqlite3.connect("industry.db")

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

def canonical_name(x: str) -> str:
    parts = x.split(" ")
    new_parts = []
    for p in parts:
        if p.upper() == "II":
            new_parts.append("II")
        elif p.lower() == "autocannon":
            new_parts.append("AutoCannon")
        elif p.lower() == "em":
            new_parts.append("EM")
        elif not p[0].isdigit():
            new_parts.append(p.title())
        elif p.endswith("mn"):
            new_parts.append(p.upper())
        elif p.endswith("mm"):
            new_parts.append(p.lower())
        else:
            log.warning("no idea how to handle {}".format(p))
            new_parts.append(p)
    return " ".join(new_parts)

with open(args.industry) as fh:
    cur = industryDB.cursor()
    reader = csv.DictReader(fh)
    for r in reader:
        outputName = canonical_name(r["Thing to make"])
        outputInfo = lib.get_type_info_byname(sde, outputName)
        if outputInfo is None:
            log.warn("output {} not recognised".format(outputName))
            continue
        inputName = canonical_name(r["Input"])
        inputInfo = lib.get_type_info_byname(sde, inputName)
        if inputInfo is None:
            log.warn("input {} not recognised".format(inputName))
            continue
        cur.execute("""INSERT OR REPLACE INTO BuildItems VALUES(?,?,?)""",[outputInfo.ID, outputInfo.Name, r["Quantity made"]])
        cur.execute("""INSERT OR REPLACE INTO BuildItemInputs VALUES(?,?,?)""",[inputInfo.ID, outputInfo.ID, r["Quantity for N"]])
    industryDB.commit()
