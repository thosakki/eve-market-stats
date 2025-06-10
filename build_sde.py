#!/usr/bin/python3

from argparse import ArgumentParser
from collections import defaultdict, namedtuple
import csv
import logging
import math
import operator
from pathlib import Path
import sqlite3
import yaml

logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)
arg_parser = ArgumentParser(prog='build-sde.py')
arg_parser.add_argument('--initial', action='store_true')
arg_parser.add_argument('--skip_types', action='store_true')
arg_parser.add_argument('--skip_systems', action='store_true')
args = arg_parser.parse_args()

def build_categories(cur):
    if args.initial:
        cur.execute("""
        CREATE TABLE Categories(
          ID      INT PRIMARY KEY NOT NULL,
          Name    TEXT NOT NULL
        );""")
        cur.execute("""
        CREATE UNIQUE INDEX Categories_ByName ON Categories(Name);
        """)

    with open("sde/fsd/categories.yaml", "rt") as fh:
        loader = yaml.SafeLoader(fh)

        # check proper stream start (should never fail)
        assert loader.check_event(yaml.StreamStartEvent)
        loader.get_event()
        assert loader.check_event(yaml.DocumentStartEvent)
        loader.get_event()

        # assume the root element is a sequence
        assert loader.check_event(yaml.MappingStartEvent)
        loader.get_event()
        added = 0
        failed = 0

        # now while the next event does not end the sequence, process each item
        while not loader.check_event(yaml.MappingEndEvent):
            # compose current item to a node as if it was the root node
            node = loader.compose_node(None, None)
            # we set deep=True for complete processing of all the node's children
            k = loader.construct_object(node, True)
            # compose current item to a node as if it was the root node
            node = loader.compose_node(None, None)
            # we set deep=True for complete processing of all the node's children
            v = loader.construct_object(node, True)

            cat_id = k
            name = v['name']['en']
            try:
                cur.execute("""INSERT OR REPLACE INTO Categories VALUES(?,?)""", [cat_id, name])
                added += 1
            except sqlite3.IntegrityError:
                log.error("failed to insert ({},{})".format(cat_id, name))
                failed += 1
        # assume document ends and no further documents are in stream
        loader.get_event()
        assert loader.check_event(yaml.DocumentEndEvent)
        loader.get_event()
        assert loader.check_event(yaml.StreamEndEvent)
        cur.commit()
        if added > 0 or failed == 0:
            log.info("Added {} categories, failed {}".format(added, failed))
        else:
            log.error("Added {} categories, failed {}".format(added, failed))

def build_market_groups(cur):
    if args.initial:
        cur.execute("""
        CREATE TABLE MarketGroups(
          ID      INT PRIMARY KEY NOT NULL,
          Path    TEXT NOT NULL
        );""")
        cur.execute("""
        CREATE UNIQUE INDEX MarketGroups_ByPath ON MarketGroups(Path);
        """)

    def get_mgroup(group_id: int):
        res = cur.execute("""
        SELECT ID, Path FROM MarketGroups
        WHERE ID = ?
        """, [group_id])
        r = res.fetchall()
        if len(r) == 0:
            return None
        assert len(r) == 1
        row = r[0]
        return row[1]

    # The marketGroups file contains forward references - so an entry may refer
    # to a parent that occurs later in the file. Greedy approach here - we just
    # repeatedly load the file, skipping entries that are broken references
    # and keep going until we have a pass with nothing skipped (or fail if we do
    # a pass with nothing added).
    skipped = 1
    added = 1
    while skipped > 0 and added > 0:
        added = 0
        skipped = 0
        with open("sde/fsd/marketGroups.yaml", "rt") as fh:
            loader = yaml.SafeLoader(fh)

            # check proper stream start (should never fail)
            assert loader.check_event(yaml.StreamStartEvent)
            loader.get_event()
            assert loader.check_event(yaml.DocumentStartEvent)
            loader.get_event()

            # assume the root element is a sequence
            assert loader.check_event(yaml.MappingStartEvent)
            loader.get_event()

            # now while the next event does not end the sequence, process each item
            while not loader.check_event(yaml.MappingEndEvent):
                # compose current item to a node as if it was the root node
                node = loader.compose_node(None, None)
                # we set deep=True for complete processing of all the node's children
                k = loader.construct_object(node, True)
                # compose current item to a node as if it was the root node
                node = loader.compose_node(None, None)
                # we set deep=True for complete processing of all the node's children
                v = loader.construct_object(node, True)

                mgroup_id = k
                name = v['nameID']['en']
                try:
                    if 'parentGroupID' in v:
                        parentPath = get_mgroup(v['parentGroupID'])
                        if parentPath is None:
                            skipped += 1
                            continue
                        path = '{}>{}'.format(parentPath,name)
                    else:
                        path = name
                    cur.execute("""INSERT OR REPLACE INTO MarketGroups VALUES(?,?)""", [mgroup_id, path])
                    added += 1
                except sqlite3.IntegrityError:
                    log.error("failed to insert ({},{})".format(mgroup_id, name, v.get('parentGroupID','-')))
            # assume document ends and no further documents are in stream
            loader.get_event()
            assert loader.check_event(yaml.DocumentEndEvent)
            loader.get_event()
            assert loader.check_event(yaml.StreamEndEvent)
            cur.commit()
            if added > 0 or skipped == 0:
                log.info("Added {} market groups, skipped {}".format(added, skipped))
            else:
                log.error("Added {} market groups, skipped {}".format(added, skipped))

# Commodity,Number of trades,Traded items,Value of trades,Lst,,as per ESI; complete New Eden; last update: 24.11.2023
# PLEX,2.974,1.078.606,4.415.528.028.140,,,

def build_groups(cur):
    if args.initial:
        cur.execute("""
        CREATE TABLE Groups(
          ID      INT PRIMARY KEY NOT NULL,
          Name    TEXT NOT NULL,
          CategoryID INT NOT NULL
        );""")
        cur.execute("""
        CREATE UNIQUE INDEX Groups_ByName ON Groups(Name);
        """)

    with open("sde/fsd/groups.yaml", "rt") as fh:
        loader = yaml.SafeLoader(fh)

        # check proper stream start (should never fail)
        assert loader.check_event(yaml.StreamStartEvent)
        loader.get_event()
        assert loader.check_event(yaml.DocumentStartEvent)
        loader.get_event()

        # assume the root element is a sequence
        assert loader.check_event(yaml.MappingStartEvent)
        loader.get_event()

        added = 0
        failed = 0

        # now while the next event does not end the sequence, process each item
        while not loader.check_event(yaml.MappingEndEvent):
            # compose current item to a node as if it was the root node
            node = loader.compose_node(None, None)
            # we set deep=True for complete processing of all the node's children
            k = loader.construct_object(node, True)
            # compose current item to a node as if it was the root node
            node = loader.compose_node(None, None)
            # we set deep=True for complete processing of all the node's children
            v = loader.construct_object(node, True)

            group_id = k
            name = v['name']['en']
            try:
                cur.execute("""INSERT OR REPLACE INTO Groups VALUES(?,?,?)""", [group_id, name, v['categoryID']])
                added += 1
            except sqlite3.IntegrityError:
                log.error("failed to insert ({},{},{})".format(group_id, name, v['categoryID']))
                failed += 1
        # assume document ends and no further documents are in stream
        loader.get_event()
        assert loader.check_event(yaml.DocumentEndEvent)
        loader.get_event()
        assert loader.check_event(yaml.StreamEndEvent)
        cur.commit()
        if added > 0 or failed == 0:
            log.info("Added {} groups, failed {}".format(added, failed))
        else:
            log.error("Added {} groups, failed {}".format(added, failed))

def build_types(cur):
    if args.initial:
        cur.execute("""
        CREATE TABLE Types(
          ID      INT PRIMARY KEY NOT NULL,
          Name    TEXT NOT NULL,
          GroupID INT NOT NULL,
          MarketGroupID INT
        );""")
        cur.execute("""
        CREATE UNIQUE INDEX Types_ByName ON Types(Name);
        """)

    with open("sde/fsd/types.yaml", "rt") as types_file:
        loader = yaml.SafeLoader(types_file)

        # check proper stream start (should never fail)
        assert loader.check_event(yaml.StreamStartEvent)
        loader.get_event()
        assert loader.check_event(yaml.DocumentStartEvent)
        loader.get_event()

        # assume the root element is a sequence
        assert loader.check_event(yaml.MappingStartEvent)
        loader.get_event()

        added = 0
        failed = 0

        # now while the next event does not end the sequence, process each item
        while not loader.check_event(yaml.MappingEndEvent):
            # compose current item to a node as if it was the root node
            node = loader.compose_node(None, None)
            # we set deep=True for complete processing of all the node's children
            k = loader.construct_object(node, True)
            # compose current item to a node as if it was the root node
            node = loader.compose_node(None, None)
            # we set deep=True for complete processing of all the node's children
            v = loader.construct_object(node, True)

            type_id = k
            name = v['name']['en']
            try:
                cur.execute("""INSERT INTO Types VALUES(?,?,?,?);""", [type_id, name, v['groupID'], v.get('marketGroupID')])
                added += 1
            except sqlite3.IntegrityError:
                log.error("failed to insert ({},{},{})".format(type_id, name, v['groupID']))
                failed += 1

            if added % 10000 == 0:
                cur.commit()

        # assume document ends and no further documents are in stream
        loader.get_event()
        assert loader.check_event(yaml.DocumentEndEvent)
        loader.get_event()
        assert loader.check_event(yaml.StreamEndEvent)
        cur.commit()
        if added > 0 or failed == 0:
            log.info("Added {} groups, failed {}".format(added, failed))
        else:
            log.error("Added {} groups, failed {}".format(added, failed))

def build_reprocessing(cur):
    if args.initial:
        con.execute("""
        CREATE TABLE ReprocessItems(
          ID       INT NOT NULL,
          OutputID INT NOT NULL,
          QuantityYielded INT
        );""")
        con.execute("""
        CREATE UNIQUE INDEX ReprocessItems_Key ON ReprocessItems(ID, OutputID);
        """)

    with open("sde/fsd/typeMaterials.yaml", "rt") as types_file:
        loader = yaml.SafeLoader(types_file)

        # check proper stream start (should never fail)
        assert loader.check_event(yaml.StreamStartEvent)
        loader.get_event()
        assert loader.check_event(yaml.DocumentStartEvent)
        loader.get_event()

        # assume the root element is a sequence
        assert loader.check_event(yaml.MappingStartEvent)
        loader.get_event()

        added = 0
        failed = 0

        # now while the next event does not end the sequence, process each item
        while not loader.check_event(yaml.MappingEndEvent):
            # compose current item to a node as if it was the root node
            node = loader.compose_node(None, None)
            # we set deep=True for complete processing of all the node's children
            k = loader.construct_object(node, True)
            # compose current item to a node as if it was the root node
            node = loader.compose_node(None, None)
            # we set deep=True for complete processing of all the node's children
            v = loader.construct_object(node, True)

            type_id = k
            for material in v['materials']:
                try:
                    con.execute("""INSERT INTO ReprocessItems VALUES(?,?,?);""", [type_id, material['materialTypeID'], material['quantity']])
                    added += 1
                except sqlite3.IntegrityError:
                    log.error("failed to insert ({},{},{})".format(type_id, material['materialTypeID'], material['quantity']))
                    failed += 1

            if added % 10000 == 0:
                cur.commit()

        # assume document ends and no further documents are in stream
        loader.get_event()
        assert loader.check_event(yaml.DocumentEndEvent)
        loader.get_event()
        assert loader.check_event(yaml.StreamEndEvent)
        con.commit()
        if added > 0 or failed == 0:
            log.info("Added {} types for reprocessing, failed {}".format(added, failed))
        else:
            log.error("Added {} types for reprocessing, failed {}".format(added, failed))

def build_stations(cur):
    if args.initial:
        cur.execute("""
        CREATE TABLE Stations(
          ID       INT PRIMARY KEY NOT NULL,
          Name     TEXT NOT NULL,
          SystemID INT NOT NULL,
          RegionID INT NOT NULL
        );""")

    with open("sde/bsd/staStations.yaml", "rt") as sta_file:
        loader = yaml.SafeLoader(sta_file)

        # check proper stream start (should never fail)
        assert loader.check_event(yaml.StreamStartEvent)
        loader.get_event()
        assert loader.check_event(yaml.DocumentStartEvent)
        loader.get_event()

        # assume the root element is a sequence
        assert loader.check_event(yaml.SequenceStartEvent)
        loader.get_event()

        added = 0
        failed = 0

        # now while the next event does not end the sequence, process each item
        while not loader.check_event(yaml.SequenceEndEvent):
            # compose current item to a node as if it was the root node
            node = loader.compose_node(None, None)
            # we set deep=True for complete processing of all the node's children
            v = loader.construct_object(node, True)

            try:
                cur.execute("""INSERT OR REPLACE INTO Stations VALUES(?,?,?,?);""", [v['stationID'], v['stationName'], v['solarSystemID'], v['regionID']])
                added += 1
            except sqlite3.IntegrityError:
                log.error("failed to insert '{}'".format(v['stationID']))
                failed += 1

        # assume document ends and no further documents are in stream
        loader.get_event()
        assert loader.check_event(yaml.DocumentEndEvent)
        loader.get_event()
        assert loader.check_event(yaml.StreamEndEvent)
        cur.commit()
        if added > 0 or failed == 0:
            log.info("Added {} stations, failed {}".format(added, failed))
        else:
            log.error("Added {} stations, failed {}".format(added, failed))

    added = 0
    failed = 0

    with open("extra-stations.csv", "rt") as more_fh:
        r = csv.DictReader(more_fh)
        for row in r:
            try:
                cur.execute("""INSERT OR REPLACE INTO Stations VALUES(?,?,?,?)""",
                        [row['ID'], row['Name'], row['SystemID'], row['RegionID']])
                added += 1
            except sqlite3.IntegrityError:
                log.error("failed to insert extra '{}'".format(row['ID']))
                failed += 1
        cur.commit()
        if added > 0 or failed == 0:
            log.info("Added {} player stations, failed {}".format(added, failed))
        else:
            log.error("Added {} player stations, failed {}".format(added, failed))

def build_systems(cur):
    if args.initial:
        cur.execute("""
        CREATE TABLE Systems(
          ID       INT PRIMARY KEY NOT NULL,
          Name     TEXT NOT NULL,
          Security FLOAT32 NOT NULL
        );""")

    names = {}

    with open("sde/bsd/invNames.yaml", "rt") as names_file:
        loader = yaml.SafeLoader(names_file)

        # check proper stream start (should never fail)
        assert loader.check_event(yaml.StreamStartEvent)
        loader.get_event()
        assert loader.check_event(yaml.DocumentStartEvent)
        loader.get_event()

        # assume the root element is a sequence
        assert loader.check_event(yaml.SequenceStartEvent)
        loader.get_event()

        count = 0
        # now while the next event does not end the sequence, process each item
        while not loader.check_event(yaml.SequenceEndEvent):
            # compose current item to a node as if it was the root node
            node = loader.compose_node(None, None)
            # we set deep=True for complete processing of all the node's children
            v = loader.construct_object(node, True)

            names[v['itemID']] = v['itemName']

        # assume document ends and no further documents are in stream
        loader.get_event()
        assert loader.check_event(yaml.DocumentEndEvent)
        loader.get_event()
        assert loader.check_event(yaml.StreamEndEvent)

    for path in Path("sde/universe/eve").glob('**/solarsystem.yaml'):
        with open(path, "rt") as fh:
            try:
                d = yaml.safe_load(fh)
                systemID = d['solarSystemID']
                cur.execute("""INSERT OR REPLACE INTO Systems VALUES(?,?,?)""",
                        [systemID, names[systemID], d['security']])
            except sqlite3.IntegrityError:
                log.error("failed to insert system from '{}'".format(path))
        cur.commit()


con = sqlite3.connect("sde.db")
cur = con.cursor()
if not args.skip_types:
    build_types(con)
build_reprocessing(con)
build_market_groups(con)
build_groups(con)
build_categories(con)
build_stations(con)
if not args.skip_systems:
    build_systems(con)

log.info('...done')
