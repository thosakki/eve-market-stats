#!/usr/bin/python3

from argparse import ArgumentParser
from collections import defaultdict, namedtuple
import csv
import logging
import math
import operator
import sqlite3
import yaml

logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)
arg_parser = ArgumentParser(prog='build-sde.py')
arg_parser.add_argument('--initial', action='store_true')
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

    with open("sde/fsd/groupIDs.yaml", "rt") as fh:
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

            cat_id = k
            name = v['name']['en']
            try:
                cur.execute("""INSERT INTO Categories VALUES(?,?)""", [cat_id, name])
            except sqlite3.IntegrityError:
                log.error("failed to insert ({},{})".format(cat_id, name))
        # assume document ends and no further documents are in stream
        loader.get_event()
        assert loader.check_event(yaml.DocumentEndEvent)
        loader.get_event()
        assert loader.check_event(yaml.StreamEndEvent)
        cur.commit()

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

    with open("sde/fsd/groupIDs.yaml", "rt") as fh:
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

            group_id = k
            name = v['name']['en']
            try:
                cur.execute("""INSERT INTO Groups VALUES(?,?,?)""", [group_id, name, v['categoryID']])
            except sqlite3.IntegrityError:
                log.error("failed to insert ({},{},{})".format(group_id, name, v['categoryID']))
        # assume document ends and no further documents are in stream
        loader.get_event()
        assert loader.check_event(yaml.DocumentEndEvent)
        loader.get_event()
        assert loader.check_event(yaml.StreamEndEvent)
        cur.commit()

def build_types(cur):
    if args.initial:
        cur.execute("""
        CREATE TABLE Types(
          ID      INT PRIMARY KEY NOT NULL,
          Name    TEXT NOT NULL,
          GroupID INT NOT NULL
        );""")
        cur.execute("""
        CREATE UNIQUE INDEX Types_ByName ON Types(Name);
        """)

    with open("sde/fsd/typeIDs.yaml", "rt") as types_file:
        loader = yaml.SafeLoader(types_file)

        # check proper stream start (should never fail)
        assert loader.check_event(yaml.StreamStartEvent)
        loader.get_event()
        assert loader.check_event(yaml.DocumentStartEvent)
        loader.get_event()

        # assume the root element is a sequence
        assert loader.check_event(yaml.MappingStartEvent)
        loader.get_event()

        count = 0
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
                cur.execute("""INSERT INTO Types VALUES(?,?,?);""", [type_id, name, v['groupID']])
            except sqlite3.IntegrityError:
                log.error("failed to insert ({},{},{})".format(type_id, name, v['groupID']))

            count+=1
            if count % 10000 == 0:
                cur.commit()

        # assume document ends and no further documents are in stream
        loader.get_event()
        assert loader.check_event(yaml.DocumentEndEvent)
        loader.get_event()
        assert loader.check_event(yaml.StreamEndEvent)
        cur.commit()

con = sqlite3.connect("sde.db")
cur = con.cursor()
build_types(con)
build_groups(con)
build_categories(con)

log.info('...done')
