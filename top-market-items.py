#!/usr/bin/python3

from argparse import ArgumentParser
from collections import defaultdict, namedtuple
import csv
import logging
import math
import operator
import yaml

logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)
arg_parser = ArgumentParser(prog='top-market-items')
arg_parser.add_argument('--include_group', nargs='*', type=int)
arg_parser.add_argument('--exclude_group', nargs='*', type=int)
arg_parser.add_argument('--include_category', nargs='*', type=int)
arg_parser.add_argument('--exclude_category', nargs='*', type=int)
args = arg_parser.parse_args()

items = {}
TypeInfo = namedtuple('TypeInfo', ['groupID', 'ID', 'name'])

# Commodity,Number of trades,Traded items,Value of trades,Lst,,as per ESI; complete New Eden; last update: 24.11.2023
# PLEX,2.974,1.078.606,4.415.528.028.140,,,

def get_groups():
    group = {}
    count = 0
    with open("sde/fsd/groupIDs.yaml", "rt") as types_file:
        loader = yaml.SafeLoader(types_file)

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
            count += 1
            name = v['name']['en']
            group[group_id] = v['categoryID']
            if count % 1000 == 0:
                log.info('{}({}): {}'.format(name, group_id, group[group_id]))
        # assume document ends and no further documents are in stream
        loader.get_event()
        assert loader.check_event(yaml.DocumentEndEvent)
        loader.get_event()
        assert loader.check_event(yaml.StreamEndEvent)
    log.info(group)
    return group

def get_types():
    type = {}
    count = 0
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
            count += 1
            name = v['name']['en']
            type[name] = TypeInfo(groupID=v['groupID'], ID=type_id, name=name)
            if count % 1000 == 0:
                log.info('{}({}): {}'.format(name, type_id, type[name]))
        # assume document ends and no further documents are in stream
        loader.get_event()
        assert loader.check_event(yaml.DocumentEndEvent)
        loader.get_event()
        assert loader.check_event(yaml.StreamEndEvent)
    return type


log.info(args.exclude_category)
log.info('Getting type info...')
group_info = get_groups()
type_info = get_types()
log.info('...type info loaded')

log.info('Reading trade volumes...')
with open('popular.csv') as market_data_csv:
    reader = csv.DictReader(market_data_csv)
    for r in reader:
        t = r['Commodity']

        ti = type_info.get(t)
        if ti is None:
            log.warning('Unknown type {}'.format(t))
            continue
        if args.include_group is not None and ti.groupID not in args.include_group: continue
        if args.exclude_group is not None and ti.groupID in args.exclude_group: continue
        category = group_info.get(ti.groupID, '-')
        if args.include_category is not None and category not in args.include_category: continue
        if args.exclude_category is not None and category in args.exclude_category: continue

        value_traded = int(r['Value of trades'].replace('.', ''))
        traded_items = int(r['Traded items'].replace('.', ''))
        items[ti.ID] = { 'name': ti.name, 'category': category, 'group': ti.groupID, 'num': traded_items, 'value': value_traded, 'score': value_traded / math.pow(value_traded/traded_items, 0.6)}
log.info('...read trade volumes')

log.info('Producing outout...')
count = 0
for w in sorted(items.items(), key=lambda x: x[1]['score'], reverse=True):
    print(w[0], w[1])
    #print("{}\t{}\t{}\t{}".format(w, items[w]['num'], items[w]['value'], items[w]['score']))
    count += 1
    if count > 10000:
        break
log.info('...done')
