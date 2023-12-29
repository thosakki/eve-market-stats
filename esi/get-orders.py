#!/usr/bin/python3

from argparse import ArgumentParser
import csv
import logging
from oauthlib.oauth2.rfc6749.tokens import OAuth2Token
from os import environ
from requests_oauthlib import OAuth2Session
import sys
from typing import Dict, Iterator
import yaml
from oauthlib.oauth2 import TokenExpiredError

import oauth_token

logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
log = logging.getLogger(__name__)
arg_parser = ArgumentParser(prog='get-token.py')
arg_parser.add_argument('--character', type=int)
args = arg_parser.parse_args()

def token_update(token):
    with open("state-{}.yaml".format(args.character), "wt") as tokenfile:
        oauth_token.ToYaml(tokenfile, token)

def get_orders(config: dict, token: OAuth2Token) -> Iterator[Dict[str, any]]:
  client = OAuth2Session(client_id=config['client_id'], token=token)
  try:
    raw = client.get('https://esi.evetech.net/v2/characters/{}/orders/'.format(args.character))
  except TokenExpiredError as e:
    token = client.refresh_token(token_url=config['token_url'], auth=(config['client_id'], config['client_secret']))
    log.info("refreshed oauth2 token")
    token_update(token)
    raw = client.get('https://esi.evetech.net/v2/characters/{}/orders/'.format(args.character))

  if raw.status_code != 200:
      if raw.status_code == 404:
          return
      raw.raise_for_status()
  r = raw.json()
  for x in r:
      if 'is_buy_order' in r: continue
      yield x

def main():
  environ['OAUTHLIB_INSECURE_TRANSPORT'] = 'yes'
  
  with open("config.yaml", "rt") as conffile:
      config = yaml.safe_load(conffile)
  with open("state-{}.yaml".format(args.character), "rt") as tokenfile:
      token = oauth_token.FromYaml(tokenfile)

  w = csv.writer(sys.stdout)
  # {'duration': 90, 'is_corporation': False, 'issued': '2023-12-29T11:56:37Z', 'location_id': 60005686, 'order_id': 6677817635, 'price': 1220000.0, 'range': 'region', 'region_id': 10000042, 'type_id': 3033, 'volume_remain': 22, 'volume_total': 22}
  w.writerow(['TypeID', 'Quantity', 'Original Quantity', 'Price', 'LocationID'])
  for r in get_orders(config, token):
      w.writerow([r['type_id'], r['volume_remain'], r['volume_total'], r['price'], r['location_id']])


if __name__ == "__main__":
    main()
