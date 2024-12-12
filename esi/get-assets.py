#!/usr/bin/python3

from argparse import ArgumentParser
import csv
import logging
from oauthlib.oauth2.rfc6749.tokens import OAuth2Token
from os import environ
import requests
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

def get_assets(config: dict, token: OAuth2Token) -> Iterator[Dict[str, any]]:
  client = OAuth2Session(client_id=config['client_id'], token=token)
  i = 1
  retries = 0
  while True:
      try:
        raw = client.get('https://esi.evetech.net/v5/characters/{}/assets/?page={}'.format(args.character, i))

        i += 1
        if i > 10:
            raise RuntimeError("runaway?")
        if raw.status_code != 200:
            if raw.status_code == 404:
                break
            raw.raise_for_status()
        r = raw.json()
        for x in r:
            yield x
      except TokenExpiredError as e:
        token = client.refresh_token(token_url=config['token_url'], auth=(config['client_id'], config['client_secret']))
        log.info("refreshed oauth2 token")
        token_update(token)
        raw = client.get('https://esi.evetech.net/v5/characters/{}/assets/?page={}'.format(args.character, i))
      except requests.exceptions.HTTPError as e:
        retries += 1
        if retries > 3:
          raise
        log.warn("failed to fetch, retrying; {}".format(e))

def main():
  environ['OAUTHLIB_INSECURE_TRANSPORT'] = 'yes'
  
  with open("config.yaml", "rt") as conffile:
      config = yaml.safe_load(conffile)
  with open("state-{}.yaml".format(args.character), "rt") as tokenfile:
      token = oauth_token.FromYaml(tokenfile)

  w = csv.writer(sys.stdout)
  # {'is_singleton': False, 'item_id': 1043802222344, 'location_flag': 'Hangar', 'location_id': 60005686, 'location_type': 'station', 'quantity': 2, 'type_id': 37457}
  w.writerow(['TypeID', 'Singleton', 'Quantity', 'LocationFlag', 'LocationType', 'LocationID'])
  for r in get_assets(config, token):
      w.writerow([r['type_id'], 'True' if r['is_singleton'] else 'False',  r['quantity'], r['location_flag'], r['location_type'], r['location_id']])


if __name__ == "__main__":
    main()
