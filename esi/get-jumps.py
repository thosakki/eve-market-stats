#!/usr/bin/python3

from argparse import ArgumentParser
import csv
import logging
from oauthlib.oauth2.rfc6749.tokens import OAuth2Token
from os import environ
from requests_oauthlib import OAuth2Session
import sqlite3
import sys
from typing import Dict, Iterator
import yaml
from oauthlib.oauth2 import TokenExpiredError

import oauth_token

sys.path.append("..")
import lib

logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
log = logging.getLogger(__name__)
arg_parser = ArgumentParser(prog='get-token.py')
arg_parser.add_argument('--character', type=int)
args = arg_parser.parse_args()

sde_conn = sqlite3.connect("../sde.db")

def token_update(token):
    with open("state-{}.yaml".format(args.character), "wt") as tokenfile:
        oauth_token.ToYaml(tokenfile, token)

def get_jumps(config: dict, token: OAuth2Token):
  client = OAuth2Session(client_id=config['client_id'], token=token)
  try:
      raw = client.get('https://esi.evetech.net/v1/universe/system_jumps/')
  except TokenExpiredError as e:
      token = client.refresh_token(token_url=config['token_url'], auth=(config['client_id'], config['client_secret']))
      log.info("refreshed oauth2 token")
      token_update(token)
      raw = client.get('https://esi.evetech.net/v1/universe/system_jumps/')

  if raw.status_code != 200:
      if raw.status_code == 404:
          return
      raw.raise_for_status()
  r = raw.json()
  for x in r:
      yield x
  
def main():
  environ['OAUTHLIB_INSECURE_TRANSPORT'] = 'yes'
  
  with open("config.yaml", "rt") as conffile:
      config = yaml.safe_load(conffile)
  with open("state-{}.yaml".format(args.character), "rt") as tokenfile:
      token = oauth_token.FromYaml(tokenfile)

  w = csv.writer(sys.stdout)
  w.writerow(['SystemID', 'System Name', 'Security', 'Ship Jumps'])
  for r in get_jumps(config, token):
      name, sec = lib.get_system_info(sde_conn.cursor(), r['system_id'])
      w.writerow([r['system_id'], name, sec, r['ship_jumps']])


if __name__ == "__main__":
    main()
