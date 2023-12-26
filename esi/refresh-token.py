#!/usr/bin/python3

import base64
from requests_oauthlib import OAuth2Session
import yaml
from os import environ

import oauth_token

environ['OAUTHLIB_INSECURE_TRANSPORT'] = 'yes'

with open("config.yaml", "rt") as conffile:
    config = yaml.safe_load(conffile)
with open("state.yaml", "rt") as tokenfile:
    token = oauth_token.FromYaml(tokenfile)

client = OAuth2Session(client_id=config['client_id'], token=token)
token = client.refresh_token(token_url=config['token_url'], auth=(config['client_id'], config['client_secret']))
print("{}".format(oauth_token))

with open("state.yaml", "wt") as statefile:
    oauth_token.ToYaml(statefile, token)

