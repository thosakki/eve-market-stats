#!/usr/bin/python3

from requests_oauthlib import OAuth2Session
import yaml
from os import environ

import oauth_token

environ['OAUTHLIB_INSECURE_TRANSPORT'] = 'yes'

with open("config.yaml", "rt") as conffile:
    config = yaml.safe_load(conffile)

print("{}".format(config))

oauth = OAuth2Session(client_id=config['client_id'], redirect_uri=config['redirect_uri'], scope=config['scopes'])
authorization_url, state = oauth.authorization_url(
        config['auth_url'],
        state='mystate',
        )
print("Go to {}, authorise access and tell me the redirect url".format(authorization_url))
authorization_response = input("Enter the full redirect URL: ")

token = oauth.fetch_token(token_url=config['token_url'], authorization_response=authorization_response, client_secret=config['client_secret'])
print("{}".format(oauth_token))

with open("state.yaml", "wt") as statefile:
    oauth_token.ToYaml(statefile, token)

