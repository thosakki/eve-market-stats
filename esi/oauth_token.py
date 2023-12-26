from oauthlib.oauth2.rfc6749.tokens import OAuth2Token
from typing import IO
import yaml

def FromYaml(fh: IO) -> OAuth2Token:
    token_data = yaml.safe_load(fh)
    scope = token_data.pop('scope')
    return OAuth2Token(params=token_data, old_scope=scope)

def ToYaml(fh: IO, token: OAuth2Token):
    token_data = dict(token.items())
    token_data['scope'] = token.scope
    yaml.dump(token_data, fh)

