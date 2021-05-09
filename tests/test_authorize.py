from typing import Final

import pytest

from dags.src.authorizer import Authorizer
from dags.src.errors import AuthorizationFailed

_TESTED_TOKEN: Final = "tested-token"


def test_authorize__successful(init_test_auth):
    auth_url, username, password = init_test_auth([
        ({'access_token': _TESTED_TOKEN}, 200)
    ])
    authorizer = Authorizer(auth_url, username, password)
    token = authorizer.authorize()
    assert _TESTED_TOKEN == token


def test_authorize__failed(init_test_auth):
    auth_url, username, password = init_test_auth([
        ({'description': 'Invalid credentials', 'error': 'Bad Request', 'status_code': 401}, 401)
    ])
    authorizer = Authorizer(auth_url, username, password)
    with pytest.raises(AuthorizationFailed):
        authorizer.authorize()
