from typing import Final

import pytest

from dags.out_of_stock_elt.authorizer import Authorizer
from dags.out_of_stock_elt.errors import GatherFailed
from dags.out_of_stock_elt.gatherer import ProductGatherer

_TESTED_DATE: Final = "2021-01-02"
_EXPECTED_PRODUCT_ID: Final = 1

_TIMEOUT: Final = 30.0

_EXPECTED_RESULT: Final = '[{"product_id": 1, "date": "2021-01-02"}]'


def test_get__successful(
        init_test_products,
        init_test_auth,
):
    auth_url, username, password = init_test_auth([
        ({'access_token': "token"}, 200)
    ])
    gather_url = init_test_products([
        ([{"product_id": _EXPECTED_PRODUCT_ID, "date": _TESTED_DATE}], 200)
    ])
    getter = ProductGatherer(
        gather_url,
        _TIMEOUT,
        Authorizer(auth_url, username, password)
    )
    product = getter.gather(_TESTED_DATE)
    assert _EXPECTED_RESULT == product


def test_get__failed(
        init_test_products,
        init_test_auth,
):
    gather_url = init_test_products([
        ({'message': 'No out_of_stock items for this date'}, 404)
    ])
    auth_url, username, password = init_test_auth([
        ({'access_token': "token"}, 200)
    ])
    with pytest.raises(GatherFailed):
        getter = ProductGatherer(
            gather_url,
            _TIMEOUT,
            Authorizer(auth_url, username, password)
        )
        getter.gather(_TESTED_DATE)
