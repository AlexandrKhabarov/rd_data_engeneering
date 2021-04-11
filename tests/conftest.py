import os

import pytest
import responses


@pytest.fixture(name="responses")
def mocked_responses():
    with responses.RequestsMock() as mocked_responses:
        yield mocked_responses


@pytest.fixture
def create_expected_target_path():
    def _create(root_path, ingestion_timestamp, date, uuid4):
        expected_path = root_path.join(
            "ingestion_timestamp=%s" % ingestion_timestamp
        ).join(
            "date=%s" % date
        ).join(
            uuid4 + ".json"
        )
        return expected_path

    return _create


@pytest.fixture
def assert_handled_product_paths():
    def _assert(actual_paths, expected_paths):
        for path in actual_paths:
            assert os.path.exists(path) is True
        assert len(actual_paths) == len(expected_paths)
        assert sorted(actual_paths) == sorted(expected_paths)

    return _assert


@pytest.fixture
def init_test_products(responses):
    url = "http://product-url.com/out_of_stock"

    def _init(payloads_with_status_codes):
        for payload, status_code in payloads_with_status_codes:
            responses.add(
                responses.GET,
                url,
                json=payload,
                status=status_code,
            )
        return url

    return _init


@pytest.fixture
def init_test_auth(responses):
    url = "http://auth-url.com/auth"
    username = "username"
    password = "password"

    def _init(payloads_with_status_codes):
        for payload, status_code in payloads_with_status_codes:
            responses.add(
                responses.POST,
                url,
                json=payload,
                status=status_code,
            )
        return url, username, password

    return _init
