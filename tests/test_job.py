import uuid
from typing import Final

from dags.out_of_stock_elt.authorizer import Authorizer
from dags.out_of_stock_elt.gatherer import ProductGatherer
from dags.out_of_stock_elt.handler import ProductHandler
from dags.out_of_stock_elt.job import Job

_TIMEOUT: Final = 30.0

_INGESTION_TIMESTAMP = "2012-12-12T12-12-12"

_FIRST_DATE: Final = "2021-01-02"
_SECOND_DATE: Final = "2021-01-03"

_FIRST_UUID4: Final = uuid.UUID('a8fe621d-0db3-41fd-8d3c-c6103da0273f')
_SECOND_UUID4: Final = uuid.UUID('be41c842-4d0d-4469-adad-a2e001bb6545')


def test_job(
        init_test_products,
        init_test_auth,
        assert_handled_product_paths,
        create_expected_target_path,
        mocker,
        tmpdir
):
    gather_url = init_test_products([
        ([{"product_id": 1, "date": _FIRST_DATE}], 200),
        ([{"product_id": 2, "date": _SECOND_DATE}], 200),
    ])
    auth_url, username, password = init_test_auth([
        ({'access_token': "token"}, 200)
    ])

    mocked_uuid = mocker.patch('out_of_stock_elt.handler.uuid')
    mocked_uuid.uuid4.side_effect = [_FIRST_UUID4, _SECOND_UUID4]

    gatherer = ProductGatherer(
        gather_url,
        _TIMEOUT,
        Authorizer(auth_url, username, password),
    )

    handler = ProductHandler(_INGESTION_TIMESTAMP, tmpdir)

    job = Job(gatherer, handler)
    actual_paths = job.run([_FIRST_DATE, _SECOND_DATE])

    assert_handled_product_paths(
        actual_paths,
        [
            str(create_expected_target_path(tmpdir, _INGESTION_TIMESTAMP, _FIRST_DATE, str(_FIRST_UUID4))),
            str(create_expected_target_path(tmpdir, _INGESTION_TIMESTAMP, _SECOND_DATE, str(_SECOND_UUID4)))
        ]
    )
