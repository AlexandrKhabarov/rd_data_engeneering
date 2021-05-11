import uuid
from typing import Final

from dags.out_of_stock_elt.handler import ProductHandler

_INGESTION_TIMESTAMP: Final = "2020-12-12T12-12-12"
_REQUESTED_DATE: Final = "2021-01-02"
_PRODUCT: Final = '[{"product_id": 1, "date": "2021-01-02"}]'

_UUID4 = uuid.UUID('a8fe621d-0db3-41fd-8d3c-c6103da0273f')


def test_handle_product(
        mocker,
        tmpdir,
        assert_handled_product_paths,
        create_expected_target_path
):
    mocked_uuid = mocker.patch('out_of_stock_elt.handler.uuid')
    mocked_uuid.uuid4.return_value = _UUID4

    handler = ProductHandler(_INGESTION_TIMESTAMP, str(tmpdir))
    actual_path = handler.handle(_PRODUCT, _REQUESTED_DATE)

    expected_path = create_expected_target_path(tmpdir, _INGESTION_TIMESTAMP, _REQUESTED_DATE, str(_UUID4))

    assert_handled_product_paths([actual_path], [expected_path])
