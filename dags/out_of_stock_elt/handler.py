import os
import uuid


class ProductHandler:
    def __init__(
            self,
            ingestion_timestamp: str,
            target_path: str,
    ) -> None:
        self._ingestion_timestamp = ingestion_timestamp
        self._target_path = target_path

    def handle(self, product: str, date: str) -> str:
        target_partitioned_path = _with_partitions(
            self._target_path,
            self._ingestion_timestamp,
            date
        )
        return self._write_product(product, target_partitioned_path)

    def _write_product(self, product: str, path: str) -> str:
        os.makedirs(path, exist_ok=True)
        with_file_name = _with_random_file_name(path)
        with open(with_file_name, mode="w") as f:
            f.write(product)
        return with_file_name


def _with_partitions(
        target_folder: str,
        ingestion_timestamp: str,
        date: str
) -> str:
    with_timestamp = os.path.join(
        target_folder,
        "ingestion_timestamp=%s" % ingestion_timestamp
    )
    with_date = os.path.join(with_timestamp, "date=%s" % date)
    return with_date


def _with_random_file_name(path: str) -> str:
    return os.path.join(path, str(uuid.uuid4()) + ".json")
