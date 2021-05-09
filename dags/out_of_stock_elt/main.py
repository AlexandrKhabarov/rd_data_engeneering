from argparse import ArgumentParser
from dataclasses import dataclass
from typing import List, Optional

from dags.out_of_stock_elt.authorizer import Authorizer
from dags.out_of_stock_elt.config import Config
from dags.out_of_stock_elt.gatherer import ProductGatherer
from dags.out_of_stock_elt.handler import ProductHandler
from dags.out_of_stock_elt.job import Job


@dataclass
class Args:
    config_path: str


def parse_args(manual_args: Optional[List[str]] = None) -> Args:
    parser = ArgumentParser()
    parser.add_argument("--CONFIG_PATH", type=str, required=True, default="./config.yaml")

    raw_args = parser.parse_args(manual_args)

    return Args(config_path=raw_args.CONFIG_PATH)


def main(manual_args: Optional[List[str]] = None) -> None:
    args = parse_args(manual_args)
    config = Config.from_yaml(args.config_path)

    authorizer = Authorizer(
        config.authorization.auth_url,
        config.authorization.username,
        config.authorization.password,
    )
    gatherer = ProductGatherer(
        config.gathering.product_url,
        config.gathering.timeout,
        authorizer,
    )
    handler = ProductHandler(
        config.gathering.ingestion_timestamp,
        config.gathering.target_path,
    )
    job = Job(gatherer, handler)
    job.run(config.gathering.dates)


if __name__ == '__main__':
    main()
