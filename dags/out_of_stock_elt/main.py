from argparse import ArgumentParser
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Args:
    auth_url: str
    username: str
    password: str
    product_url: str
    target_path: str
    timeout: float
    ingestion_timestamp: str
    dates: List[str]


def parse_args(manual_args: Optional[List[str]] = None) -> Args:
    parser = ArgumentParser()
    parser.add_argument("--AUTH_URL", type=str, required=True)
    parser.add_argument("--USERNAME", type=str, required=True)
    parser.add_argument("--PASSWORD", type=str, required=True)
    parser.add_argument("--PRODUCT_URL", type=str, required=True)
    parser.add_argument("--TARGET_PATH", type=str, required=True)
    parser.add_argument("--TIMEOUT", type=float, required=True)
    parser.add_argument("--INGESTION_TIMESTAMP", type=str, required=True)
    parser.add_argument("--DATES", type=str, required=True, nargs="+")

    raw_args = parser.parse_args(manual_args)

    return Args(
        auth_url=raw_args.AUTH_URL,
        username=raw_args.USERNAME,
        password=raw_args.PASSWORD,
        product_url=raw_args.PRODUCT_URL,
        target_path=raw_args.TARGET_PATH,
        timeout=raw_args.TIMEOUT,
        ingestion_timestamp=raw_args.INGESTION_TIMESTAMP,
        dates=raw_args.DATES,
    )


def main(manual_args: Optional[List[str]] = None) -> None:
    from .authorizer import Authorizer
    from .gatherer import ProductGatherer
    from .handler import ProductHandler
    from .job import Job

    args = parse_args(manual_args)

    authorizer = Authorizer(
        args.auth_url,
        args.username,
        args.password,
    )
    gatherer = ProductGatherer(
        args.product_url,
        args.timeout,
        authorizer,
    )
    handler = ProductHandler(
        args.ingestion_timestamp,
        args.target_path,
    )
    job = Job(gatherer, handler)
    job.run(args.dates)


if __name__ == '__main__':
    main()
