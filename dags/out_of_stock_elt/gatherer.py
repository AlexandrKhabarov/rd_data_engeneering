from typing import Dict

import requests
from requests import HTTPError

from out_of_stock_elt.errors import GatherFailed
from out_of_stock_elt.interfaces import Authorizer


class ProductGatherer:
    def __init__(
            self,
            url: str,
            timeout: float,
            authorizer: Authorizer
    ) -> None:
        self._url = url
        self._timeout = timeout
        self._authorizer = authorizer

    def gather(self, date: str) -> str:
        token = self._authorizer.authorize()
        try:
            response = requests.get(
                self._url,
                json=_create_payload(date),
                headers=_create_headers(token),
                timeout=self._timeout
            )
            response.raise_for_status()
        except HTTPError as e:
            raise GatherFailed("Can not get product by date %s" % date) from e
        else:
            return response.text


def _create_payload(date: str) -> Dict[str, str]:
    return {'date': date}


def _create_headers(token: str) -> Dict[str, str]:
    return {"Authorization": "JWT %s" % token}
