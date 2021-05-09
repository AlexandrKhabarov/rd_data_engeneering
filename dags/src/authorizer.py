from typing import Dict

import requests
from requests import HTTPError

from dags.src.errors import AuthorizationFailed


class Authorizer:
    def __init__(
            self,
            url: str,
            username: str,
            password: str
    ) -> None:
        self._url = url
        self._username = username
        self._password = password

    def authorize(self) -> str:
        try:
            response = requests.post(
                self._url,
                json=_create_payload(self._username, self._password),
            )
            response.raise_for_status()
        except HTTPError as e:
            raise AuthorizationFailed("Authorization failed.") from e
        else:
            return _parse_response_payload(response.json())


def _create_payload(username: str, password: str) -> Dict[str, str]:
    return {"username": username, "password": password}


def _parse_response_payload(response: Dict[str, str]) -> str:
    return response['access_token']
