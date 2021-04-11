from typing import List

import yaml
from pydantic import BaseModel


class Authorization(BaseModel):
    auth_url: str
    username: str
    password: str


class Gathering(BaseModel):
    product_url: str
    target_path: str
    timeout: float
    ingestion_timestamp: str
    dates: List[str]


class Config(BaseModel):
    authorization: Authorization
    gathering: Gathering

    @classmethod
    def from_yaml(cls, path: str) -> 'Config':
        with open(path, "r") as f:
            return cls(**yaml.load(f, Loader=yaml.FullLoader))
