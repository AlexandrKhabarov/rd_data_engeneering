from typing_extensions import Protocol


class Gatherer(Protocol):
    def gather(self, date: str) -> str:
        pass


class Authorizer(Protocol):
    def authorize(self) -> str:
        pass


class Handler(Protocol):
    def handle(self, product: str, date: str) -> str:
        pass
