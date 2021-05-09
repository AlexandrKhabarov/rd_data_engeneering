from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

from dags.src.errors import GatherFailed
from dags.src.interfaces import Gatherer, Handler


class Job:
    def __init__(
            self,
            gatherer: Gatherer,
            handler: Handler
    ) -> None:
        self._gatherer = gatherer
        self._handler = handler

    def run(self, dates: List[str]) -> List[str]:
        results, gather_tasks, writing_tasks = [], {}, []
        with ThreadPoolExecutor() as gather_pool, ThreadPoolExecutor() as writing_pool:
            for date in dates:
                future = gather_pool.submit(self._gatherer.gather, date)
                gather_tasks[future] = date

            for future in as_completed(gather_tasks):
                try:
                    date = gather_tasks[future]
                    writing_tasks.append(writing_pool.submit(self._handler.handle, future.result(), date))
                except GatherFailed:
                    pass

            for future in as_completed(writing_tasks):
                results.append(future.result())

        return results

    def _handle_date(self, date: str) -> str:
        product = self._gatherer.gather(date)
        return self._handler.handle(product, date)
