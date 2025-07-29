import dataclasses
import datetime
import logging
from asyncio import Queue, QueueEmpty
from typing import Dict

from prometheus_client import Gauge

from fa_search_bot.sites.submission_id import SubmissionID

logger = logging.getLogger(__name__)

refresh_counter_dict_size = Gauge(
    "fasearchbot_refreshcounter_dict_size",
    "Number of submissions being tracked by the refresh counter",
)
refresh_counter_max_count = Gauge(
    "fasearchbot_refreshcounter_max_count",
    "Maximum number of refreshes of any item in the refresh counter"
)


@dataclasses.dataclass
class RefreshEntry:
    refresh_count: int = 1
    first_seen: datetime.datetime = dataclasses.field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc)
    )
    latest_seen: datetime.datetime = dataclasses.field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc)
    )

    def observe(self) -> None:
        self.refresh_count += 1
        self.latest_seen = datetime.datetime.now(datetime.timezone.utc)


class TooManyRefresh(Exception):
    pass


class RefreshCounter:
    MAX_AGE = datetime.timedelta(minutes=5)

    def __init__(self, refresh_limit: int):
        self.refresh_limit = refresh_limit
        self._refresh_dict: Dict[SubmissionID, RefreshEntry] = {}
        refresh_counter_dict_size.set_function(lambda: len(self._refresh_dict))
        refresh_counter_max_count.set_function(
            lambda: max([e.refresh_count for e in self._refresh_dict.values()] + [0])
        )

    def _clean(self) -> None:
        old_ids = []
        for sub_id, entry in self._refresh_dict.items():
            if entry.latest_seen < (datetime.datetime.now(datetime.timezone.utc) - self.MAX_AGE):
                old_ids.append(sub_id)
        for sub_id in old_ids:
            del self._refresh_dict[sub_id]

    def add(self, sub_id: SubmissionID) -> None:
        self._clean()
        if entry := self._refresh_dict.get(sub_id):
            if entry.refresh_count > self.refresh_limit:
                logger.warning("Submission %s has been refreshed too many times, raising exception", sub_id)
                raise TooManyRefresh(f"Submission {sub_id} has been refreshed too many ({entry.refresh_count} > {self.refresh_limit}) times")
            entry.observe()
            return
        self._refresh_dict[sub_id] = RefreshEntry()


class FetchQueue:

    def __init__(self, refresh_limit: int = 100):
        self._new_queue: Queue[SubmissionID] = Queue()
        self._refresh_queue: Queue[SubmissionID] = Queue()
        self.refresh_counter = RefreshCounter(refresh_limit=refresh_limit)  # TODO: configurable

    def get_nowait(self) -> SubmissionID:
        try:
            return self._refresh_queue.get_nowait()
        except QueueEmpty:
            return self._new_queue.get_nowait()

    async def put_new(self, sub_id: SubmissionID) -> None:
        await self._new_queue.put(sub_id)

    async def put_refresh(self, sub_id: SubmissionID) -> None:
        self.refresh_counter.add(sub_id)
        await self._refresh_queue.put(sub_id)

    def qsize(self) -> int:
        return self._refresh_queue.qsize() + self._new_queue.qsize()

    def qsize_new(self) -> int:
        return self._new_queue.qsize()

    def qsize_refresh(self) -> int:
        return self._refresh_queue.qsize()
