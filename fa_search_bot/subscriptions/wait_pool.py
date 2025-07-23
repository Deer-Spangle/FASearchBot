from __future__ import annotations

import dataclasses
import logging
from asyncio import Lock, QueueEmpty, Event
from typing import Optional, Dict, Union

from telethon.tl.types import TypeInputPeer

from fa_search_bot.config import DEFAULT_MAX_READY_FOR_UPLOAD
from fa_search_bot.sites.furaffinity.fa_submission import FASubmissionFull
from fa_search_bot.sites.sendable import UploadedMedia, DownloadedFile, SendSettings
from fa_search_bot.sites.sent_submission import SentSubmission
from fa_search_bot.sites.submission_id import SubmissionID
from fa_search_bot.subscriptions.fetch_queue import FetchQueue
from fa_search_bot.subscriptions.subscription import Subscription

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class SubmissionCheckState:
    sub_id: SubmissionID
    full_data: Optional[FASubmissionFull] = None
    matching_subscriptions: Optional[list[Subscription]] = None
    media_downloading: bool = False
    dl_file: Optional[tuple[DownloadedFile, SendSettings]] = None
    media_uploading: bool = False
    cache_entry: Optional[SentSubmission] = None
    uploaded_media: Optional[UploadedMedia] = None
    sent_to: list[Union[int, TypeInputPeer]] = dataclasses.field(default_factory=list)

    def key(self) -> int:
        return int(self.sub_id.submission_id)

    def reset(self) -> None:
        self.full_data = None
        self.matching_subscriptions = None
        self.media_downloading = False
        self.dl_file = None
        self.media_uploading = False
        self.cache_entry = None
        self.uploaded_media = None

    def is_ready_for_media_download(self) -> bool:
        return all(
            [
                self.full_data is not None,
                self.dl_file is None,
                not self.media_downloading,
            ]
        )

    def is_ready_for_media_upload(self) -> bool:
        return all(
            [
                self.dl_file is not None,
                not self.media_uploading,
                not self.is_ready_to_send(),
            ]
        )

    def is_ready_to_send(self) -> bool:
        return any(
            [
                self.uploaded_media is not None,
                self.cache_entry is not None,
            ]
        )


class WaitPool:
    """
    WaitPool governs the overall progress of the subscription watcher. New IDs are added here, and then populated by the
    data fetchers and media watchers.
    The sender is watching for the next item in the pool which is ready to send
    """

    def __init__(self, max_ready_for_upload: int = DEFAULT_MAX_READY_FOR_UPLOAD) -> None:
        self.max_ready_for_upload = max_ready_for_upload
        self.submission_state: Dict[SubmissionID, SubmissionCheckState] = {}
        self.active_states: Dict[SubmissionID, SubmissionCheckState] = {}
        self.fetch_data_queue: FetchQueue = FetchQueue()
        self._lock = Lock()
        self._media_uploading_event = Event()
        self._cache_qsize_download: Optional[int] = None
        self._cache_qsize_upload: Optional[int] = None
        self._cache_qsize_send: Optional[int] = None

    async def add_sub_id(self, sub_id: SubmissionID) -> None:
        async with self._lock:
            state = SubmissionCheckState(sub_id)
            self.submission_state[sub_id] = state
            await self.fetch_data_queue.put_new(sub_id)

    async def get_next_for_data_fetch(self) -> SubmissionID:
        return self.fetch_data_queue.get_nowait()

    async def set_fetched_data(
            self,
            sub_id: SubmissionID,
            full_data: FASubmissionFull,
            matching_subscriptions: list[Subscription],
    ) -> None:
        # Provide backpressure on data fetcher, to avoid it running ahead of downstream processing
        # But only if that submission is not being actively handled somewhere
        if sub_id not in self.active_states:
            while self.size_active() > self.max_ready_for_upload:
                logger.debug("Waiting for media uploads to get below submission count limit")
                await self._media_uploading_event.wait()
        async with self._lock:
            if sub_id not in self.submission_state:
                return
            self.submission_state[sub_id].full_data = full_data
            self.submission_state[sub_id].matching_subscriptions = matching_subscriptions
            # When data is fetched, copy to active states
            self.active_states[sub_id] = self.submission_state[sub_id]

    async def revert_data_fetch(self, sub_id: SubmissionID) -> None:
        # This reverts a submission back to before any data was fetched about it, and re-queues it for data fetch
        async with self._lock:
            if sub_id not in self.submission_state:
                self.submission_state[sub_id] = SubmissionCheckState(sub_id)
            self.submission_state[sub_id].reset()
            # Don't remove from active states, that would risk a deadlock
            # Re-queue for data fetch refresh
            await self.fetch_data_queue.put_refresh(sub_id)

    def states_ready_for_media_download(self) -> list[SubmissionCheckState]:
        return [s for s in self.active_states.values() if s.is_ready_for_media_download()]

    async def get_next_for_media_download(self) -> FASubmissionFull:
        async with self._lock:
            submission_states = self.states_ready_for_media_download()
            if not submission_states:
                raise QueueEmpty()
            next_state = min(submission_states, key=lambda state: state.key())
            next_state.media_downloading = True
            self._media_uploading_event.set()
            self._media_uploading_event.clear()
            return next_state.full_data

    async def set_downloaded(self, sub_id: SubmissionID, downloaded: tuple[DownloadedFile, SendSettings]) -> None:
        async with self._lock:
            if sub_id not in self.submission_state:
                return
            self.submission_state[sub_id].dl_file = downloaded
            self.submission_state[sub_id].media_downloading = False

    def states_ready_for_media_upload(self) -> list[SubmissionCheckState]:
        return [s for s in self.active_states.values() if s.is_ready_for_media_upload()]

    async def get_next_for_media_upload(self) -> SubmissionCheckState:
        async with self._lock:
            submission_states = self.states_ready_for_media_upload()
            if not submission_states:
                raise QueueEmpty()
            next_state = min(submission_states, key=lambda state: state.key())
            next_state.media_uploading = True
            self._media_uploading_event.set()
            self._media_uploading_event.clear()
            if next_state.full_data is None or next_state.dl_file is None:
                raise ValueError(f"Submission ID {next_state.sub_id} is ready for upload, but lacks data or media")
            return next_state

    async def set_cached(self, sub_id: SubmissionID, cache_entry: SentSubmission) -> None:
        async with self._lock:
            if sub_id not in self.submission_state:
                return
            self.submission_state[sub_id].cache_entry = cache_entry
            self.submission_state[sub_id].media_uploading = False

    async def set_uploaded(self, sub_id: SubmissionID, uploaded: UploadedMedia) -> None:
        async with self._lock:
            if sub_id not in self.submission_state:
                return
            self.submission_state[sub_id].uploaded_media = uploaded
            self.submission_state[sub_id].media_uploading = False

    async def remove_state(self, sub_id: SubmissionID) -> None:
        async with self._lock:
            if sub_id not in self.submission_state:
                raise ValueError("This state cannot be removed because it is not in the wait pool")
            del self.submission_state[sub_id]

    def states_ready_to_send(self) -> list[SubmissionCheckState]:
        return [s for s in self.active_states.values() if s.is_ready_to_send()]

    async def pop_next_ready_to_send(self) -> Optional[SubmissionCheckState]:
        async with self._lock:
            submission_states = self.submission_state.values()
            if not submission_states:
                return None
            next_state = min(submission_states, key=lambda state: state.key())
            if not next_state.is_ready_to_send():
                if self.size() > self.max_ready_for_upload:
                    logger.debug(
                        "Backlog is large, but next submission in wait pool, %s, is unready to send",
                        next_state.sub_id
                    )
                return None
            del self.submission_state[next_state.sub_id]
            del self.active_states[next_state.sub_id]
            self._media_uploading_event.set()
            self._media_uploading_event.clear()
            return next_state

    async def return_populated_state(self, state: SubmissionCheckState) -> None:
        async with self._lock:
            self.submission_state[state.sub_id] = state
            if state.full_data is not None:
                self.active_states[state.sub_id] = state

    def size(self) -> int:
        return len(self.submission_state)

    def size_active(self) -> int:
        return len(self.active_states)

    def qsize_fetch_new(self) -> int:
        return self.fetch_data_queue.qsize_new()

    def qsize_fetch_refresh(self) -> int:
        return self.fetch_data_queue.qsize_refresh()

    def qsize_download(self) -> int:
        try:
            self._cache_qsize_download = len(self.states_ready_for_media_download())
        except Exception:
            pass
        return self._cache_qsize_download

    def qsize_upload(self) -> int:
        try:
            self._cache_qsize_upload = len(self.states_ready_for_media_upload())
        except Exception:
            pass
        return self._cache_qsize_upload

    def qsize_send(self) -> int:
        try:
            self._cache_qsize_send = len(self.states_ready_to_send())
        except Exception:
            pass
        return self._cache_qsize_send
