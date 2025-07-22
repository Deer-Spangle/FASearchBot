from __future__ import annotations

import asyncio
import logging
from asyncio import QueueEmpty
from typing import Optional, TYPE_CHECKING

from aiohttp import ClientPayloadError, ServerDisconnectedError, ClientOSError
from prometheus_client import Counter

from fa_search_bot.sites.furaffinity.sendable import SendableFASubmission
from fa_search_bot.sites.sendable import UploadedMedia, try_delete_sandbox_file
from fa_search_bot.sites.submission_id import SubmissionID
from fa_search_bot.subscriptions.runnable import Runnable, ShutdownError
from fa_search_bot.subscriptions.utils import time_taken, TimeKeeper
from fa_search_bot.subscriptions.wait_pool import SubmissionCheckState

if TYPE_CHECKING:
    from fa_search_bot.subscriptions.subscription_watcher import SubscriptionWatcher


logger = logging.getLogger(__name__)

time_taken_waiting = time_taken.labels(
    task="waiting for new events in queue", runnable="MediaUploader", task_type="waiting"
)
time_taken_uploading = time_taken.labels(
    task="uploading media to telegram", runnable="MediaUploader", task_type="active"
)
time_taken_publishing = time_taken.labels(
    task="publishing results to queues", runnable="MediaUploader", task_type="waiting"
)
cache_results = Counter(
    "fasearchbot_mediauploader_cache_fetch_count",
    "Count of how many times the media uploader checked the cache for submission media",
    labelnames=["result"]
)
cache_hits = cache_results.labels(result="hit")
cache_misses = cache_results.labels(result="miss")


class MediaUploader(Runnable):
    CONNECTION_BACKOFF = 20

    def __init__(self, watcher: "SubscriptionWatcher") -> None:
        super().__init__(watcher)
        self.last_processed: Optional[SubmissionID] = None

    async def do_process(self) -> None:
        try:
            sub_state = await self.watcher.wait_pool.get_next_for_media_upload()
        except QueueEmpty:
            with time_taken_waiting.time():
                await asyncio.sleep(self.QUEUE_BACKOFF)
            return
        sub_id = sub_state.sub_id
        self.last_processed = sub_id
        logger.debug("Got %s from queue, uploading media", sub_id)
        # Check if cache entry exists
        cache_entry = self.watcher.submission_cache.load_cache(sub_id)
        if cache_entry:
            cache_hits.inc()
            logger.debug("Got cache entry for %s, setting in waitpool", sub_id)
            with time_taken_publishing.time():
                await self.watcher.wait_pool.set_cached(sub_id, cache_entry)
            return
        cache_misses.inc()
        # Upload the file
        logger.debug("Uploading submission media: %s", sub_id)
        upload_timer = TimeKeeper(time_taken_uploading)
        with upload_timer.time():
            uploaded_media = await self.upload_media(sub_state)
        logger.debug("Uploaded submission media: %s, duration: %s seconds", sub_id, upload_timer.duration)
        # Publish result
        logger.debug("Upload complete for %s, publishing to wait pool", sub_id)
        with time_taken_publishing.time():
            await self.watcher.wait_pool.set_uploaded(sub_id, uploaded_media)
        self.latest_id_gauge.set(sub_id.submission_id)

    async def upload_media(self, sub_state: SubmissionCheckState) -> UploadedMedia:
        sendable = SendableFASubmission(sub_state.full_data)
        dl_file, send_settings = sub_state.dl_file
        while self.running:
            try:
                uploaded_media = await sendable.upload_only(self.watcher.client, dl_file, send_settings)
                try_delete_sandbox_file(dl_file.dl_path)
                return uploaded_media
            except ConnectionError as e:
                logger.warning(
                    "Upload failed, telegram has disconnected, trying again in %s",
                    self.CONNECTION_BACKOFF,
                    exc_info=e
                )
                await self._wait_while_running(self.CONNECTION_BACKOFF)
                continue
            except ClientPayloadError as e:
                logger.warning(
                    "Upload failed, telegram response incomplete, trying again in %s",
                    self.CONNECTION_BACKOFF,
                    exc_info=e,
                )
                await self._wait_while_running(self.CONNECTION_BACKOFF)
                continue
            except ServerDisconnectedError as e:
                logger.warning(
                    "Disconnected from server while uploading %s, trying again in %s",
                    sendable.submission_id,
                    self.CONNECTION_BACKOFF,
                    exc_info=e
                )
                await self._wait_while_running(self.CONNECTION_BACKOFF)
                continue
            except ClientOSError as e:
                logger.warning(
                    "Client error while uploading %s, trying again in %s",
                    sendable.submission_id,
                    self.CONNECTION_BACKOFF,
                    exc_info=e
                )
                await self._wait_while_running(self.CONNECTION_BACKOFF)
                continue
            except Exception as e:
                raise ValueError("Failed to upload media to telegram for submission: %s", sendable.submission_id) from e
        raise ShutdownError("Media uploader has shutdown while trying to upload media")

    async def revert_last_attempt(self) -> None:
        if self.last_processed is None:
            raise ValueError("Cannot revert last attempt, as there was not a previous attempt")
        # If media failed to send, re-fetch the data, as something may have changed
        await self.watcher.wait_pool.revert_data_fetch(self.last_processed)
