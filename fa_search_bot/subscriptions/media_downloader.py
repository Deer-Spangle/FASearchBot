from __future__ import annotations

import asyncio
import logging
from asyncio import QueueEmpty
from typing import Optional, TYPE_CHECKING

from aiohttp import ClientPayloadError, ServerDisconnectedError, ClientOSError
from prometheus_client import Counter

from fa_search_bot.sites.furaffinity.sendable import SendableFASubmission
from fa_search_bot.sites.sendable import UploadedMedia, DownloadError, SendSettings, CaptionSettings, DownloadedFile
from fa_search_bot.sites.submission_id import SubmissionID
from fa_search_bot.subscriptions.runnable import Runnable, ShutdownError
from fa_search_bot.subscriptions.utils import time_taken, TimeKeeper
from fa_search_bot.subscriptions.fetch_queue import TooManyRefresh

if TYPE_CHECKING:
    from fa_search_bot.subscriptions.subscription_watcher import SubscriptionWatcher


logger = logging.getLogger(__name__)

time_taken_waiting = time_taken.labels(
    task="waiting for new events in queue", runnable="MediaDownloader", task_type="waiting"
)
time_taken_downloading = time_taken.labels(
    task="downloading media from art site", runnable="MediaDownloader", task_type="active"
)
time_taken_publishing = time_taken.labels(
    task="publishing results to queues", runnable="MediaDownloader", task_type="waiting"
)
cache_results = Counter(
    "fasearchbot_mediadownloader_cache_fetch_count",
    "Count of how many times the media downloader checked the cache for submission media",
    labelnames=["result"]
)
cache_hits = cache_results.labels(result="hit")
cache_misses = cache_results.labels(result="miss")


class MediaDownloader(Runnable):
    CONNECTION_BACKOFF = 20

    def __init__(self, watcher: "SubscriptionWatcher") -> None:
        super().__init__(watcher)
        self.last_processed: Optional[SubmissionID] = None

    async def do_process(self) -> None:
        try:
            full_data = await self.watcher.wait_pool.get_next_for_media_download()
        except QueueEmpty:
            with time_taken_waiting.time():
                await asyncio.sleep(self.QUEUE_BACKOFF)
            return
        sendable = SendableFASubmission(full_data)
        sub_id = sendable.submission_id
        self.last_processed = sub_id
        logger.debug("Got %s from queue, downloading media", sub_id)
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
        logger.debug("Downloading submission media: %s", sub_id)
        try:
            download_timer = TimeKeeper(time_taken_downloading)
            with download_timer.time():
                dl_file = await self.download_sendable(sendable)
            logger.debug("Downloaded submission media: %s, duration: %s seconds", sub_id, download_timer.duration)
        except DownloadError as e:
            if e.exc.status != 404:
                raise ValueError(
                    "Download error while downloading media for submission: %s",
                    sendable.submission_id,
                ) from e
            with time_taken_publishing.time():
                await self.handle_deleted(sendable)
            return
        logger.debug("Download complete for %s, publishing to wait pool", sub_id)
        with time_taken_publishing.time():
            await self.watcher.wait_pool.set_downloaded(sub_id, dl_file)
        self.latest_id_gauge.set(sub_id.submission_id)

    async def handle_deleted(self, sendable: SendableFASubmission) -> None:
        sub_id = sendable.submission_id
        logger.debug("Media for %s disappeared before it could be downloaded, throwing back to the fetch queue", sub_id)
        try:
            await self.watcher.wait_pool.revert_data_fetch(sub_id)
        except TooManyRefresh as e:
            logger.warning(
                "Sending submission %s without media. Image could not be downloaded after maximum retries: %s",
                sub_id,
                e
            )
            uploaded_media = UploadedMedia(sub_id, None, SendSettings(CaptionSettings(False, True, True, True), False, False))
            await self.watcher.wait_pool.set_uploaded(sub_id, uploaded_media)

    async def download_sendable(self, sendable: SendableFASubmission) -> Optional[tuple[DownloadedFile, SendSettings]]:
        while self.running:
            try:
                return await sendable.download()
            except DownloadError as e:
                if e.exc.status in [502, 520, 522, 403, 524]:
                    logger.warning(
                        "Media download failed with %s error. Trying again in %s",
                        e.exc.status,
                        self.CONNECTION_BACKOFF,
                    )
                    await self._wait_while_running(self.CONNECTION_BACKOFF)
                    continue
                raise e
            except ClientPayloadError as e:
                logger.warning(
                    "Download failed, server response incomplete, trying again in %s",
                    self.CONNECTION_BACKOFF,
                    exc_info=e,
                )
                await self._wait_while_running(self.CONNECTION_BACKOFF)
                continue
            except ServerDisconnectedError as e:
                logger.warning(
                    "Disconnected from server while downloading %s, trying again in %s",
                    sendable.submission_id,
                    self.CONNECTION_BACKOFF,
                    exc_info=e
                )
                await self._wait_while_running(self.CONNECTION_BACKOFF)
                continue
            except ClientOSError as e:
                logger.warning(
                    "Client error while downloading %s, trying again in %s",
                    sendable.submission_id,
                    self.CONNECTION_BACKOFF,
                    exc_info=e
                )
                await self._wait_while_running(self.CONNECTION_BACKOFF)
                continue
            except Exception as e:
                raise ValueError("Failed to download media for submission: %s", sendable.submission_id) from e
        raise ShutdownError("Media downloader has shutdown while trying to download media")

    async def revert_last_attempt(self) -> None:
        if self.last_processed is None:
            raise ValueError("Cannot revert last attempt, as there was not a previous attempt")
        # If media failed to download, re-fetch the data, as something may have changed
        await self.watcher.wait_pool.revert_data_fetch(self.last_processed)
