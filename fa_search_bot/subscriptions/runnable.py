from __future__ import annotations

import asyncio
import datetime
import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

import heartbeat
from prometheus_client import Gauge, Counter

from fa_search_bot.subscriptions.utils import time_taken

if TYPE_CHECKING:
    from fa_search_bot.subscriptions.subscription_watcher import SubscriptionWatcher


logger = logging.getLogger(__name__)

heartbeat.heartbeat_app_url = "https://heartbeat.spangle.org.uk/"
heartbeat_app_name = "FASearchBot_task_SubIDGatherer"

latest_processed_time = Gauge(
    "fasearchbot_subscriptiontask_last_processed_unixtime",
    "The UNIX timestamp of when the task most recently completed its processing method",
    labelnames=["runnable"],
)
total_processed_count = Counter(
    "fasearchbot_subscriptiontask_total_processed_count",
    "Count of how many times a task type has run its processing method",
    labelnames=["runnable"],
)
time_taken_processing = Summary(
    "fasearchbot_subscriptiontask_do_process_time_taken",
    "Amount of time taken (in seconds) running the do_process() method on each task runner",
    labelnames=["runnable"],
)


class ShutdownError(RuntimeError):
    """
    Used if the runnable was shut down while trying to do something.
    Exists such that it can be ignored during proper shutdown.
    """
    pass


class Runnable(ABC):
    QUEUE_BACKOFF = 0.5
    SECONDS_PER_HEARTBEAT = 60

    def __init__(self, watcher: "SubscriptionWatcher"):
        self.watcher = watcher
        self.running = False
        self.heartbeat_expiry = datetime.datetime.now()
        self.class_name = self.__class__.__name__
        self.time_taken_updating_heartbeat = time_taken.labels(
            task="updating heartbeat", runnable=self.class_name, task_type="active",
        )
        self.runnable_latest_processed = latest_processed_time.labels(runnable=self.class_name)
        self.runnable_processed_count = total_processed_count.labels(runnable=self.class_name)
        self.time_taken_processing = time_taken_processing.labels(runnable=self.class_name)

    async def run(self) -> None:
        # Start the subscription task
        self.running = True
        while self.running:
            try:
                with time_taken_processing.time():
                    await self.do_process()
            except Exception as e:
                logger.error("Runnable task %s has failed (will restart) with exception:", self.class_name, exc_info=e)
                # Revert the failed attempt, so it may be attempted again
                await self.revert_last_attempt()
            # Update metrics
            self.update_processed_metrics()
            # Update heartbeat
            self.update_heartbeat()

    @abstractmethod
    async def do_process(self) -> None:
        pass

    @abstractmethod
    async def revert_last_attempt(self) -> None:
        """
        If the processing fails, call this method to try and revert the item which failed to process, such that
        processing can continue.
        """
        pass

    def stop(self) -> None:
        self.running = False

    def update_processed_metrics(self) -> None:
        self.runnable_latest_processed.set_to_current_time()
        self.runnable_processed_count.inc()

    def update_heartbeat(self):
        if datetime.datetime.now() > self.heartbeat_expiry:
            with self.time_taken_updating_heartbeat.time():
                heartbeat.update_heartbeat(f"FASearchBot_task_{self.class_name}")
            logger.debug("Heartbeat from %s", self.class_name)
            self.heartbeat_expiry = datetime.datetime.now() + datetime.timedelta(seconds=self.SECONDS_PER_HEARTBEAT)

    async def _wait_while_running(self, seconds: float) -> None:
        sleep_end = datetime.datetime.now() + datetime.timedelta(seconds=seconds)
        while datetime.datetime.now() < sleep_end:
            if not self.running:
                break
            await asyncio.sleep(0.1)
