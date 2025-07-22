from __future__ import annotations

import logging
from typing import List, Optional, TYPE_CHECKING

from prometheus_client import Counter

from fa_search_bot.sites.furaffinity.fa_export_api import CloudflareError
from fa_search_bot.sites.furaffinity.fa_submission import FASubmission
from fa_search_bot.sites.submission_id import SubmissionID
from fa_search_bot.subscriptions.runnable import Runnable
from fa_search_bot.subscriptions.utils import time_taken, _latest_submission_in_list

if TYPE_CHECKING:
    from fa_search_bot.subscriptions.subscription_watcher import SubscriptionWatcher

logger = logging.getLogger(__name__)

time_taken_listing_api = time_taken.labels(
    task="listing submissions to check", runnable="SubIDGatherer", task_type="active"
)
time_taken_waiting = time_taken.labels(task="waiting before re-checking", runnable="SubIDGatherer", task_type="waiting")
time_taken_publishing = time_taken.labels(
    task="publishing results to queues", runnable="SubIDGatherer", task_type="active"
)
counter_browse_requests = Counter(
    "fasearchbot_subidgatherer_browse_page_request_count",
    "Number of times the browse page has been requested by the submission ID gatherer",
    labelnames=["result"],
)
browse_request_success = counter_browse_requests.labels(result="success")
browse_request_cloudflare = counter_browse_requests.labels(result="cloudflare_error")
browse_request_error = counter_browse_requests.labels(result="error")
counter_home_requests = Counter(
    "fasearchbot_subidgatherer_home_page_request_count",
    "Number of times the home page has been requested by the submission ID gatherer",
    labelnames=["result"]
)
home_request_success = counter_home_requests.labels(result="success")
home_request_cloudflare = counter_home_requests.labels(result="cloudflare_error")
home_request_error = counter_home_requests.labels(result="error")


class SubIDGatherer(Runnable):
    BROWSE_RETRY_BACKOFF = 20
    NEW_ID_BACKOFF = 20
    NEW_ID_BACKOFF_BIG_BACKLOG = 60 * 4
    BIG_BACKOFF_CUTOFF = 500

    def __init__(self, watcher: "SubscriptionWatcher"):
        super().__init__(watcher)
        self.latest_recorded_id: Optional[int] = None
        if self.watcher.latest_ids:
            self.latest_recorded_id = max(int(x) for x in self.watcher.latest_ids)
        self.latest_id_gauge.set_function(lambda: self.latest_recorded_id)

    async def do_process(self) -> None:
        try:
            with time_taken_listing_api.time():
                new_results = await self._get_new_results()
        except Exception as e:
            logger.error("Failed to get new results", exc_info=e)
            return
        for result in new_results:
            sub_id = SubmissionID("fa", result.submission_id)
            logger.debug("Publishing %s to queue and wait pool", sub_id)
            # Publish submission ID to queues for the other tasks
            with time_taken_publishing.time():
                await self.watcher.wait_pool.add_sub_id(sub_id)
        # Wait before checking for more
        with time_taken_waiting.time():
            pool_size = self.watcher.wait_pool.size()
            if pool_size < self.BIG_BACKOFF_CUTOFF:
                await self._wait_while_running(self.NEW_ID_BACKOFF)
            else:
                logger.debug(
                    "Backlog is quite large (%s>%s). Will backoff for %ss before checking for new submission IDs",
                    pool_size, self.BIG_BACKOFF_CUTOFF, self.NEW_ID_BACKOFF_BIG_BACKLOG)
                await self._wait_while_running(self.NEW_ID_BACKOFF_BIG_BACKLOG)

    async def _get_new_results(self) -> List[FASubmission]:
        """
        Gets new results since last scan, returning them in order from oldest to newest.
        """
        if self.latest_recorded_id is None:
            logger.info("First time checking subscriptions, getting initial submissions")
            newest_submission = await self._get_newest_submission()
            if not newest_submission:
                return []
            self.latest_recorded_id = int(newest_submission.submission_id)
            return []
        newest_submission = await self._get_newest_submission()
        if not newest_submission:
            return []
        newest_id = int(newest_submission.submission_id)
        logger.info("Newest ID on FA: %s, latest recorded ID: %s", newest_id, self.latest_recorded_id)
        new_results = [FASubmission(str(x)) for x in range(newest_id, int(self.latest_recorded_id), -1)]
        self.latest_recorded_id = newest_id
        logger.info("New submissions: %s", len(new_results))
        # Return oldest result first
        return new_results[::-1]

    async def _get_newest_submission(self) -> Optional[FASubmission]:
        while self.running:
            try:
                browse_results = await self.watcher.api.get_browse_page(1)
                browse_request_success.inc()
                return _latest_submission_in_list(browse_results)
            except CloudflareError:
                browse_request_cloudflare.inc()
                logger.warning("FA is under cloudflare protection, waiting before retry")
                await self._wait_while_running(self.BROWSE_RETRY_BACKOFF)
            except Exception as e:
                browse_request_error.inc()
                logger.warning("Failed to get browse page, attempting home page", exc_info=e)
            try:
                home_page = await self.watcher.api.get_home_page()
                home_request_success.inc()
                return _latest_submission_in_list(home_page.all_submissions())
            except CloudflareError:
                home_request_cloudflare.inc()
                logger.warning("FA is under cloudflare protection, waiting before retry")
                await self._wait_while_running(self.BROWSE_RETRY_BACKOFF)
            except Exception as e:
                home_request_error.inc()
                logger.warning("Failed to get browse or home page, retrying", exc_info=e)
                await self._wait_while_running(self.BROWSE_RETRY_BACKOFF)
        return None

    async def revert_last_attempt(self) -> None:
        return
