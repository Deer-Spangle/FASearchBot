import dataclasses
import datetime
from typing import Optional

from prometheus_client import Counter, Summary
from prometheus_client.context_managers import Timer

from fa_search_bot.database import Database, DBCacheEntry
from fa_search_bot.sites.handler_group import HandlerGroup
from fa_search_bot.sites.sent_submission import SentSubmission
from fa_search_bot.sites.submission_id import SubmissionID


cache_save_calls = Counter(
    "fasearchbot_submissioncache_save_entry_calls",
    "Number of times a submission is saved to cache",
    labelnames=["site_code"],
)
cache_load_calls = Counter(
    "fasearchbot_submissioncache_load_entry_calls",
    "Number of times a submission is attempted to load from cache",
    labelnames=["result", "site_code"],
)
cache_save_timer = Summary(
    "fasearchbot_submissioncache_save_time_taken",
    "Amount of time taken (in seconds) to save submissions to the cache",
)
cache_load_timer = Summary(
    "fasearchbot_submissioncache_load_time_taken",
    "Amount of time taken (in seconds) to load submissions from the cache",
    labelnames=["result"],
)


def now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


@dataclasses.dataclass
class CacheEntry:
    sent_submission: SentSubmission
    cache_date: datetime.datetime


class SubmissionCache:
    LABEL_RESULT_MISS = "miss"
    LABEL_RESULT_MISS_FULL = "miss_full"
    LABEL_RESULT_HIT = "hit"

    def __init__(self, db: Database) -> None:
        self.db = db

    def initialise_metrics(self, handlers: HandlerGroup) -> None:
        for site_code in handlers.site_codes():
            cache_save_calls.labels(site_code=site_code)
            for result in [self.LABEL_RESULT_MISS, self.LABEL_RESULT_MISS_FULL, self.LABEL_RESULT_HIT]:
                cache_load_calls.labels(site_code=site_code, result=result)
                cache_load_timer.labels(result=result)

    def save_cache(self, sent_submission: Optional[SentSubmission]) -> None:
        if sent_submission is None:
            return
        if not sent_submission.save_cache:
            return
        with cache_save_timer.time():
            cache_entry = DBCacheEntry(
                sent_submission.sub_id.site_code,
                str(sent_submission.sub_id.submission_id),
                sent_submission.is_photo,
                sent_submission.media_id,
                sent_submission.access_hash,
                sent_submission.file_url,
                sent_submission.caption,
                now(),
                sent_submission.full_image,
            )
            cache_save_calls.labels(site_code=sent_submission.sub_id.site_code).inc()
            self.db.save_cache_entry(cache_entry)

    def load_cache(self, sub_id: SubmissionID, *, allow_inline: bool = False) -> Optional[SentSubmission]:
        # Create the label vals dict here, and then we can update it later when we know the result
        # As long as we ensure we update it before we leave the function, we're fine
        label_vals = {}
        with Timer(lambda x: cache_load_timer.labels(**label_vals).observe(x)):
            # Fetch the cache entry from the database
            entry = self.db.fetch_cache_entry(sub_id.site_code, str(sub_id.submission_id))
            if entry is None:
                cache_load_calls.labels(site_code=sub_id.site_code, result=self.LABEL_RESULT_MISS).inc()
                label_vals["result"] = self.LABEL_RESULT_MISS
                return None
            # Unless this is for inline use, only return full image results
            if not allow_inline and not entry.full_image:
                cache_load_calls.labels(site_code=sub_id.site_code, result=self.LABEL_RESULT_MISS_FULL).inc()
                label_vals["result"] = self.LABEL_RESULT_MISS_FULL
                return None
            cache_load_calls.labels(site_code=sub_id.site_code, result=self.LABEL_RESULT_HIT).inc()
            label_vals["result"] = self.LABEL_RESULT_HIT
            return SentSubmission(
                SubmissionID(entry.site_code, entry.submission_id),
                entry.is_photo,
                entry.media_id,
                entry.access_hash,
                entry.file_url,
                entry.caption,
                entry.full_image
            )
