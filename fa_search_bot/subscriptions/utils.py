from typing import List, Optional

from prometheus_client import Summary
from prometheus_client.context_managers import Timer

from fa_search_bot.sites.furaffinity.fa_submission import FASubmission

time_taken = Summary(
    "fasearchbot_fasubwatcher_time_taken",
    "Amount of time taken (in seconds) doing various tasks of the subscription watcher",
    labelnames=["runnable", "task", "task_type"],
)


def _latest_submission_in_list(submissions: List[FASubmission]) -> Optional[FASubmission]:
    if not submissions:
        return None
    return max(submissions, key=lambda sub: int(sub.submission_id))


class TimeKeeper:
    def __init__(self, recorder: Summary) -> None:
        self.recorder = recorder
        self.duration = None

    def callback(self, duration: float) -> None:
        self.recorder.observe(duration)
        self.duration = duration

    def time(self) -> Timer:
        return Timer(self.callback)
