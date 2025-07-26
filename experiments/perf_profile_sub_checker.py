import asyncio
import json
import logging
import pathlib
import sys

from prometheus_client.metrics import Summary

from fa_search_bot.config import SubscriptionWatcherConfig
from fa_search_bot.sites.furaffinity.fa_export_api import FAExportAPI
from fa_search_bot.subscriptions.query_target import QueryTarget
from fa_search_bot.subscriptions.query_parser import Query
from fa_search_bot.subscriptions.subscription_watcher import SubscriptionWatcher
from fa_search_bot.subscriptions.utils import TimeKeeper

#####
# This experiment is looking at performance profiling of the subscriptions checker against real world data
#####

logger = logging.getLogger(__name__)
time_taken = Summary(
    "fasearchbot_experiment_perf_time_taken",
    "Experiment: Not to be used or collected, just using it to power TimeKeeper",
)


def setup_logging(log_level: str = "INFO") -> None:
    formatter = logging.Formatter("{asctime}:{levelname}:{name}:{message}", style="{")

    base_logger = logging.getLogger()
    base_logger.setLevel(log_level.upper())
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    base_logger.addHandler(console_handler)


async def load_submission_cache(fa_api: FAExportAPI) -> list[QueryTarget]:
    cache_file = pathlib.Path(__file__).parent / "perf_profile_submission_data.json"
    try:
        with open(cache_file, "r") as f:
            data = json.loads(f.read())
            subs = [QueryTarget.from_json(sub) for sub in data["submissions"]]
            return subs
    except json.decoder.JSONDecodeError:
        pass
    except FileNotFoundError:
        pass
    browse_page = await fa_api.get_browse_page()
    query_targets = []
    for sub_short in browse_page:
        # noinspection PyBroadException
        try:
            sub = await fa_api.get_full_submission(sub_short.submission_id)
            query_targets.append(sub.to_query_target())
        except Exception as e:
            pass
    with open(cache_file, "w") as f:
        # noinspection PyTypeChecker
        json.dump({"submissions": [qt.to_json() for qt in query_targets]}, f, indent=2)
    return query_targets


async def perf_test(sub_watcher: SubscriptionWatcher, submissions: list[QueryTarget]) -> None:
    for submission in submissions:
        logger.info("Checking submission titled: %s", submission.title)
        matches = await sub_watcher.check_subscriptions(submission)
        logger.info("Submission matches %s subscriptions!!", len(matches))


def query_complexity(query: Query) -> float:
    return repr(query).count("(")


def most_complex_queries(queries: list[Query]) -> list[Query]:
    return sorted(queries, key=query_complexity, reverse=True)


def list_queries(sub_watcher: SubscriptionWatcher) -> list[Query]:
    queries = []
    for subscription in sub_watcher.subscriptions:
        queries.append(subscription.query)
    for dest_blocklist in sub_watcher.blocklists.values():
        for block in dest_blocklist.blocklists.values():
            queries.append(block)
    return queries


def load_sub_watcher(fa_api: FAExportAPI) -> SubscriptionWatcher:
    sub_watcher_config = SubscriptionWatcherConfig(False, 0, 0, 0, 0)
    subscriptions_file_name = "perf_profile_subscriptions.json"
    SubscriptionWatcher.FILENAME = subscriptions_file_name
    SubscriptionWatcher.FILENAME_TEMP = f"{subscriptions_file_name}.temp.json"
    # noinspection PyTypeChecker
    sub_watcher = SubscriptionWatcher.load_from_json(sub_watcher_config, fa_api, None, None)
    if len(sub_watcher.subscriptions) == 0:
        raise ValueError(f"Failed to load subscriptions, place a subscriptions.json file at {subscriptions_file_name}")
    return sub_watcher


async def main():
    collate_most_complex = False
    run_perf_test = True
    # Set everything up
    setup_logging(log_level="DEBUG")
    fa_api = FAExportAPI("https://faexport.spangle.org.uk")
    try:
        sub_watcher = load_sub_watcher(fa_api)
        print(f"There are {len(sub_watcher.subscriptions)} subscriptions")
        # Collate most complex queries
        if collate_most_complex:
            most_complex = most_complex_queries(list_queries(sub_watcher))
            print("Top 10 most complex queries:")
            for num, query in enumerate(most_complex[:10]):
                print(f"{num}: ({query_complexity(query)}) {query!r}")
            return
        # Load submissions
        submissions = await load_submission_cache(fa_api)
        print(f"There are {len(submissions)} submissions")
        # Run perf test
        if run_perf_test:
            perf_time = TimeKeeper(time_taken)
            with perf_time.time():
                await perf_test(sub_watcher, submissions)
            print(f"Perf test took: {perf_time.duration} seconds")
    finally:
        await fa_api.close()


if __name__ == "__main__":
    asyncio.run(main())
