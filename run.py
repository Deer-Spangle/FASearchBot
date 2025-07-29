import asyncio
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

import click
from prometheus_client import Counter

from fa_search_bot.bot import FASearchBot
from fa_search_bot.config import Config, DEFAULT_MAX_READY_FOR_UPLOAD, DEFAULT_NUM_DATA_FETCHERS, \
    DEFAULT_NUM_MEDIA_DOWNLOADERS, DEFAULT_NUM_MEDIA_UPLOADERS, DEFAULT_FETCH_REFRESH_LIMIT

log_entries = Counter(
    "fasearchbot_log_messages_total",
    "Number of log messages by logger and level",
    labelnames=["logger", "level"]
)


class LogMetricsHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        log_entries.labels(logger=record.name, level=record.levelname).inc()


def setup_logging(log_level: str = "INFO") -> None:
    os.makedirs("logs", exist_ok=True)
    formatter = logging.Formatter("{asctime}:{levelname}:{name}:{message}", style="{")

    base_logger = logging.getLogger()
    base_logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    base_logger.addHandler(console_handler)

    # FA search bot log, for diagnosing the bot. Should not contain user information.
    fa_logger = logging.getLogger("fa_search_bot")
    file_handler = TimedRotatingFileHandler("logs/fa_search_bot.log", when="midnight")
    file_handler.setFormatter(formatter)
    fa_logger.addHandler(file_handler)
    fa_logger.setLevel(log_level.upper())
    fa_logger.addHandler(LogMetricsHandler())


@click.command()
@click.option("--log-level", type=str, help="Log level for the logger", default="INFO")
@click.option("--no-subscriptions", type=bool, default=False, help="Disable subscription watcher")
@click.option("--sub-watcher-data-fetchers", type=int, default=DEFAULT_NUM_DATA_FETCHERS, help="Number of DataFetcher tasks which should spin up in the subscription watcher")
@click.option("--sub-watcher-media-downloaders", type=int, default=DEFAULT_NUM_MEDIA_DOWNLOADERS, help="Number of MediaDownloader tasks which should spin up in the subscription watcher")
@click.option("--sub-watcher-media-uploaders", type=int, default=DEFAULT_NUM_MEDIA_UPLOADERS, help="Number of MediaUploader tasks which should spin up in the subscription watcher")
@click.option("--sub-watcher-max-ready-for-upload", type=int, default=DEFAULT_MAX_READY_FOR_UPLOAD, help="Maximum number of submissions which should have data and media fetched before being uploaded to Telegram, to prevent data being too stale by the time it comes to upload, especially if catching up on backlog")
@click.option("--fetch-max-data-refresh", type=int, default=DEFAULT_FETCH_REFRESH_LIMIT, help="How many times a submission should get pushed back for data refresh before giving up and declaring the submission media to be broken")
def main(
        log_level: str,
        no_subscriptions: bool,
        sub_watcher_data_fetchers: int,
        sub_watcher_media_downloaders: int,
        sub_watcher_media_uploaders: int,
        sub_watcher_max_ready_for_upload: int,
        fetch_max_data_refresh: int,
) -> None:
    setup_logging(log_level)
    # Construct config and ingest flags
    config = Config.load_from_file(os.getenv('CONFIG_FILE', 'config.json'))
    config.subscription_watcher.enabled = not no_subscriptions
    config.subscription_watcher.num_data_fetchers = sub_watcher_data_fetchers
    config.subscription_watcher.num_media_downloaders = sub_watcher_media_downloaders
    config.subscription_watcher.num_media_uploaders = sub_watcher_media_uploaders
    config.subscription_watcher.max_ready_for_upload = sub_watcher_max_ready_for_upload
    config.subscription_watcher.fetch_refresh_limit = fetch_max_data_refresh
    # Create and start the bot
    bot = FASearchBot(config)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(bot.run())


if __name__ == "__main__":
    main()
