from __future__ import annotations

import dataclasses
import json
from typing import Optional

DEFAULT_NUM_DATA_FETCHERS = 2
DEFAULT_NUM_MEDIA_DOWNLOADERS = 2
DEFAULT_NUM_MEDIA_UPLOADERS = 1
DEFAULT_MAX_READY_FOR_UPLOAD = 100    # Maximum number of submissions which should be ready for media upload, to prevent data being too stale by the time it comes to upload, especially if catching up on backlog
DEFAULT_FETCH_REFRESH_LIMIT = 25


@dataclasses.dataclass
class TelegramConfig:
    api_id: int
    api_hash: str
    bot_token: str

    @classmethod
    def from_dict(cls, conf: dict) -> "TelegramConfig":
        return cls(conf["telegram_api_id"], conf["telegram_api_hash"], conf["bot_key"])


@dataclasses.dataclass
class E621Config:
    username: str
    api_key: str

    @classmethod
    def from_dict(cls, conf: dict) -> "E621Config":
        return cls(conf["username"], conf["api_key"])


@dataclasses.dataclass
class WeasylConfig:
    api_key: str

    @classmethod
    def from_dict(cls, conf: dict) -> "WeasylConfig":
        return cls(conf["api_key"])


@dataclasses.dataclass
class SubscriptionWatcherConfig:
    enabled: bool
    num_data_fetchers: int
    num_media_downloaders: int
    num_media_uploaders: int
    max_ready_for_upload: int
    fetch_refresh_limit: int

    def total_num_task_runners(self) -> int:
        return self.num_data_fetchers + self.num_media_downloaders + self.num_media_uploaders

    @classmethod
    def from_dict(cls, conf: dict) -> "SubscriptionWatcherConfig":
        return cls(
            enabled=conf.get("enabled", True),
            num_data_fetchers=conf.get("num_data_fetchers", DEFAULT_NUM_DATA_FETCHERS),
            num_media_downloaders=conf.get("num_media_downloaders", DEFAULT_NUM_MEDIA_DOWNLOADERS),
            num_media_uploaders=conf.get("num_media_uploaders", DEFAULT_NUM_MEDIA_UPLOADERS),
            max_ready_for_upload=conf.get("max_ready_for_upload", DEFAULT_MAX_READY_FOR_UPLOAD),
            fetch_refresh_limit=conf.get("fetch_refresh_limit", DEFAULT_FETCH_REFRESH_LIMIT),
        )


@dataclasses.dataclass
class Config:
    fa_api_url: str
    telegram: TelegramConfig
    e621: E621Config
    weasyl: Optional[WeasylConfig]
    subscription_watcher: SubscriptionWatcherConfig
    prometheus_port: Optional[int]

    @classmethod
    def from_dict(cls, conf: dict) -> "Config":
        weasyl_data = conf.get("weasyl")
        weasyl_config = None
        if weasyl_data:
            weasyl_config = WeasylConfig.from_dict(weasyl_data)
        return cls(
            conf["api_url"],
            TelegramConfig.from_dict(conf),
            E621Config.from_dict(conf["e621"]),
            weasyl_config,
            SubscriptionWatcherConfig.from_dict(conf.get("subscription_watcher", {})),
            conf.get("prometheus_port", 7065),
        )

    @classmethod
    def load_from_file(cls, file_name: str) -> "Config":
        with open(file_name, "r") as f:
            return cls.from_dict(json.load(f))
