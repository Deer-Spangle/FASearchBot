from __future__ import annotations

import logging
import re
from abc import ABC
from typing import TYPE_CHECKING, TypedDict, Dict

import aiohttp
import dateutil.parser
from telethon import Button

from fa_search_bot.sites.submission import Rating
from fa_search_bot.sites.submission_id import SubmissionID
from fa_search_bot.subscriptions.query_target import QueryTarget

if TYPE_CHECKING:
    import datetime
    from typing import Awaitable, List, Optional, Union

    from telethon.tl.custom import InlineBuilder
    from telethon.tl.types import InputBotInlineResultPhoto


logger = logging.getLogger(__name__)

UserShortResp = TypedDict(
    "UserShortResp",
    {
        "name": str,
        "profile_name": str,
    },
)


class SubmissionShortResp(TypedDict, UserShortResp):
    id: str
    title: str
    thumbnail: str


class SubmissionFavShortResp(TypedDict, SubmissionShortResp):
    fav_id: str


class SubmissionResp(TypedDict, UserShortResp):
    link: str
    download: str
    full: str
    thumbnail: Optional[str]
    title: str
    description_body: str
    keywords: List[str]
    rating: str
    posted_at: str


class OnlineStatusResp(TypedDict):
    guests: int
    registered: int
    other: int
    total: int


class StatusResp(TypedDict):
    online: OnlineStatusResp
    fa_server_time_at: str


class FAUser(ABC):
    def __init__(self, name: str, profile_name: str):
        self.name = name
        self.profile_name = profile_name
        self.link = f"https://furaffinity.net/user/{profile_name}/"

    @staticmethod
    def from_short_dict(short_dict: UserShortResp) -> Union["FAUserShort"]:
        return FAUser.from_submission_dict(short_dict)

    @staticmethod
    def from_submission_dict(short_dict: UserShortResp) -> Union["FAUserShort"]:
        name = short_dict["name"]
        profile_name = short_dict["profile_name"]
        new_user = FAUserShort(name, profile_name)
        return new_user


class FAUserShort(FAUser):
    def __init__(self, name: str, profile_name: str):
        super().__init__(name, profile_name)


class FASubmission(ABC):
    def __init__(self, submission_id: str) -> None:
        self.submission_id = submission_id
        self.link = f"https://furaffinity.net/view/{submission_id}/"

    @staticmethod
    def from_short_dict(short_dict: SubmissionShortResp) -> "FASubmissionShort":
        submission_id = short_dict["id"]
        thumbnail_url = FASubmission.make_thumbnail_bigger(short_dict["thumbnail"])
        title = short_dict["title"]
        author = FAUser.from_short_dict(short_dict)
        return FASubmissionShort(submission_id, thumbnail_url, title, author)

    @staticmethod
    def from_short_fav_dict(short_dict: SubmissionFavShortResp) -> "FASubmissionShortFav":
        submission_id = short_dict["id"]
        thumbnail_url = FASubmission.make_thumbnail_bigger(short_dict["thumbnail"])
        title = short_dict["title"]
        author = FAUser.from_short_dict(short_dict)
        return FASubmissionShortFav(submission_id, thumbnail_url, title, author, short_dict["fav_id"])

    @staticmethod
    def from_full_dict(full_dict: SubmissionResp) -> "FASubmissionFull":
        full_link = full_dict["link"]
        submission_id = FASubmission.id_from_link(full_link)
        download_url = full_dict["download"]
        full_image_url = full_dict["full"]
        if full_dict["thumbnail"] is None:
            thumbnail_url = FASubmission.construct_thumbnail_url(submission_id, download_url)
        else:
            thumbnail_url = FASubmission.make_thumbnail_bigger(full_dict["thumbnail"])
        title = full_dict["title"]
        description = full_dict["description_body"]
        author = FAUser.from_submission_dict(full_dict)
        keywords: List[str] = full_dict["keywords"]
        rating = {
            "Adult": Rating.ADULT,
            "Mature": Rating.MATURE,
            "General": Rating.GENERAL,
        }[full_dict["rating"]]
        posted_at = dateutil.parser.parse(full_dict["posted_at"])
        new_submission = FASubmissionFull(
            submission_id,
            thumbnail_url,
            download_url,
            full_image_url,
            title,
            author,
            description,
            keywords,
            rating,
            posted_at,
        )
        return new_submission

    @staticmethod
    def make_thumbnail_bigger(thumbnail_url: str) -> str:
        return re.sub("@[0-9]+-", "@1600-", thumbnail_url).replace("facdn", "furaffinity")

    @staticmethod
    def construct_thumbnail_url(submission_id: str, download_url: str) -> str:
        # TODO: reuse regex between here and neaten functionality
        direct_link_regex = re.compile(
            r"d2?\.(?:facdn|furaffinity)\.net/art/([^/]+)/(?:|stories/|poetry/|music/)([0-9]+)/",
            re.I,
        )
        sub_match = direct_link_regex.search(download_url)
        if not sub_match:
            raise ValueError("This is not a valid download URL")
        sub_timestamp = sub_match.group(2)
        return f"https://t.furaffinity.net/{submission_id}@1600-{sub_timestamp}.jpg"

    @staticmethod
    def make_thumbnail_smaller(thumbnail_url: str) -> str:
        return re.sub("@[0-9]+-", "@300-", thumbnail_url)

    @staticmethod
    def id_from_link(link: str) -> str:
        id_match = re.search("view/([0-9]+)", link)
        if not id_match:
            raise ValueError("Link does not seem to have a valid ID")
        return id_match.group(1)

    @staticmethod
    async def _get_file_size(url: str) -> int:
        async with aiohttp.ClientSession() as session:
            async with session.head(url) as resp:
                return int(resp.headers.get("content-length", 0))


class FASubmissionShort(FASubmission):
    def __init__(self, submission_id: str, thumbnail_url: str, title: str, author: FAUser) -> None:
        super().__init__(submission_id)
        self.thumbnail_url = thumbnail_url
        self.title = title
        self.author = author

    def to_inline_query_result(
        self, builder: InlineBuilder, site_code: Optional[str] = None
    ) -> Awaitable[InputBotInlineResultPhoto]:
        inline_id = f"{self.submission_id}"
        if site_code:
            inline_id = f"{site_code}:{self.submission_id}"
        return builder.photo(
            file=self.thumbnail_url,
            id=inline_id,
            text=self.link,
            # Button is required such that the bot can get a callback with the message id, and edit it later.
            buttons=[Button.inline("⏳ Optimising", f"neaten_me:{inline_id}")],
        )


class FASubmissionShortFav(FASubmissionShort):
    def __init__(
        self,
        submission_id: str,
        thumbnail_url: str,
        title: str,
        author: FAUser,
        fav_id: str,
    ) -> None:
        super().__init__(submission_id, thumbnail_url, title, author)
        self.fav_id = fav_id


class FASubmissionFull(FASubmissionShort):
    def __init__(
        self,
        submission_id: str,
        thumbnail_url: str,
        download_url: str,
        full_image_url: str,
        title: str,
        author: FAUser,
        description: str,
        keywords: List[str],
        rating: Rating,
        posted_at: datetime.datetime,
    ) -> None:
        super().__init__(submission_id, thumbnail_url, title, author)
        self.download_url = download_url
        self.full_image_url = full_image_url
        self.description = description
        self.keywords = keywords
        self.rating = rating
        self.posted_at = posted_at
        self._download_file_size: Optional[int] = None

    async def download_file_size(self) -> int:
        if self._download_file_size is None:
            self._download_file_size = await FASubmission._get_file_size(self.download_url)
        return self._download_file_size

    @property
    def download_file_ext(self) -> str:
        return self.download_url.split(".")[-1].lower()

    def to_query_target(self) -> QueryTarget:
        return QueryTarget(
            sub_id=SubmissionID("fa", self.submission_id),
            title=[self.title],
            keywords=self.keywords,
            description=[self.description],
            artist=[self.author.name, self.author.profile_name],
            rating=self.rating,
        )


class FAStatus:
    def __init__(
        self,
        online_guests: int,
        online_registered: int,
        online_other: int,
        online_total: int,
        server_time: datetime.datetime,
    ):
        self.online_guests = online_guests
        self.online_registered = online_registered
        self.online_other = online_other
        self.online_total = online_total
        self.server_time = server_time

    @classmethod
    def from_dict(cls, status_dict: StatusResp) -> "FAStatus":
        return FAStatus(
            status_dict["online"]["guests"],
            status_dict["online"]["registered"],
            status_dict["online"]["other"],
            status_dict["online"]["total"],
            dateutil.parser.parse(status_dict["fa_server_time_at"]),
        )


class FAHomeCategory:
    def __init__(self, name: str, submissions: List[FASubmissionShort]):
        self.name = name
        self.submissions = submissions


class FAHomePage:
    def __init__(self, categories: List[FAHomeCategory]):
        self.categories = categories

    def all_submissions(self) -> List[FASubmissionShort]:
        all_subs = []
        for category in self.categories:
            all_subs.extend(category.submissions)
        return all_subs

    @classmethod
    def from_dict(cls, home_dict: Dict[str, List[Dict]]) -> "FAHomePage":
        return cls(
            [
                FAHomeCategory(
                    key,
                    [FASubmission.from_short_dict(sub) for sub in sub_list]
                )
                for key, sub_list in home_dict.items()
            ]
        )
