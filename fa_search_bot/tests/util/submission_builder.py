from __future__ import annotations

import datetime
import random
from typing import TYPE_CHECKING

from fa_search_bot.sites.furaffinity.fa_submission import FASubmissionFull, FASubmissionShort, FAUser
from fa_search_bot.sites.submission import Rating
from fa_search_bot.tests.util.mock_export_api import MockSubmission, _random_image_id, _random_string

if TYPE_CHECKING:
    from typing import List, Union


class SubmissionBuilder:
    def __init__(
        self,
        *,
        submission_id: Union[str, int] = None,
        username: str = None,
        image_id: int = None,
        file_size: int = None,
        thumb_size: int = None,
        file_ext: str = "jpg",
        fav_id: str = None,
        title: str = None,
        author: FAUser = None,
        description: str = None,
        keywords: List[str] = None,
        rating: Rating = None,
        posted_at: datetime.datetime = None,
    ):
        if submission_id is None:
            submission_id = random.randint(10_000, 100_000)
        submission_id = str(submission_id)
        # Internal variables
        if image_id is None:
            image_id = _random_image_id(int(submission_id))
        if username is None:
            if author is None:
                username = _random_string()
            else:
                username = author.profile_name
        folder = ""
        if file_ext in MockSubmission.EXTENSIONS_MUSIC:
            folder = "music/"
        if file_ext in MockSubmission.EXTENSIONS_STORY:
            folder = "stories/"
        if thumb_size is None:
            thumb_size = 1600
        # Variables for superclass
        thumbnail_url = f"https://t.furaffinity.net/{submission_id}@{thumb_size}-{image_id}.jpg"
        download_url = (
            f"https://d.furaffinity.net/art/{username}/{folder}{image_id}/"
            f"{image_id}.{username}_{_random_string()}.{file_ext}"
        )
        if file_ext in MockSubmission.EXTENSIONS_ART:
            full_image_url = download_url
        else:
            full_image_url = download_url + ".jpg"
        if title is None:
            title = _random_string()
        if author is None:
            author = FAUser.from_submission_dict({"name": username.title(), "profile_name": username})
        if description is None:
            description = _random_string() * 5
        if keywords is None:
            keywords = [_random_string() for _ in range(3)]
        if rating is None:
            rating = Rating.GENERAL
        if posted_at is None:
            _age = datetime.timedelta(seconds=random.randint(0, 1_000_000))
            posted_at = datetime.datetime.now(tz=datetime.timezone.utc) - _age
        # Set all the variables
        self.submission_id = submission_id
        self.link = f"https://furaffinity.net/view/{submission_id}/"
        self.thumbnail_url = thumbnail_url
        self.title = title
        self.author = author
        self.download_url = download_url
        self.full_image_url = full_image_url
        self.description = description
        self.rating = rating
        self.keywords = keywords
        self.fav_id = fav_id
        self.posted_at = posted_at
        self._image_id = image_id
        self._file_ext = file_ext
        self._download_file_size = file_size

    def build_full_submission(self):
        sub = FASubmissionFull(
            self.submission_id,
            self.thumbnail_url,
            self.download_url,
            self.full_image_url,
            self.title,
            self.author,
            self.description,
            self.keywords,
            self.rating,
            self.posted_at,
        )
        sub._download_file_size = self._download_file_size
        return sub

    def build_short_submission(self):
        sub = FASubmissionShort(self.submission_id, self.thumbnail_url, self.title, self.author)
        return sub

    def build_mock_submission(self):
        sub = MockSubmission(
            self.submission_id,
            username=self.author.profile_name,
            image_id=self._image_id,
            file_size=14852 if self._download_file_size is None else self._download_file_size,
            file_ext=self._file_ext,
            fav_id=self.fav_id,
            title=self.title,
            author=self.author,
            description=self.description,
            keywords=self.keywords,
            thumbnail_url=self.thumbnail_url,
            download_url=self.download_url,
            full_image_url=self.full_image_url,
        )
        return sub

    def build_submission_json(self):
        return {
            "title": self.title,
            "description": self.description,
            "description_body": self.description,
            "name": self.author.name,
            "profile": self.author.link,
            "profile_name": self.author.profile_name,
            "link": f"https://www.furaffinity.net/view/{self.submission_id}/",
            "download": self.download_url,
            "full": self.full_image_url,
            "thumbnail": self.thumbnail_url,
            "rating": {
                Rating.GENERAL: "General",
                Rating.MATURE: "Mature",
                Rating.ADULT: "Adult",
            }[self.rating],
            "keywords": self.keywords,
            "posted_at": self.posted_at.isoformat(),
        }

    def build_search_json(self):
        return {
            "id": self.submission_id,
            "title": self.title,
            "thumbnail": self.thumbnail_url,
            "link": f"https://www.furaffinity.net/view/{self.submission_id}/",
            "name": self.author.name,
            "profile": self.author.link,
            "profile_name": self.author.profile_name,
        }

    def build_fav_json(self):
        data = self.build_search_json()
        if self.fav_id:
            data["fav_id"] = self.fav_id
        return data
