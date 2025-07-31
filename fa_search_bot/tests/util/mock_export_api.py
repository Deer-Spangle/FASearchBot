from __future__ import annotations

import datetime
import random
import string
from typing import TYPE_CHECKING

from fa_search_bot.sites.furaffinity.fa_export_api import FAExportAPI, PageNotFound
from fa_search_bot.sites.furaffinity.fa_submission import FASubmissionFull, FAUser
from fa_search_bot.sites.submission import Rating

if TYPE_CHECKING:
    from typing import List, Union

    from fa_search_bot.sites.furaffinity.fa_submission import FASubmission


def _random_image_id(submission_id: int) -> int:
    return int(submission_id * 48.5 + random.randint(0, 13))


def _random_string() -> str:
    return "".join(random.choice(string.ascii_lowercase) for _ in range(random.randint(5, 20)))


def _random_rating() -> Rating:
    return random.choice([Rating.GENERAL, Rating.MATURE, Rating.ADULT])


class MockSubmission(FASubmissionFull):
    EXTENSIONS_STORY = ["doc", "docx", "pdf", "rtf", "txt", "odt"]
    EXTENSIONS_MUSIC = ["mp3", "mid", "wav", "mpeg"]
    EXTENSIONS_ART = ["gif", "jpg", "jpeg", "png"]

    def __init__(
        self,
        submission_id: Union[str, int],
        *,
        username: str = None,
        image_id: int = None,
        file_size: int = 14852,
        file_ext: str = "jpg",
        fav_id: str = None,
        title: str = None,
        author: FAUser = None,
        description: str = None,
        keywords: List[str] = None,
        thumbnail_url: str = None,
        download_url: str = None,
        full_image_url: str = None,
        rating: Rating = None,
        posted_at: datetime.datetime = None,
    ):
        # Internal variables
        if image_id is None:
            image_id = _random_image_id(int(submission_id))
        if username is None:
            username = _random_string()
        if fav_id is None:
            fav_id = str(_random_image_id(int(submission_id)))
        folder = ""
        if file_ext in self.EXTENSIONS_MUSIC:
            folder = "music/"
        if file_ext in self.EXTENSIONS_STORY:
            folder = "stories/"
        # Variables for superclass
        if thumbnail_url is None:
            thumbnail_url = f"https://t.furaffinity.net/{submission_id}@1600-{image_id}.jpg"
        if download_url is None:
            download_url = (
                f"https://d.furaffinity.net/art/{username}/{folder}{image_id}/"
                f"{image_id}.{username}_{_random_string()}.{file_ext}"
            )
        if full_image_url is None:
            if file_ext in self.EXTENSIONS_ART:
                full_image_url = download_url
            else:
                full_image_url = download_url + ".jpg"
        if title is None:
            title = _random_string()
        if author is None:
            profile_name = _random_string()
            author = FAUser.from_submission_dict({"name": profile_name.title(), "profile_name": profile_name})
        if description is None:
            description = _random_string() * 5
        if keywords is None:
            keywords = [_random_string() for _ in range(3)]
        if rating is None:
            rating = _random_rating()
        if posted_at is None:
            _age = datetime.timedelta(seconds=random.randint(0, 1_000_000))
            posted_at = datetime.datetime.now(datetime.timezone.utc) - _age
        # Super
        super().__init__(
            str(submission_id),
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
        self.fav_id = fav_id
        self._download_file_size = file_size

    @property
    def download_file_size(self) -> int:
        return self._download_file_size

    def __repr__(self):
        return f"MockSubmission(id={self.submission_id}, ...)"


class MockExportAPI(FAExportAPI):
    def __init__(self):
        self.browse_count = 0
        self.call_after_x_browse = (lambda: None, -1)
        super().__init__("--")
        self.submissions = {}
        self.user_folders = {}
        self.search_results = {}
        self.browse_results = {}

    def with_submission(self, submission: MockSubmission) -> "MockExportAPI":
        self.submissions[submission.submission_id] = submission
        return self

    def with_submissions(self, list_submissions: List[MockSubmission]) -> "MockExportAPI":
        for submission in list_submissions:
            self.with_submission(submission)
        return self

    def with_user_folder(
        self,
        username: str,
        folder: str,
        list_submissions: List[MockSubmission],
        page: int = 1,
    ) -> "MockExportAPI":
        if username not in self.user_folders:
            self.user_folders[username] = {}
        self.user_folders[username][f"{folder}:{page}"] = list_submissions
        self.with_submissions(list_submissions)
        return self

    def with_user_favs(
        self, username: str, list_submissions: List[MockSubmission], next_id: str = None
    ) -> "MockExportAPI":
        if username not in self.user_folders:
            self.user_folders[username] = {}
        self.user_folders[username][f"favs:{next_id}"] = list_submissions
        self.with_submissions(list_submissions)
        return self

    def with_search_results(self, query: str, list_submissions: List[MockSubmission], page: int = 1):
        self.search_results[f"{query.lower()}:{page}"] = list_submissions
        self.with_submissions(list_submissions)
        return self

    def with_browse_results(self, list_submissions: List[MockSubmission], page: int = 1):
        self.browse_results[page] = list_submissions
        self.with_submissions(list_submissions)
        return self

    async def get_full_submission(self, submission_id: str) -> FASubmission:
        if submission_id not in self.submissions:
            raise PageNotFound(f"Submission not found with ID: {submission_id}")
        return self.submissions[submission_id]

    async def get_user_folder(self, user: str, folder: str, page: int = 1) -> List[FASubmission]:
        if user not in self.user_folders:
            return []
        if f"{folder}:{page}" not in self.user_folders[user]:
            return []
        return self.user_folders[user][f"{folder}:{page}"][:]

    async def get_user_favs(self, user: str, next_id: int = 1) -> List[FASubmission]:
        if user not in self.user_folders:
            return []
        if f"favs:{next_id}" not in self.user_folders[user]:
            return []
        return self.user_folders[user][f"favs:{next_id}"][:]

    async def get_search_results(self, query: str, page: int = 1) -> List[FASubmission]:
        if f"{query.lower()}:{page}" not in self.search_results:
            return []
        return self.search_results[f"{query.lower()}:{page}"][:]

    async def get_browse_page(self, page: int = 1) -> List[FASubmission]:
        self.browse_count += 1
        if self.browse_count >= self.call_after_x_browse[1]:
            self.call_after_x_browse[0]()
        if page not in self.browse_results:
            return []
        return self.browse_results[page][:]
