import datetime

import pytest

from fa_search_bot.sites.furaffinity.fa_submission import FASubmissionFull, FAUser
from fa_search_bot.sites.submission import Rating
from fa_search_bot.tests.util.submission_builder import SubmissionBuilder


def test_constructor():
    post_id = "1234"
    image_id = "5324543"
    link = f"https://furaffinity.net/view/{post_id}/"
    thumb_link = f"https://t.furaffinity.net/{post_id}@400-{image_id}.jpg"
    full_link = f"https://d.furaffinity.net/art/fender/{image_id}/{image_id}.fender_blah-de-blah.jpg"
    title = "Example post"
    author = FAUser.from_short_dict({"name": "John", "profile_name": "john"})
    description = "This is an example post for testing"
    keywords = ["example", "test"]
    rating = Rating.GENERAL
    posted_at = datetime.datetime.now(datetime.timezone.utc)

    submission = FASubmissionFull(
        post_id,
        thumb_link,
        full_link,
        full_link,
        title,
        author,
        description,
        keywords,
        rating,
        posted_at,
    )

    assert isinstance(submission, FASubmissionFull)
    assert submission.submission_id == post_id
    assert submission.link == link
    assert submission.thumbnail_url == thumb_link
    assert submission.full_image_url == full_link
    assert submission.download_url == full_link
    assert submission.title == title
    assert submission.author == author
    assert submission.description == description
    assert submission.keywords == keywords
    assert submission.rating == rating


@pytest.mark.asyncio
async def test_download_file_size(requests_mock):
    submission = SubmissionBuilder().build_full_submission()
    size = 23124
    requests_mock.head(submission.full_image_url, headers={"content-length": str(size)})

    file_size = await submission.download_file_size()

    assert isinstance(file_size, int)
    assert file_size == size

    requests_mock.head(submission.full_image_url, status_code=404)

    file_size2 = await submission.download_file_size()

    assert isinstance(file_size2, int)
    assert file_size2 == size


def test_download_file_ext():
    submission = SubmissionBuilder(file_ext="JPEG").build_full_submission()

    ext = submission.download_file_ext

    assert ext == "jpeg"
