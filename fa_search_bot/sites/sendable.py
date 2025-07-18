from __future__ import annotations

import asyncio
import dataclasses
import datetime
import enum
import json
import logging
import os
import shutil
import uuid
from abc import ABC, abstractmethod
from asyncio import Lock
from contextlib import contextmanager, asynccontextmanager
from typing import TYPE_CHECKING, Callable, TypeVar, Generator, Tuple, Dict, List, ContextManager, AsyncGenerator

import aiohttp
import docker
from PIL import Image, UnidentifiedImageError, ImageFile
from aiohttp import ClientError, ClientResponseError
from prometheus_client import Counter, Summary
from prometheus_client.metrics import Histogram
from telethon import Button
from telethon.tl.types import (
    InputBotInlineResultPhoto, TypeInputPeer, InputMediaPhotoExternal, InputMediaDocumentExternal, TypeInputMedia,
    InputMediaUploadedPhoto, InputMediaUploadedDocument, DocumentAttributeFilename, DocumentAttributeVideo,
    DocumentAttributeAudio
)

from fa_search_bot.sites.sent_submission import SentSubmission, sent_from_cache

if TYPE_CHECKING:
    from typing import Any, Awaitable, Optional

    from docker import DockerClient
    from docker.models.containers import Container
    from telethon import TelegramClient
    from telethon.tl.custom import InlineBuilder

    from fa_search_bot.sites.handler_group import HandlerGroup
    from fa_search_bot.sites.submission_id import SubmissionID

logger = logging.getLogger(__name__)

sendable_sent = Counter(
    "fasearchbot_sendable_sent_message_total",
    "Number of submissions sent or edited, labelled by site",
    labelnames=["site_code"],
)
sendable_gif_to_png = Counter(
    "fasearchbot_sendable_convert_gif_to_png_total",
    "Number of images which were converted from static gif to png",
    labelnames=["site_code"],
)
sendable_edit = Counter(
    "fasearchbot_sendable_edit_total",
    "Number of submissions which were sent as an edit",
    labelnames=["site_code"],
)
sendable_failure = Counter(
    "fasearchbot_sendable_exception_total",
    "Number of sendable attempts which raised an exception",
    labelnames=["site_code"],
)
sendable_image = Counter(
    "fasearchbot_sendable_image_total",
    "Number of static images which were sent",
    labelnames=["site_code"],
)
sendable_animated = Counter(
    "fasearchbot_sendable_animated_total",
    "Number of animated images and videos which were sent",
    labelnames=["site_code"],
)
sendable_animated_cached = Counter(
    "fasearchbot_sendable_animated_cached_total",
    "Number of animated images and videos which were sent from file cache",
    labelnames=["site_code"],
)
sendable_auto_doc = Counter(
    "fasearchbot_sendable_auto_document_total",
    "Number of documents sent which telegram can automatically handle",
    labelnames=["site_code"],
)
sendable_audio = Counter(
    "fasearchbot_sendable_audio_total",
    "Number of audio files which were sent",
    labelnames=["site_code"],
)
sendable_other = Counter(
    "fasearchbot_sendable_other_files_total",
    "Number of files sent which had no special handling",
    labelnames=["site_code"],
)

convert_video_total = Counter(
    "fasearchbot_convert_video_total",
    "Number of animated images or videos which we tried to process",
    labelnames=["site_code"],
)
convert_video_failures = Counter(
    "fasearchbot_convert_video_exception_total",
    "Number of video conversions which raised an exception",
    labelnames=["site_code"],
)
convert_video_animated = Counter(
    "fasearchbot_convert_video_animated_total",
    "Number of animated images which we tried to convert to a video",
    labelnames=["site_code"],
)
convert_video_no_audio = Counter(
    "fasearchbot_convert_video_no_audio_total",
    "Number of videos without audio which we tried to convert",
    labelnames=["site_code"],
)
convert_video_no_audio_gif = Counter(
    "fasearchbot_convert_video_no_audio_gif_total",
    "Number of videos without audio which we tried to convert to a telegram gif",
    labelnames=["site_code"],
)
convert_video_to_video = Counter(
    "fasearchbot_convert_video_to_video_total",
    "Number of videos which we tried to convert into a video with audio",
    labelnames=["site_code"],
)
convert_video_only_one_attempt = Counter(
    "fasearchbot_convert_video_only_one_attempt_total",
    "Number of videos which we converted with a one-pass conversion attempt",
    labelnames=["site_code"],
)
convert_video_two_pass = Counter(
    "fasearchbot_convert_video_two_pass_total",
    "Number of videos which required a two-pass conversion to fit within telegram limits",
    labelnames=["site_code"],
)

convert_gif_total = Counter(
    "fasearchbot_convert_gif_total",
    "Number of animated images or short silent videos we tried to convert into telegram gifs",
    labelnames=["site_code"],
)
convert_image_failures = Counter(
    "fasearchbot_convert_image_exception_total",
    "Number of telegram image conversions/resize attempts which raised an exception",
    labelnames=["site_code"],
)
thumbnail_video_failures = Counter(
    "fasearchbot_thumbnail_video_exception_total",
    "Number of attempts to get a video thumbnail which raised an exception",
    labelnames=["site_code"],
)
convert_gif_failures = Counter(
    "fasearchbot_convert_gif_exception_total",
    "Number of telegram gif conversions which raised an exception",
    labelnames=["site_code"],
)
convert_gif_only_one_attempt = Counter(
    "fasearchbot_convert_gif_only_one_attempt_total",
    "Number of telegram gifs which required only one-pass conversion",
    labelnames=["site_code"],
)
convert_gif_two_pass = Counter(
    "fasearchbot_convert_gif_two_pass_total",
    "Number of telegram gifs which required two-pass conversion to fit within telegram limits",
    labelnames=["site_code"],
)

video_length = Histogram(
    "fasearchbot_video_length_seconds",
    "Length of the videos processed by the bot, in seconds",
    buckets=[1, 2, 5, 10, 30, 60, 120, 300, 600, 1200, float("inf")],
    labelnames=["site_code"],
)

docker_run_time = Histogram(
    "fasearchbot_docker_runtime_seconds",
    "Time the docker image took to run and return, in seconds",
    buckets=[0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, float("inf")],
    labelnames=["site_code", "entrypoint"],
)
docker_failures = Counter(
    "fasearchbot_docker_failure_total",
    "Number of times an exception was raised while running a docker image",
    labelnames=["site_code", "entrypoint"],
)

inline_results = Counter(
    "fasearchbot_sendable_inline_result_total",
    "Total number of inline results sent",
    labelnames=["site_code"],
)

time_taken = Summary(
    "fasearchbot_sendable_time_taken",
    "Amount of time taken (in seconds) doing various parts of sending a message",
    labelnames=["task"],
)
time_taken_downloading_image = time_taken.labels(task="downloading image")
time_taken_converting_static_gif = time_taken.labels(task="converting static gif to png")
time_taken_uploading_file = time_taken.labels(task="uploading file")
time_taken_editing_message = time_taken.labels(task="editing message")
time_taken_sending_message = time_taken.labels(task="sending message")
time_taken_converting_animated = time_taken.labels(task="converting animated image")
time_taken_converting_video = time_taken.labels(task="converting video")
time_taken_fetching_filesize = time_taken.labels(task="fetching filesize")

SANDBOX_DIR = "sandbox"


@dataclasses.dataclass
class CaptionSettings:
    direct_link: bool = False
    title: bool = False
    author: bool = False
    no_media: bool = False


@dataclasses.dataclass
class SendSettings:
    caption: CaptionSettings
    force_doc: bool = False
    save_cache: bool = True


@dataclasses.dataclass
class UploadedMedia:
    sub_id: SubmissionID
    media: Optional[TypeInputMedia]
    settings: SendSettings


@dataclasses.dataclass
class VideoMetadata:
    raw_data: Dict

    @property
    def audio_streams(self) -> List[Dict]:
        return [stream for stream in self.raw_data.get("streams", []) if stream.get("codec_type") == "audio"]

    @property
    def video_streams(self) -> List[Dict]:
        return [stream for stream in self.raw_data.get("streams", []) if stream.get("codec_type") == "video"]

    @property
    def duration(self) -> Optional[float]:
        duration_str = self.raw_data.get("format", {}).get("duration")
        if duration_str:
            return float(duration_str)
        return None

    @property
    def has_audio(self) -> bool:
        return bool(self.audio_streams)

    @property
    def audio_bitrate(self) -> Optional[int]:
        audio_streams = self.audio_streams
        if audio_streams:
            bit_rate_str = audio_streams[0].get("bit_rate")
            if bit_rate_str:
                return int(bit_rate_str)
        return None

    @property
    def width(self) -> Optional[int]:
        video_streams = self.video_streams
        if video_streams:
            return video_streams[0].get("width")
        return None

    @property
    def height(self) -> Optional[int]:
        video_streams = self.video_streams
        if video_streams:
            return video_streams[0].get("height")
        return None

    @classmethod
    def from_json_str(cls, json_str: str) -> "VideoMetadata":
        json_data = json.loads(json_str)
        return cls(json_data)


def initialise_metrics_labels(handlers: HandlerGroup) -> None:
    for site_code in handlers.site_codes():
        for metric in [
            sendable_sent, sendable_gif_to_png, sendable_edit, sendable_failure, sendable_image, sendable_animated,
            sendable_auto_doc, sendable_audio, sendable_other, sendable_animated_cached, convert_video_total,
            convert_video_failures, convert_video_animated, convert_video_no_audio, convert_video_no_audio_gif,
            convert_video_to_video, convert_video_only_one_attempt, convert_video_two_pass, convert_gif_total,
            convert_gif_failures, convert_gif_only_one_attempt, convert_gif_two_pass, video_length, sent_from_cache,
        ]:
            metric.labels(site_code=site_code)
        for entrypoint in DockerEntrypoint:
            docker_run_time.labels(site_code=site_code, entrypoint=entrypoint.value)
            docker_failures.labels(site_code=site_code, entrypoint=entrypoint.value)
        inline_results.labels(site_code=site_code)


def file_ext(file_path: str) -> str:
    return file_path.split(".")[-1].lower()


def clean_sandbox() -> None:
    shutil.rmtree(SANDBOX_DIR, ignore_errors=True)


def temp_sandbox_path(ext: str = "mp4") -> str:
    os.makedirs(SANDBOX_DIR, exist_ok=True)
    return f"{SANDBOX_DIR}/{uuid.uuid4()}.{ext}"


def try_delete_sandbox_file(file_path: str) -> None:
    if os.path.commonpath([SANDBOX_DIR, file_path]) != SANDBOX_DIR:
        raise ValueError("Can only delete files in sandbox directory")
    try:
        os.remove(file_path)
    except FileNotFoundError:
        pass


@contextmanager
def temp_sandbox_file(ext: str = "mp4") -> Generator[str, None, None]:
    temp_path = temp_sandbox_path(ext=ext)
    try:
        yield temp_path
    finally:
        try_delete_sandbox_file(temp_path)


class DownloadError(Exception):
    def __init__(self, url: str, exc: ClientResponseError) -> None:
        self.url = url
        self.exc = exc


@dataclasses.dataclass
class DownloadedFile:
    dl_path: str
    filesize: int

    def file_ext(self) -> str:
        return file_ext(self.dl_path)


async def _download_file(url: str) -> DownloadedFile:
    dl_path = temp_sandbox_path(file_ext(url))
    with time_taken_downloading_image.time():
        session = aiohttp.ClientSession()
        dl_filesize = 0
        async with session.get(url) as resp:
            try:
                resp.raise_for_status()
            except ClientResponseError as e:
                raise DownloadError(url, e)
            with open(dl_path, "wb") as f:
                async for chunk in resp.content.iter_chunked(8192):
                    f.write(chunk)
                    dl_filesize += len(chunk)
    return DownloadedFile(dl_path, dl_filesize)


@asynccontextmanager
async def _downloaded_file(url: str) -> AsyncGenerator[DownloadedFile, None, None]:
    dl_file = None
    try:
        dl_file = await _download_file(url)
        yield dl_file
    finally:
        if dl_file is not None:
            try_delete_sandbox_file(dl_file.dl_path)


def _img_has_transparency(img: Image) -> bool:
    if img.info.get("transparency", None) is not None:
        return True
    if img.mode == "P":
        transparent = img.info.get("transparency", -1)
        for _, index in img.getcolors():
            if index == transparent:
                return True
    elif img.mode == "RGBA":
        extrema = img.getextrema()
        if extrema[3][0] < 255:
            return True
    return False


def _img_size(img: Image) -> Tuple[int, int]:
    return img.size


IMG_EDIT_LOCK = Lock()


@asynccontextmanager
async def open_image(img_path: str, *, load_truncated: bool = False) -> ContextManager[Image]:
    async with IMG_EDIT_LOCK:
        try:
            ImageFile.LOAD_TRUNCATED_IMAGES = load_truncated
            with Image.open(img_path) as img:
                yield img
        finally:
            ImageFile.LOAD_TRUNCATED_IMAGES = False


async def _is_animated(file_path: str) -> bool:
    if file_ext(file_path) not in Sendable.EXTENSIONS_ANIMATED:
        return False
    try:
        async with open_image(file_path) as img:
            # is_animated attribute might not exist, if file is a jpg named ".png"
            return getattr(img, "is_animated", False)
    except (OSError, UnidentifiedImageError) as e:
        logger.warning("Failed to load image %s, trying with truncated image flag", file_path, exc_info=e)
        async with open_image(file_path, load_truncated=True) as img:
            return getattr(img, "is_animated", False)


WrapReturn = TypeVar("WrapReturn", covariant=True)
WrapFunc = Callable[..., WrapReturn]


def _count_exceptions_with_labels(counter: Counter) -> Callable[[WrapFunc], WrapFunc]:
    def _count_exceptions(f: WrapFunc) -> WrapFunc:
        async def wrapper(s: "Sendable", *args: Any, **kwargs: Any) -> WrapReturn:
            self = s
            with counter.labels(site_code=self.site_id).count_exceptions():
                return await f(self, *args, **kwargs)

        return wrapper

    return _count_exceptions


class DockerEntrypoint(enum.Enum):
    FFMPEG = "ffmpeg"
    FFPROBE = "ffprobe"
    OTHER = "other"

    @classmethod
    def from_string(cls, entrypoint_string: Optional[str]) -> "DockerEntrypoint":
        if entrypoint_string is None:
            return cls.FFMPEG
        if entrypoint_string == "ffprobe":
            return cls.FFPROBE
        return cls.OTHER


class CantSendFileType(Exception):
    pass


class InlineSendable(ABC):

    @property
    def site_id(self) -> str:
        return self.submission_id.site_code

    @property
    def id(self) -> str:
        return self.submission_id.submission_id

    @property
    @abstractmethod
    def submission_id(self) -> SubmissionID:
        raise NotImplementedError

    @property
    @abstractmethod
    def thumbnail_url(self) -> str:
        """
        A scaled down thumbnail, of the full image, or of the preview image
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def link(self) -> str:
        raise NotImplementedError

    def to_inline_query_result(self, builder: InlineBuilder) -> Awaitable[InputBotInlineResultPhoto]:
        inline_results.labels(site_code=self.site_id).inc()
        return builder.photo(
            file=self.thumbnail_url,
            id=f"{self.site_id}:{self.id}",
            text=self.link,
            # Button is required such that the bot can get a callback with the message id, and edit it later.
            buttons=[Button.inline("⏳ Optimising", f"neaten_me:{self.submission_id.to_inline_code()}")],
        )


def _url_to_media(url: str, as_img: bool) -> TypeInputMedia:
    if as_img:
        return InputMediaPhotoExternal(url)
    else:
        return InputMediaDocumentExternal(url)


class Sendable(InlineSendable):
    EXTENSIONS_GIF = ["gif"]  # These should be converted to png, if not animated
    EXTENSIONS_ANIMATED = [
        "gif",
        "png",
    ]  # These should be converted to mp4, without sound, if they are animated
    EXTENSIONS_VIDEO = ["webm"]  # These should be converted to mp4, with sound

    EXTENSIONS_AUTO_DOCUMENT = ["pdf"]  # Telegram can embed these as documents
    EXTENSIONS_AUDIO = ["mp3"]  # Telegram can embed these as audio
    EXTENSIONS_PHOTO = ["jpg", "jpeg"]  # Telegram can embed these as images
    # Maybe use these for labels

    SIZE_LIMIT_IMAGE = 5 * 1000**2  # Maximum 5MB image size on telegram
    SIZE_LIMIT_GIF = 8 * 1000**2  # Maximum 8MB gif size on telegram
    SIZE_LIMIT_VIDEO = 10 * 1000**2  # Maximum 10MB video autodownload size on telegram
    SIZE_LIMIT_DOCUMENT = 20 * 1000**2  # Maximum 20MB document size on telegram
    LENGTH_LIMIT_GIF = 40  # Maximum 40 seconds for gifs, otherwise video, for ease
    SEMIPERIMETER_LIMIT_IMAGE = 10_000  # Maximum 10,000 pixels semiperimeter for images on telegram
    IMG_TRANSPARENCY_COlOUR = (255, 255, 255)  # Colour to mask out transparency with when sending images

    DOCKER_TIMEOUT = 5 * 60

    @property
    @abstractmethod
    def download_url(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def download_file_ext(self) -> str:
        raise NotImplementedError

    @abstractmethod
    async def download_file_size(self) -> int:
        raise NotImplementedError

    @property
    @abstractmethod
    def preview_image_url(self) -> str:
        """
        For image submissions, the preview image is probably the same as the download url.
        For non-image submissions, the preview image is probably a cover image.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def title(self) -> Optional[str]:
        raise NotImplementedError

    @property
    @abstractmethod
    def author(self) -> Optional[str]:
        raise NotImplementedError

    @_count_exceptions_with_labels(sendable_failure)
    async def send_message(
        self,
        client: TelegramClient,
        chat: TypeInputPeer,
        *,
        reply_to: int = None,
        prefix: str = None,
        edit: bool = False,
        uploaded_media: UploadedMedia = None
    ) -> Optional[SentSubmission]:
        sendable_sent.labels(site_code=self.site_id).inc()
        send_partial = self._partial_sender(client, chat, reply_to, prefix, edit)
        if not uploaded_media:
            uploaded_media = await self.upload(client)
        return await send_partial(uploaded_media.media, uploaded_media.settings)

    def _partial_sender(
            self,
            client: TelegramClient,
            chat: TypeInputPeer,
            reply_to: int,
            prefix: str,
            edit: bool,
    ) -> Callable[[TypeInputMedia, SendSettings], Awaitable[SentSubmission]]:
        async def send_partial(input_media: TypeInputMedia, settings: SendSettings) -> Optional[SentSubmission]:
            if edit:
                sendable_edit.labels(site_code=self.site_id).inc()
                with time_taken_editing_message.time():
                    msg = await client.edit_message(
                        entity=chat,
                        file=input_media,
                        message=self.caption(settings.caption, prefix),
                        force_document=settings.force_doc,
                        parse_mode="html",
                    )
            else:
                with time_taken_sending_message.time():
                    msg = await client.send_message(
                        entity=chat,
                        file=input_media,
                        message=self.caption(settings.caption, prefix),
                        reply_to=reply_to,
                        force_document=settings.force_doc,
                        parse_mode="html",  # Markdown is not safe here, because of the prefix.
                    )
            sent_sub = SentSubmission.from_resp(
                self.submission_id, msg, self.download_url, self.caption(settings.caption)
            )
            if sent_sub:
                sent_sub.save_cache = settings.save_cache
            return sent_sub
        return send_partial

    async def upload(self, client: TelegramClient) -> UploadedMedia:
        settings = SendSettings(CaptionSettings())
        ext = self.download_file_ext

        # Handle potentially animated formats
        if ext in self.EXTENSIONS_ANIMATED:
            async with _downloaded_file(self.download_url) as dl_file:
                if await self._is_animated(dl_file.dl_path):
                    return await self._upload_video(client, dl_file, settings)
                else:
                    return await self._upload_image(client, dl_file, settings)
        # Handle photos
        if ext in self.EXTENSIONS_PHOTO:
            async with _downloaded_file(self.download_url) as dl_file:
                return await self._upload_image(client, dl_file, settings)
        # Handle videos, which can be made pretty
        if ext in self.EXTENSIONS_VIDEO:
            async with _downloaded_file(self.download_url) as dl_file:
                sendable_animated.labels(site_code=self.site_id).inc()
                return await self._upload_video(client, dl_file, settings)
        # Everything else is a file, send with title and author
        settings.caption.title = True
        settings.caption.author = True
        # Special handling, if it's small enough
        with time_taken_fetching_filesize.time():
            dl_filesize = await self.download_file_size()
        if dl_filesize < self.SIZE_LIMIT_DOCUMENT:
            # Handle pdfs, which can be sent as documents
            if ext in self.EXTENSIONS_AUTO_DOCUMENT:
                settings.force_doc = True
                return UploadedMedia(self.submission_id, _url_to_media(self.download_url, False), settings)
            # Handle audio
            if ext in self.EXTENSIONS_AUDIO:
                async with _downloaded_file(self.download_url) as dl_file:
                    return await self._upload_audio(client, dl_file, settings)
        # Handle files telegram can't handle
        sendable_other.labels(site_code=self.site_id).inc()
        settings.caption.direct_link = True
        try:
            async with _downloaded_file(self.preview_image_url) as dl_file:
                return await self._upload_image(client, dl_file, settings)
        except DownloadError as e:
            # Sometimes with stories, the preview image does not exist, so use thumbnail
            if e.exc.status == 404:
                async with _downloaded_file(self.thumbnail_url) as dl_file:
                    return await self._upload_image(client, dl_file, settings)
            raise e

    def _save_to_debug(self, file_path: str) -> None:
        os.makedirs("debug", exist_ok=True)
        ext = file_ext(file_path)
        shutil.copy(file_path, f"debug/{self.submission_id.site_code}_{self.submission_id.submission_id}.{ext}")

    async def _is_animated(self, file_path: str) -> bool:
        try:
            return await _is_animated(file_path)
        except UnidentifiedImageError as e:
            self._save_to_debug(file_path)
            raise e

    async def _upload_video(
            self,
            client: TelegramClient,
            dl_file: DownloadedFile,
            settings: SendSettings
    ) -> UploadedMedia:
        sendable_animated.labels(site_code=self.site_id).inc()
        try:
            logger.info("Sending video, submission ID %s, converting to mp4", self.submission_id)
            thumb_handle = None
            with temp_sandbox_file("mp4") as output_path:
                with temp_sandbox_file("jpg") as thumb_path:
                    thumbnail = False
                    with time_taken_converting_video.time():
                        video_metadata = await self._convert_video(dl_file.dl_path, output_path)
                        if video_metadata.has_audio or video_metadata.duration > self.LENGTH_LIMIT_GIF:
                            await self._thumbnail_video(output_path, thumb_path)
                            thumbnail = True
                    with time_taken_uploading_file.time():
                        file_handle = await client.upload_file(output_path)
                        if thumbnail:
                            thumb_handle = await client.upload_file(thumb_path)
            media = InputMediaUploadedDocument(
                file=file_handle,
                mime_type="video/mp4",
                attributes=[
                    DocumentAttributeFilename(f"{self.submission_id.to_filename()}.mp4"),
                    DocumentAttributeVideo(int(video_metadata.duration), video_metadata.width, video_metadata.height),
                ],
                thumb=thumb_handle,
                force_file=False,
                nosound_video=False,
            )
            return UploadedMedia(self.submission_id, media, settings)
        except Exception as e:
            logger.error("Failed to convert video to mp4. Submission ID: %s", self.submission_id, exc_info=e)
            settings.save_cache = False
            settings.caption.direct_link = True
            media = _url_to_media(self.download_url, False)
            return UploadedMedia(self.submission_id, media, settings)

    async def _upload_image(
            self,
            client: TelegramClient,
            dl_file: DownloadedFile,
            settings: SendSettings
    ) -> UploadedMedia:
        sendable_image.labels(site_code=self.site_id).inc()
        # If filesize is too big, set caption to true
        if os.path.getsize(dl_file.dl_path) > self.SIZE_LIMIT_IMAGE:
            settings.caption.direct_link = True
        # Load as image and check things
        with temp_sandbox_file("jpg") as output_file:
            try:
                async with open_image(dl_file.dl_path) as img:
                    settings = await self._convert_image(img, output_file, settings)
            except (OSError, UnidentifiedImageError) as e:
                logger.warning(
                    "Failed to convert image %s, trying with truncated image flag",
                    self.submission_id,
                    exc_info=e
                )
                try:
                    async with open_image(dl_file.dl_path, load_truncated=True) as img:
                        settings = await self._convert_image(img, output_file, settings)
                except (OSError, UnidentifiedImageError) as e2:
                    logger.error(
                        "Failed to convert image %s, even with truncated image flag",
                        self.submission_id,
                        exc_info=e2
                    )
                    self._save_to_debug(dl_file.dl_path)
                    raise e2

            filesize = os.path.getsize(output_file)
            with time_taken_uploading_file.time():
                file_handle = await client.upload_file(
                    output_file,
                    file_size=filesize
                )
        media = InputMediaUploadedPhoto(file_handle)
        return UploadedMedia(self.submission_id, media, settings)

    async def _upload_audio(
            self,
            client: TelegramClient,
            dl_file: DownloadedFile,
            settings: SendSettings
    ) -> UploadedMedia:
        sendable_audio.labels(site_code=self.site_id).inc()
        async with _downloaded_file(self.thumbnail_url) as thumb_file:
            with time_taken_uploading_file.time():
                file_handle = await client.upload_file(dl_file.dl_path)
                thumb_handle = await client.upload_file(thumb_file.dl_path)
        media = InputMediaUploadedDocument(
            file=file_handle,
            mime_type="audio/mp3",
            attributes=[
                DocumentAttributeFilename(f"{self.submission_id.to_filename()}.mp3"),
                DocumentAttributeAudio(
                    duration=0,
                    title=self.title,
                    performer=self.author
                ),
            ],
            thumb=thumb_handle,
            force_file=False,
        )
        return UploadedMedia(self.submission_id, media, settings)

    @abstractmethod
    def caption(self, settings: CaptionSettings, prefix: Optional[str] = None) -> str:
        raise NotImplementedError  # TODO: Pull caption builder out, I guess? Three implementations of caption data?

    @_count_exceptions_with_labels(convert_image_failures)
    async def _convert_image(self, img: Image, output_path: str, settings: SendSettings) -> SendSettings:
        # Get exif data
        exif = img.info.get("exif")

        # Check image resolution and scale
        width, height = _img_size(img)
        semiperimeter = width + height
        if semiperimeter > self.SEMIPERIMETER_LIMIT_IMAGE:
            settings.caption.direct_link = True
            scale_factor = self.SEMIPERIMETER_LIMIT_IMAGE / semiperimeter
            new_width = int(width * scale_factor)
            new_height = int(height * scale_factor)
            img = img.resize((new_width, new_height), Image.ANTIALIAS)

        # Mask out transparency
        if img.mode == 'P':
            img = img.convert('RGBA')
        alpha_index = img.mode.find('A')
        if alpha_index != -1:
            if _img_has_transparency(img):
                settings.caption.direct_link = True
            result = Image.new('RGB', img.size, self.IMG_TRANSPARENCY_COlOUR)
            result.paste(img, mask=img.split()[alpha_index])
            img = result

        if img.mode != 'RGB':
            img = img.convert('RGB')

        # Save image as jpg
        with open(output_path, "wb") as out_handle:
            kwargs = {}
            if exif:
                kwargs["exif"] = exif
            img.save(out_handle, 'JPEG', progressive=True, quality=95, **kwargs)
        return settings

    @_count_exceptions_with_labels(thumbnail_video_failures)
    async def _thumbnail_video(self, video_path: str, thumbnail_path: str) -> None:
        client = docker.from_env()
        await self._run_docker(client, f"-i /{video_path} -ss 00:00:01.000 -vframes 1 /{thumbnail_path}")

    @_count_exceptions_with_labels(convert_gif_failures)
    async def _convert_gif(self, gif_path: str, output_path: str) -> VideoMetadata:
        convert_gif_total.labels(site_code=self.site_id).inc()
        ffmpeg_options = (
            " -an -vcodec libx264 -tune animation -preset veryslow -movflags faststart -pix_fmt yuv420p "
            "-vf \"scale='min(1280,iw)':'min(1280,ih)':force_original_aspect_"
            'ratio=decrease,scale=trunc(iw/2)*2:trunc(ih/2)*2" -profile:v baseline -level 3.0 -vsync vfr'
        )
        crf_option = " -crf 18"
        # first pass
        client = docker.from_env()
        await self._run_docker(client, f"-i /{gif_path} {ffmpeg_options} {crf_option} /{output_path}")
        # Get metadata
        metadata = await self._video_metadata(client, output_path)
        # Check file size
        if os.path.getsize(output_path) < self.SIZE_LIMIT_GIF:
            convert_gif_only_one_attempt.labels(site_code=self.site_id).inc()
            return metadata
        # If it's too big, do a 2 pass run
        convert_gif_two_pass.labels(site_code=self.site_id).inc()
        return await self._convert_two_pass(client, gif_path, output_path, metadata, ffmpeg_options)

    @_count_exceptions_with_labels(convert_video_failures)
    async def _convert_video(self, video_path: str, output_path: str) -> VideoMetadata:
        convert_video_total.labels(site_code=self.site_id).inc()
        # If it's a gif, it has no audio track
        if file_ext(video_path) in self.EXTENSIONS_ANIMATED:
            convert_video_animated.labels(site_code=self.site_id).inc()
            return await self._convert_gif(video_path, output_path)
        client = docker.from_env()
        ffmpeg_options = "-qscale 0"
        ffmpeg_prefix = ""
        # Get video metadata
        metadata = await self._video_metadata(client, video_path)
        # Check if it has audio
        if not metadata.has_audio:
            convert_video_no_audio.labels(site_code=self.site_id).inc()
            ffmpeg_options = "-qscale:v 0 -acodec aac -map 0:0 -map 1:0 -shortest"
            ffmpeg_prefix = "-f lavfi -i aevalsrc=0"
            if metadata.duration < self.LENGTH_LIMIT_GIF:
                convert_video_no_audio_gif.labels(site_code=self.site_id).inc()
                return await self._convert_gif(video_path, output_path)
        # first pass
        convert_video_to_video.labels(site_code=self.site_id).inc()
        await self._run_docker(client, f"{ffmpeg_prefix} -i /{video_path} {ffmpeg_options} /{output_path}")
        # Check file size
        if os.path.getsize(output_path) < self.SIZE_LIMIT_VIDEO:
            convert_video_only_one_attempt.labels(site_code=self.site_id).inc()
            return await self._video_metadata(client, output_path)
        # If it's too big, do a 2 pass run
        convert_video_two_pass.labels(site_code=self.site_id).inc()
        return await self._convert_two_pass(client, video_path, output_path, metadata, ffmpeg_options, ffmpeg_prefix)

    async def _convert_two_pass(
        self,
        client: DockerClient,
        video_path: str,
        output_path: str,
        metadata: VideoMetadata,
        ffmpeg_options: str,
        ffmpeg_prefix: str = "",
    ) -> VideoMetadata:
        logger.info(
            "Doing a two pass video conversion on site ID %s, submission ID %s",
            self.site_id,
            self.id,
        )
        # Get video metadata
        if metadata is None:
            metadata = await self._video_metadata(client, video_path)
        # 2 pass run
        bitrate = (self.SIZE_LIMIT_VIDEO / metadata.duration) * 8
        # If it has an audio stream, subtract audio bitrate from total bitrate to get video bitrate
        if metadata.has_audio:
            bitrate = bitrate - (metadata.audio_bitrate or 0)
            if bitrate < 0:
                logger.error(
                    "Desired bitrate for submission (site id %s sub id %s) is higher than the audio bitrate alone.",
                    self.site_id,
                    self.id,
                )
                raise ValueError("Bitrate cannot be negative")
        with temp_sandbox_file("mp4") as two_pass_file:
            with temp_sandbox_file("log") as log_file:
                full_ffmpeg_options = f"{ffmpeg_prefix} -i /{video_path} {ffmpeg_options} -b:v {bitrate}"
                await self._run_docker(
                    client,
                    f"{full_ffmpeg_options} -pass 1 -f mp4 -passlogfile /{log_file} /dev/null -y",
                )
                await self._run_docker(
                    client,
                    f"{full_ffmpeg_options} -pass 2 -passlogfile /{log_file} /{two_pass_file} -y",
                )
            # Copy output to output file
            try:
                os.remove(output_path)
            except FileNotFoundError:
                pass
            with open(output_path, "wb") as output_handle:
                with open(two_pass_file, "rb") as input_handle:
                    shutil.copyfileobj(input_handle, output_handle)
        # Get new metadata
        return await self._video_metadata(client, output_path)

    async def _video_metadata(self, client: DockerClient, input_path: str) -> VideoMetadata:
        metadata_json_str = await self._run_docker(
            client,
            f"-show_entries format=duration:stream=width,height,bit_rate,codec_type /{input_path} -of json -v error",
            entrypoint="ffprobe",
        )
        return VideoMetadata.from_json_str(metadata_json_str)

    async def _run_docker(self, client: DockerClient, args: str, entrypoint: Optional[str] = None) -> str:
        labels = {
            "site_code": self.site_id,
            "entrypoint": DockerEntrypoint.from_string(entrypoint).value,
        }
        with docker_run_time.labels(**labels).time():
            with docker_failures.labels(**labels).count_exceptions():
                sandbox_dir = os.getcwd() + "/" + SANDBOX_DIR
                logger.debug(
                    "Running docker container with args %s and entrypoint %s",
                    args,
                    entrypoint,
                )
                container: Container = client.containers.run(
                    "jrottenberg/ffmpeg:alpine",
                    args,
                    entrypoint=entrypoint,
                    volumes={sandbox_dir: {"bind": "/sandbox", "mode": "rw"}},
                    working_dir="/sandbox",
                    detach=True,
                )
                start_time = datetime.datetime.now()
                while (datetime.datetime.now() - start_time).total_seconds() < self.DOCKER_TIMEOUT:
                    container.reload()
                    if container.status == "exited":
                        output = container.logs()
                        container.remove(force=True)
                        return output
                    await asyncio.sleep(2)
                # Kill container
                logger.warning("Docker timed out, killing container.")
                container.kill()
                container.remove(force=True)
                raise TimeoutError("Docker container timed out")
