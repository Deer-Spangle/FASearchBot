from __future__ import annotations

import re
import string
from abc import ABC, abstractmethod
from functools import lru_cache
from typing import NewType

from fa_search_bot.sites.submission import Rating
from fa_search_bot.sites.submission_id import SubmissionID

punctuation = string.punctuation.replace("-", "").replace("_", "")
punctuation_pattern = r"[\s" + re.escape(punctuation) + "]+"
not_punctuation_pattern = r"[^\s" + re.escape(punctuation) + "]+"
boundary_pattern_start = r"(?:^|(?<=[\s" + re.escape(punctuation) + "]))"
boundary_pattern_end = r"(?:(?=[\s" + re.escape(punctuation) + "])|$)"


def _split_text_to_words(text: str) -> list[str]:
    return re.split(punctuation_pattern, text)


def _clean_word_list(words: list[str]) -> list[str]:
    return [x.lower().strip(punctuation) for x in words]


def _split_text_to_cleaned_words(text: str) -> list[str]:
    return _clean_word_list(_split_text_to_words(text))


FieldLocation = NewType("FieldLocation", str)


class Field(ABC):

    @classmethod
    @abstractmethod
    def get_field(cls, sub: QueryTarget) -> Field:
        raise NotImplementedError()

    @abstractmethod
    def words(self) -> list[str]:
        raise NotImplementedError()

    @abstractmethod
    def texts(self) -> list[str]:
        raise NotImplementedError()

    @abstractmethod
    def texts_dict(self) -> dict[str, list[str]]:
        raise NotImplementedError()


class SpecificField(Field, ABC):
    def __init__(self, value: list[str]) -> None:
        self.value = value


class KeywordField(SpecificField):
    @classmethod
    def get_field(cls, sub: QueryTarget) -> KeywordField:
        return sub.keywords

    @lru_cache
    def words(self) -> list[str]:
        return _clean_word_list(self.value)

    @lru_cache
    def texts(self) -> list[str]:
        return self.value

    @lru_cache
    def texts_dict(self) -> dict[FieldLocation, str]:
        return {FieldLocation(f"keyword_{num}"): keyword for num, keyword in enumerate(self.value)}


class TitleField(SpecificField):
    @classmethod
    def get_field(cls, sub: QueryTarget) -> TitleField:
        return sub.title

    @lru_cache
    def words(self) -> list[str]:
        return sum([_split_text_to_cleaned_words(title) for title in self.value], start=[])

    @lru_cache
    def texts(self) -> list[str]:
        return self.value

    @lru_cache
    def texts_dict(self) -> dict[FieldLocation, str]:
        return {FieldLocation(f"title_{num}"): title for num, title in enumerate(self.value)}


class DescriptionField(SpecificField):
    @classmethod
    def get_field(cls, sub: QueryTarget) -> DescriptionField:
        return sub.description

    @lru_cache
    def words(self) -> list[str]:
        return sum([_split_text_to_cleaned_words(desc) for desc in self.value], start=[])

    @lru_cache
    def texts(self) -> list[str]:
        return self.value

    @lru_cache
    def texts_dict(self) -> dict[FieldLocation, str]:
        return {FieldLocation(f"description_{num}"): desc for num, desc in enumerate(self.value)}


class ArtistField(SpecificField):
    @classmethod
    def get_field(cls, sub: QueryTarget) -> ArtistField:
        return sub.artist

    @lru_cache
    def words(self) -> list[str]:
        return [artist.lower() for artist in self.value]

    @lru_cache
    def texts(self) -> list[str]:
        return self.value

    @lru_cache
    def texts_dict(self) -> dict[FieldLocation, str]:
        return {FieldLocation(f"artist_{num}"): artist for num, artist in enumerate(self.value)}


class AnyField(Field):
    def __init__(
            self,
            title: TitleField,
            description: DescriptionField,
            keyword: KeywordField,
            artist: ArtistField,
    ) -> None:
        self.title = title
        self.description = description
        self.keyword = keyword
        self.artist = artist

    @classmethod
    def get_field(cls, sub: QueryTarget) -> AnyField:
        return sub.any_field

    @lru_cache
    def words(self) -> list[str]:
        return [
            *self.title.words(),
            *self.description.words(),
            *self.keyword.words(),
            *self.artist.words(),
        ]

    @lru_cache
    def texts(self) -> list[str]:
        return [
            *self.title.texts(),
            *self.description.texts(),
            *self.keyword.texts(),
            *self.artist.texts(),
        ]

    @lru_cache
    def texts_dict(self) -> dict[FieldLocation, str]:
        return {
            **self.title.texts_dict(),
            **self.description.texts_dict(),
            **self.keyword.texts_dict(),
            **self.artist.texts_dict(),
        }


class QueryTarget:
    def __init__(
            self,
            sub_id: SubmissionID,
            title: list[str],
            description: list[str],
            keywords: list[str],
            artist: list[str],
            rating: Rating,
    ) -> None:
        self.sub_id = sub_id
        self.title = TitleField(title)
        self.description = DescriptionField(description)
        self.keywords = KeywordField(keywords)
        self.artist = ArtistField(artist)
        self.rating = rating
        self.any_field = AnyField(self.title, self.description, self.keywords, self.artist)

    def to_json(self) -> dict:
        return {
            "sub_id": self.sub_id.to_inline_code(),
            "title": self.title.value,
            "keywords": self.keywords.value,
            "description": self.description.value,
            "artist": self.artist.value,
            "rating": self.rating.name,
        }

    @classmethod
    def from_json(cls, data: dict) -> QueryTarget:
        # noinspection PyTypeChecker
        rating: Rating = Rating[data["rating"]]
        return QueryTarget(
            sub_id=SubmissionID.from_inline_code(data["sub_id"]),
            title=data["title"],
            keywords=data["keywords"],
            description=data["description"],
            artist=data["artist"],
            rating=rating,
        )
