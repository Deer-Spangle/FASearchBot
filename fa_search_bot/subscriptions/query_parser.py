from __future__ import annotations

import functools
import logging
import re
import string
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, NewType

import pyparsing
from pyparsing import (
    CaselessKeyword,
    Forward,
    Group,
    Literal,
    ParseException,
    QuotedString,
    Word,
    ZeroOrMore,
    printables,
)

from fa_search_bot.sites.submission import Rating, QueryTarget

if TYPE_CHECKING:
    from typing import Any, Optional, Pattern, Sequence

    from pyparsing import ParserElement, ParseResults


logger = logging.getLogger(__name__)

rating_dict = {
    "general": Rating.GENERAL,
    "safe": Rating.GENERAL,
    "mature": Rating.MATURE,
    "questionable": Rating.MATURE,
    "adult": Rating.ADULT,
    "explicit": Rating.ADULT,
}

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
    @abstractmethod
    def get_field_words(self, sub: QueryTarget) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def get_texts(self, sub: QueryTarget) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def get_texts_dict(self, sub: QueryTarget) -> dict[FieldLocation, str]:
        raise NotImplementedError

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, self.__class__)

    def __repr__(self) -> str:
        return self.__class__.__name__


class KeywordField(Field):
    def get_field_words(self, sub: QueryTarget) -> list[str]:
        return _clean_word_list(sub.keywords)

    def get_texts(self, sub: QueryTarget) -> list[str]:
        return sub.keywords

    def get_texts_dict(self, sub: QueryTarget) -> dict[FieldLocation, str]:
        return {FieldLocation(f"keyword_{num}"): keyword for num, keyword in enumerate(sub.keywords)}


class TitleField(Field):
    def get_field_words(self, sub: QueryTarget) -> list[str]:
        return sum([_split_text_to_cleaned_words(title) for title in sub.title], start=[])

    def get_texts(self, sub: QueryTarget) -> list[str]:
        return sub.title

    def get_texts_dict(self, sub: QueryTarget) -> dict[FieldLocation, str]:
        return {FieldLocation(f"title_{num}"): title for num, title in enumerate(sub.title)}


class DescriptionField(Field):
    def get_field_words(self, sub: QueryTarget) -> list[str]:
        return sum([_split_text_to_cleaned_words(desc) for desc in sub.description], start=[])

    def get_texts(self, sub: QueryTarget) -> list[str]:
        return sub.description

    def get_texts_dict(self, sub: QueryTarget) -> dict[FieldLocation, str]:
        return {FieldLocation(f"description_{num}"): desc for num, desc in enumerate(sub.description)}


class ArtistField(Field):
    def get_field_words(self, sub: QueryTarget) -> list[str]:
        return [artist.lower() for artist in sub.artist]

    def get_texts(self, sub: QueryTarget) -> list[str]:
        return sub.artist

    def get_texts_dict(self, sub: QueryTarget) -> dict[FieldLocation, str]:
        return {FieldLocation(f"artist_{num}"): artist for num, artist in enumerate(sub.artist)}


class AnyField(Field):
    def get_field_words(self, sub: QueryTarget) -> list[str]:
        return [
            *TitleField().get_field_words(sub),
            *DescriptionField().get_field_words(sub),
            *KeywordField().get_field_words(sub),
            *ArtistField().get_field_words(sub),
        ]

    def get_texts(self, sub: QueryTarget) -> list[str]:
        return [
            *TitleField().get_texts(sub),
            *DescriptionField().get_texts(sub),
            *KeywordField().get_texts(sub),
            *ArtistField().get_texts(sub),
        ]

    def get_texts_dict(self, sub: QueryTarget) -> dict[FieldLocation, str]:
        return {
            **TitleField().get_texts_dict(sub),
            **DescriptionField().get_texts_dict(sub),
            **KeywordField().get_texts_dict(sub),
            **ArtistField().get_texts_dict(sub),
        }


class MatchLocation:
    def __init__(self, field: FieldLocation, start_position: int, end_position: int):
        self.field = field
        self.start_position = start_position
        self.end_position = end_position

    def overlaps(self, location: "MatchLocation") -> bool:
        if self.field != location.field:
            return False
        if self.start_position < location.start_position:
            return self.end_position > location.start_position
        else:
            return location.end_position > self.start_position

    def overlaps_any(self, locations: list["MatchLocation"]) -> bool:
        return any(self.overlaps(location) for location in locations)

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, MatchLocation)
            and self.field == other.field
            and self.start_position == other.start_position
            and self.end_position == other.end_position
        )

    def __hash__(self) -> int:
        return hash((self.field, self.start_position, self.end_position))

    def __repr__(self) -> str:
        return f"MatchLocation(FieldLocation({self.field}), {self.start_position}, {self.end_position})"


class Query(ABC):
    @abstractmethod
    def matches_submission(self, sub: QueryTarget) -> bool:
        raise NotImplementedError


class LocationQuery(Query, ABC):
    @abstractmethod
    def match_locations(self, sub: QueryTarget) -> list[MatchLocation]:
        raise NotImplementedError


class OrQuery(Query):
    def __init__(self, sub_queries: Sequence["Query"]):
        self.sub_queries = sub_queries

    def matches_submission(self, sub: QueryTarget) -> bool:
        return any(q.matches_submission(sub) for q in self.sub_queries)

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, OrQuery)
            and len(self.sub_queries) == len(other.sub_queries)
            and all(self.sub_queries[i] == other.sub_queries[i] for i in range(len(self.sub_queries)))
        )

    def __repr__(self) -> str:
        return "OR(" + ", ".join(repr(q) for q in self.sub_queries) + ")"

    def __str__(self) -> str:
        return "(" + " OR ".join(str(q) for q in self.sub_queries) + ")"


class LocationOrQuery(OrQuery, LocationQuery):
    def __init__(self, sub_queries: list["LocationQuery"]):
        super().__init__(sub_queries)
        # Set it again, so we know sub_queries are LocationQuery objects, rather than just Query objects
        self.sub_queries: list["LocationQuery"] = sub_queries

    def match_locations(self, sub: QueryTarget) -> list[MatchLocation]:
        return list(set(match for q in self.sub_queries for match in q.match_locations(sub)))


class AndQuery(Query):
    def __init__(self, sub_queries: list["Query"]):
        self.sub_queries = sub_queries

    def matches_submission(self, sub: QueryTarget) -> bool:
        return all(q.matches_submission(sub) for q in self.sub_queries)

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, AndQuery)
            and len(self.sub_queries) == len(other.sub_queries)
            and all(self.sub_queries[i] == other.sub_queries[i] for i in range(len(self.sub_queries)))
        )

    def __repr__(self) -> str:
        return "AND(" + ", ".join(repr(q) for q in self.sub_queries) + ")"

    def __str__(self) -> str:
        return "(" + " AND ".join(str(q) for q in self.sub_queries) + ")"


class NotQuery(Query):
    def __init__(self, sub_query: "Query"):
        self.sub_query = sub_query

    def matches_submission(self, sub: QueryTarget) -> bool:
        return not self.sub_query.matches_submission(sub)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, NotQuery) and self.sub_query == other.sub_query

    def __repr__(self) -> str:
        return f"NOT({self.sub_query!r})"

    def __str__(self) -> str:
        return f"-{self.sub_query}"


class RatingQuery(Query):
    def __init__(self, rating: Rating):
        self.rating = rating

    def matches_submission(self, sub: QueryTarget) -> bool:
        return sub.rating == self.rating

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, RatingQuery) and self.rating == other.rating

    def __repr__(self) -> str:
        return f"RATING({self.rating})"

    def __str__(self) -> str:
        return f"rating:{self.rating}"


class WordQuery(LocationQuery):
    def __init__(self, word: str, field: Optional["Field"] = None):
        self.word = word
        if field is None:
            field = AnyField()
        self.field = field

    def matches_submission(self, sub: QueryTarget) -> bool:
        return self.word.lower() in self.field.get_field_words(sub)

    def match_locations(self, sub: QueryTarget) -> list[MatchLocation]:
        regex = re.compile(boundary_pattern_start + re.escape(self.word) + boundary_pattern_end, re.I)
        return [
            MatchLocation(location, m.start(), m.end())
            for location, text in self.field.get_texts_dict(sub).items()
            for m in regex.finditer(text)
        ]

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, WordQuery) and self.word == other.word and self.field == other.field

    def __repr__(self) -> str:
        if self.field == AnyField():
            return f"WORD({self.word})"
        return f"WORD({self.word}, {self.field})"

    def __str__(self) -> str:
        if self.field == AnyField():
            return self.word
        return f"{self.field}:{self.word}"


class PrefixQuery(LocationQuery):
    def __init__(self, prefix: str, field: Optional["Field"] = None):
        self.prefix = prefix
        if field is None:
            field = AnyField()
        self.field = field

    def matches_submission(self, sub: QueryTarget) -> bool:
        return any(
            word.startswith(self.prefix.lower()) and word != self.prefix.lower()
            for word in self.field.get_field_words(sub)
        )

    def match_locations(self, sub: QueryTarget) -> list[MatchLocation]:
        regex = re.compile(
            boundary_pattern_start + re.escape(self.prefix) + not_punctuation_pattern + boundary_pattern_end,
            re.I,
        )
        return [
            MatchLocation(location, m.start(), m.end())
            for location, text in self.field.get_texts_dict(sub).items()
            for m in regex.finditer(text)
        ]

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, PrefixQuery) and self.prefix == other.prefix and self.field == other.field

    def __repr__(self) -> str:
        if self.field == AnyField():
            return f"PREFIX({self.prefix})"
        return f"PREFIX({self.prefix}, {self.field})"

    def __str__(self) -> str:
        if self.field == AnyField():
            return self.prefix + "*"
        return f"{self.field}:{self.prefix}*"


class SuffixQuery(LocationQuery):
    def __init__(self, suffix: str, field: Optional["Field"] = None):
        self.suffix = suffix
        if field is None:
            field = AnyField()
        self.field = field

    def matches_submission(self, sub: QueryTarget) -> bool:
        return any(
            word.endswith(self.suffix.lower()) and word != self.suffix.lower()
            for word in self.field.get_field_words(sub)
        )

    def match_locations(self, sub: QueryTarget) -> list[MatchLocation]:
        regex = re.compile(
            boundary_pattern_start + not_punctuation_pattern + re.escape(self.suffix) + boundary_pattern_end,
            re.I,
        )
        return [
            MatchLocation(location, m.start(), m.end())
            for location, text in self.field.get_texts_dict(sub).items()
            for m in regex.finditer(text)
        ]

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, SuffixQuery) and self.suffix == other.suffix and self.field == other.field

    def __repr__(self) -> str:
        if self.field == AnyField():
            return f"SUFFIX({self.suffix})"
        return f"SUFFIX({self.suffix}, {self.field})"

    def __str__(self) -> str:
        if self.field == AnyField():
            return "*" + self.suffix
        return f"{self.field}:*{self.suffix}"


class RegexQuery(LocationQuery):
    def __init__(self, pattern: Pattern[str], field: Optional["Field"] = None):
        self.pattern = pattern
        if field is None:
            field = AnyField()
        self.field = field

    def matches_submission(self, sub: QueryTarget) -> bool:
        return any(self.pattern.search(word) for word in self.field.get_field_words(sub))

    def match_locations(self, sub: QueryTarget) -> list[MatchLocation]:
        return [
            MatchLocation(location, m.start(), m.end())
            for location, text in self.field.get_texts_dict(sub).items()
            for m in self.pattern.finditer(text)
        ]

    @classmethod
    def from_string_with_asterisks(cls, word: str, field: Optional["Field"] = None) -> "RegexQuery":
        word_split = re.split(r"\*+", word)
        parts = [re.escape(part) for part in word_split]
        regex = boundary_pattern_start + not_punctuation_pattern.join(parts) + boundary_pattern_end
        pattern = re.compile(regex, re.I)
        return RegexQuery(pattern, field)

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, RegexQuery)
            and self.pattern.pattern == other.pattern.pattern
            and self.field == other.field
        )

    def __repr__(self) -> str:
        if self.field == AnyField():
            return f"REGEX({self.pattern.pattern})"
        return f"REGEX({self.pattern.pattern}, {self.field})"

    def __str__(self) -> str:
        if self.field == AnyField():
            return self.pattern.pattern
        return f"{self.field}:{self.pattern.pattern}"


class PhraseQuery(LocationQuery):
    def __init__(self, phrase: str, field: Optional["Field"] = None):
        self.phrase = phrase
        self.phrase_regex = re.compile(boundary_pattern_start + re.escape(self.phrase) + boundary_pattern_end, re.I)
        if field is None:
            field = AnyField()
        self.field = field

    def matches_submission(self, sub: QueryTarget) -> bool:
        return any(self.phrase_regex.search(text) for text in self.field.get_texts(sub))

    def match_locations(self, sub: QueryTarget) -> list[MatchLocation]:
        return [
            MatchLocation(location, m.start(), m.end())
            for location, text in self.field.get_texts_dict(sub).items()
            for m in self.phrase_regex.finditer(text)
        ]

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, PhraseQuery) and self.phrase == other.phrase and self.field == other.field

    def __repr__(self) -> str:
        if self.field == AnyField():
            return f"PHRASE({self.phrase})"
        return f'PHRASE("{self.phrase}", {self.field})'

    def __str__(self) -> str:
        if self.field == AnyField():
            return f'"{self.phrase}"'
        return f'{self.field}:"{self.phrase}"'


class ExceptionQuery(Query):
    def __init__(self, word: LocationQuery, exception: LocationQuery):
        self.word = word
        self.exception = exception

    def matches_submission(self, sub: QueryTarget) -> bool:
        word_locations = self.word.match_locations(sub)
        exception_locations = self.exception.match_locations(sub)
        return any(not location.overlaps_any(exception_locations) for location in word_locations)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, ExceptionQuery) and self.word == other.word and self.exception == other.exception

    def __repr__(self) -> str:
        return f"EXCEPTION({self.word!r}, {self.exception!r})"

    def __str__(self) -> str:
        return f"{self.word} EXCEPT {self.exception}"


class InvalidQueryException(Exception):
    pass


@functools.lru_cache()
def query_parser() -> ParserElement:
    # Creating the grammar
    valid_chars = printables.replace("(", "").replace(")", "").replace(":", "").replace('"', "")
    expr = Forward().setName("expression")

    quotes = QuotedString('"', "\\").setName("quoted string").setResultsName("quotes")

    brackets = (
        Group(Literal("(").suppress() + expr + Literal(")").suppress())
        .setName("bracketed expression")
        .setResultsName("brackets")
    )

    words = Word(valid_chars).setName("word").setResultsName("word")

    exception_elem = (
        Group(quotes | words).setName("exception element").setResultsName("exception_element", listAllMatches=True)
    )
    exception = (
        Group(
            exception_elem
            | (
                Literal("(")
                + exception_elem
                + ZeroOrMore(pyparsing.Optional(CaselessKeyword("or")) + exception_elem)
                + Literal(")")
            )
        )
        .setName("exception")
        .setResultsName("exception")
    )
    exception_connector = (
        (CaselessKeyword("except") | CaselessKeyword("ignore")).setName("Except").setResultsName("except")
    )
    exception_word = words.setName("exception word").setResultsName("exception_word")

    word_with_exception = (
        Group(exception_word + exception_connector + exception)
        .setName("word with exception")
        .setResultsName("word_with_exception")
    )
    word_with_exception_brackets = Literal("(") + word_with_exception + Literal(")")

    field_name = (
        Group((Literal("@").suppress() + Word(valid_chars)) | (Word(valid_chars) + Literal(":").suppress()))
        .setName("field name")
        .setResultsName("field_name")
    )
    field_value = (
        Group(quotes | word_with_exception_brackets | word_with_exception | words)
        .setName("field value")
        .setResultsName("field_value")
    )
    field = Group(field_name + field_value).setName("field").setResultsName("field")

    negator = (
        Group(pyparsing.Optional(Literal("!") | Literal("-") | CaselessKeyword("not")))
        .setName("negator")
        .setResultsName("negator")
    )
    element = (
        Group(quotes | brackets | field | word_with_exception | words).setName("element").setResultsName("element")
    )
    full_element = Group(negator + element).setName("full element").setResultsName("full_element", listAllMatches=True)
    connector = (
        Group(pyparsing.Optional(CaselessKeyword("or") | CaselessKeyword("and")))
        .setName("connector")
        .setResultsName("connector", listAllMatches=True)
    )
    expr <<= full_element + ZeroOrMore(connector + full_element)
    return expr


def parse_query(query_str: str) -> "Query":
    logger.debug("Parsing query: %s", query_str)
    expr = query_parser()
    # Parsing input
    try:
        parsed = expr.parseString(query_str, parseAll=True)
    except ParseException as e:
        logger.warning("Failed to parse query %s.", query_str, exc_info=e)
        raise InvalidQueryException(f"ParseException was thrown: {e}")
    # Turning into query
    return parse_expression(parsed)


def parse_expression(parsed: ParseResults) -> "Query":
    result = parse_full_element(parsed.full_element[0])
    num_connectors = len(parsed.connector)
    for i in range(num_connectors):
        connector = parsed.connector[i]
        full_element = parse_full_element(parsed.full_element[i + 1])
        result = parse_connector(connector, result, full_element)
    return result


def parse_connector(parsed: ParseResults, query1: "Query", query2: "Query") -> "Query":
    if not parsed:
        return AndQuery([query1, query2])
    if parsed[0].lower() == "and":
        return AndQuery([query1, query2])
    if parsed[0].lower() == "or":
        return OrQuery([query1, query2])
    logger.warning("Unrecognised query connector: %s", parsed[0].lower())
    raise InvalidQueryException(f"I do not recognise this connector: {parsed}")


def parse_full_element(parsed: ParseResults) -> "Query":
    if not parsed.negator:
        return parse_element(parsed.element)
    return NotQuery(parse_element(parsed.element))


def parse_element(parsed: ParseResults) -> "Query":
    if parsed.quotes:
        return parse_quotes(parsed.quotes)
    if parsed.brackets:
        return parse_expression(parsed.brackets)
    if parsed.field:
        return parse_field(parsed.field)
    if parsed.word_with_exception:
        return parse_word_with_exception(parsed.word_with_exception)
    if parsed.word:
        return parse_word(parsed.word)
    logger.warning("Unrecognised query element: %s", parsed)
    raise InvalidQueryException(f"I do not recognise this element: {parsed}")


def parse_quotes(phrase: str, field: Optional["Field"] = None) -> "LocationQuery":
    return PhraseQuery(phrase, field)


def parse_field(parsed: ParseResults) -> "Query":
    field_name = parsed.field_name[0]
    field_value = parsed.field_value
    if field_name.lower() == "rating":
        return parse_rating_field(field_value)
    field = parse_field_name(field_name)
    if field_value.quotes:
        return parse_quotes(field_value.quotes, field)
    if field_value.word_with_exception:
        return parse_word_with_exception(field_value.word_with_exception, field)
    if field_value.word:
        return parse_word(field_value.word, field)
    logger.warning("Unrecognised query field value type: %s", field_value)
    raise InvalidQueryException(f"Unrecognised field value {field_value}")


def parse_rating_field(field_value: ParseResults) -> "Query":
    if field_value.quotes:
        logger.warning("Rating field cannot be a quote")
        raise InvalidQueryException("Rating field cannot be a quote")
    rating = rating_dict.get(field_value.word)
    if rating is None:
        logger.warning("Unrecognised rating field value: %s", field_value.word)
        raise InvalidQueryException(f"Unrecognised rating field value: {field_value.word}")
    return RatingQuery(rating)


def parse_field_name(field_name: str) -> "Field":
    if field_name.lower() == "title":
        return TitleField()
    if field_name.lower() in ["desc", "description", "message"]:
        return DescriptionField()
    if field_name.lower() in ["keywords", "keyword", "tag", "tags"]:
        return KeywordField()
    if field_name.lower() in ["artist", "author", "poster", "lower", "uploader"]:
        return ArtistField()
    logger.warning("Unrecognised field name: %s", field_name)
    raise InvalidQueryException(f"Unrecognised field name: {field_name}")


def parse_word(word: str, field: Optional["Field"] = None) -> "LocationQuery":
    if word.startswith("*") and "*" not in word[1:]:
        return SuffixQuery(word[1:], field)
    if word.endswith("*") and "*" not in word[:-1]:
        return PrefixQuery(word[:-1], field)
    if "*" in word:
        return RegexQuery.from_string_with_asterisks(word, field)
    reserved_keywords = ["not", "and", "or", "except", "ignore"]
    if word.lower() in reserved_keywords:
        logger.warning('Word query ("%s") cannot be a reserved keyword.', word)
        raise InvalidQueryException(
            f'Word query ("{word}") cannot be a reserved keyword: {reserved_keywords}. '
            f"If you really want to search for this word, please surround it with quotation marks"
        )
    return WordQuery(word, field)


def parse_exception(parsed: ParseResults, field: Optional["Field"]) -> "LocationQuery":
    elements = []
    for elem in parsed.exception_element:
        if elem.quotes:
            elements.append(parse_quotes(elem.quotes, field))
            continue
        if elem.word:
            elements.append(parse_word(elem.word, field))
            continue
        logger.error("Unrecognised exception query element: %s", parsed)
        raise ParseException(f"Unrecognised exception query element: {parsed}")
    return LocationOrQuery(elements)


def parse_word_with_exception(parsed: ParseResults, field: Optional["Field"] = None) -> "Query":
    word = parse_word(parsed.exception_word, field)
    exc = parse_exception(parsed.exception, field)
    return ExceptionQuery(word, exc)
