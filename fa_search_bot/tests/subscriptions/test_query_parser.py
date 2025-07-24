import pytest

from fa_search_bot.subscriptions.query_parser import (
    AndQuery,
    ArtistField,
    DescriptionField,
    ExceptionQuery,
    InvalidQueryException,
    KeywordField,
    LocationOrQuery,
    NotQuery,
    OrQuery,
    PhraseQuery,
    PrefixQuery,
    RatingQuery,
    RegexQuery,
    SuffixQuery,
    TitleField,
    WordQuery,
    parse_query,
)
from fa_search_bot.sites.submission import Rating


def test_parser():
    assert parse_query("first") == WordQuery("first")
    assert parse_query("first document") == AndQuery([WordQuery("first"), WordQuery("document")])


def test_negation():
    assert parse_query("-first") == NotQuery(WordQuery("first"))
    assert parse_query("!first") == NotQuery(WordQuery("first"))
    assert parse_query("! first") == NotQuery(WordQuery("first"))
    assert parse_query("not first") == NotQuery(WordQuery("first"))
    assert parse_query("NOT first") == NotQuery(WordQuery("first"))
    assert parse_query("first not second") == AndQuery([WordQuery("first"), NotQuery(WordQuery("second"))])


def test_connectors():
    assert parse_query("first and document") == AndQuery([WordQuery("first"), WordQuery("document")])
    assert parse_query("first or document") == OrQuery([WordQuery("first"), WordQuery("document")])
    assert parse_query("first AND doc OR document") == OrQuery(
        [AndQuery([WordQuery("first"), WordQuery("doc")]), WordQuery("document")]
    )
    assert parse_query("first doc OR document") == OrQuery(
        [AndQuery([WordQuery("first"), WordQuery("doc")]), WordQuery("document")]
    )


def test_brackets():
    assert parse_query("(first)") == WordQuery("first")
    assert parse_query("first and (doc or document)") == AndQuery(
        [WordQuery("first"), OrQuery([WordQuery("doc"), WordQuery("document")])]
    )
    assert parse_query("first (doc or document)") == AndQuery(
        [WordQuery("first"), OrQuery([WordQuery("doc"), WordQuery("document")])]
    )
    with pytest.raises(InvalidQueryException):
        parse_query("first (doc or document")


def test_quotes():
    assert parse_query('"first document"') == PhraseQuery("first document")
    assert parse_query('"Hello WORLD!"') == PhraseQuery("Hello WORLD!")


def test_quotes_and_brackets():
    assert parse_query('not (hello "first :) document")') == NotQuery(
        AndQuery([WordQuery("hello"), PhraseQuery("first :) document")])
    )


def test_extra_quotes():
    with pytest.raises(InvalidQueryException):
        parse_query('"hello " document"')


def test_escaped_quotes():
    assert parse_query('"hello \\" document"') == PhraseQuery('hello " document')


def test_fields():
    assert parse_query("keyword:first document") == AndQuery(
        [WordQuery("first", KeywordField()), WordQuery("document")]
    )
    assert parse_query('keyword:"first document"') == PhraseQuery("first document", KeywordField())
    with pytest.raises(InvalidQueryException):
        parse_query("keyword:(first and document)")
    assert parse_query("@keyword first document") == AndQuery(
        [WordQuery("first", KeywordField()), WordQuery("document")]
    )


def test_field_brackets():
    with pytest.raises(InvalidQueryException):
        parse_query("keywords:(deer ych)")


def test_keyword_field():
    assert parse_query("keywords:ych") == WordQuery("ych", KeywordField())
    assert parse_query("tags:deer") == WordQuery("deer", KeywordField())
    assert parse_query("tag:dragon") == WordQuery("dragon", KeywordField())


def test_title_field():
    assert parse_query("title:first") == WordQuery("first", TitleField())
    assert parse_query("TITLE: first") == WordQuery("first", TitleField())


def test_description_field():
    assert parse_query("description:first") == WordQuery("first", DescriptionField())
    assert parse_query("desc:first") == WordQuery("first", DescriptionField())
    assert parse_query("message:first") == WordQuery("first", DescriptionField())


def test_artist_field():
    assert parse_query("artist:rajii") == WordQuery("rajii", ArtistField())
    assert parse_query("author:va-artist") == WordQuery("va-artist", ArtistField())
    assert parse_query("poster:fender") == WordQuery("fender", ArtistField())
    assert parse_query("lower:fender") == WordQuery("fender", ArtistField())
    assert parse_query("uploader:zephyr42") == WordQuery("zephyr42", ArtistField())


def test_rating_field():
    assert parse_query("rating:general") == RatingQuery(Rating.GENERAL)
    assert parse_query("-rating:adult") == NotQuery(RatingQuery(Rating.ADULT))
    assert parse_query("ych rating:mature") == AndQuery([WordQuery("ych"), RatingQuery(Rating.MATURE)])


def test_rating_quote():
    with pytest.raises(InvalidQueryException):
        parse_query('rating:"general adult"')


def test_rating_invalid():
    with pytest.raises(InvalidQueryException):
        parse_query("rating:alright")


def test_invalid_field():
    with pytest.raises(InvalidQueryException):
        parse_query("fake:first")


def test_prefix():
    assert parse_query("first*") == PrefixQuery("first")


def test_suffix():
    assert parse_query("*first") == SuffixQuery("first")


def test_regex():
    assert parse_query("fi*st") == RegexQuery.from_string_with_asterisks("fi*st")
    assert parse_query("*fi*st*") == RegexQuery.from_string_with_asterisks("*fi*st*")


def test_exception():
    assert parse_query("multi* except multitude") == ExceptionQuery(
        PrefixQuery("multi"), LocationOrQuery([WordQuery("multitude")])
    )


def test_exceptions():
    assert parse_query('multi* except (multitude or "no multi" or multicol*)') == ExceptionQuery(
        PrefixQuery("multi"),
        LocationOrQuery([WordQuery("multitude"), PhraseQuery("no multi"), PrefixQuery("multicol")]),
    )


def test_field_exceptions():
    assert parse_query("keywords:multi* except multicol*") == ExceptionQuery(
        PrefixQuery("multi", KeywordField()),
        LocationOrQuery([PrefixQuery("multicol", KeywordField())]),
    )


def test_field_exceptions_brackets():
    assert parse_query("keywords:(multi* except multicol*)") == ExceptionQuery(
        PrefixQuery("multi", KeywordField()),
        LocationOrQuery([PrefixQuery("multicol", KeywordField())]),
    )


def test_field_exception_invalid():
    with pytest.raises(InvalidQueryException):
        parse_query("multi* except keywords:multitude")


def test_keyword_exception_invalid():
    with pytest.raises(InvalidQueryException):
        q = parse_query("multi* except (multiple and multitude)")
        print(q)
    with pytest.raises(InvalidQueryException):
        parse_query("multi* except (not multitude)")


def test_word_starting_not():
    assert parse_query("notice") == WordQuery("notice")
