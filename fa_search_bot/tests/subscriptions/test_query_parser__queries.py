from fa_search_bot.subscriptions.query_parser import (
    AndQuery,
    ArtistField,
    DescriptionField,
    ExceptionQuery,
    FieldLocation,
    KeywordField,
    LocationOrQuery,
    MatchLocation,
    NotQuery,
    OrQuery,
    PhraseQuery,
    PrefixQuery,
    RatingQuery,
    RegexQuery,
    SuffixQuery,
    TitleField,
    WordQuery,
)
from fa_search_bot.sites.furaffinity.fa_submission import FAUser
from fa_search_bot.sites.submission import Rating
from fa_search_bot.tests.util.submission_builder import SubmissionBuilder


def test_match_location_overlaps():
    location1 = MatchLocation(FieldLocation("title"), 0, 5)
    location2 = MatchLocation(FieldLocation("title"), 4, 8)

    assert location1.overlaps(location2)


def test_match_location_overlaps__tips_touch():
    location1 = MatchLocation(FieldLocation("title"), 0, 5)
    location2 = MatchLocation(FieldLocation("title"), 5, 8)

    assert not location1.overlaps(location2)


def test_match_location_overlaps__no_touch():
    location1 = MatchLocation(FieldLocation("title"), 0, 5)
    location2 = MatchLocation(FieldLocation("title"), 6, 8)

    assert not location1.overlaps(location2)


def test_match_location_overlaps__different_field():
    location1 = MatchLocation(FieldLocation("title"), 0, 5)
    location2 = MatchLocation(FieldLocation("description"), 4, 8)

    assert not location1.overlaps(location2)


def test_match_location_overlaps__one_envelopes_two():
    location1 = MatchLocation(FieldLocation("title"), 0, 10)
    location2 = MatchLocation(FieldLocation("title"), 4, 8)

    assert location1.overlaps(location2)


def test_match_location_overlaps__two_envelopes_one():
    location1 = MatchLocation(FieldLocation("title"), 6, 7)
    location2 = MatchLocation(FieldLocation("title"), 4, 8)

    assert location1.overlaps(location2)


def test_match_location_overlaps_any():
    location1 = MatchLocation(FieldLocation("title"), 0, 6)
    location2 = MatchLocation(FieldLocation("title"), 4, 8)
    location3 = MatchLocation(FieldLocation("description"), 4, 8)

    assert location1.overlaps_any([location2, location3])


def test_match_location_overlaps_any__overlaps_all():
    location1 = MatchLocation(FieldLocation("title"), 0, 6)
    location2 = MatchLocation(FieldLocation("title"), 0, 4)
    location3 = MatchLocation(FieldLocation("title"), 5, 8)

    assert location1.overlaps_any([location2, location3])


def test_match_location_overlaps_any__overlaps_none():
    location1 = MatchLocation(FieldLocation("title"), 0, 4)
    location2 = MatchLocation(FieldLocation("title"), 5, 8)
    location3 = MatchLocation(FieldLocation("title"), 10, 15)

    assert not location1.overlaps_any([location2, location3])


def test_match_location_overlaps_any__overlaps_only_in_other_fields():
    location1 = MatchLocation(FieldLocation("title"), 0, 6)
    location2 = MatchLocation(FieldLocation("description"), 0, 4)
    location3 = MatchLocation(FieldLocation("description"), 5, 8)

    assert not location1.overlaps_any([location2, location3])


def test_or_query__both():
    submission = SubmissionBuilder(rating=Rating.GENERAL, title="test").build_full_submission()
    query = OrQuery([RatingQuery(Rating.GENERAL), WordQuery("test", TitleField())])

    assert query.matches_submission(submission)


def test_or_query__first():
    submission = SubmissionBuilder(rating=Rating.GENERAL, title="test").build_full_submission()
    query = OrQuery([RatingQuery(Rating.GENERAL), WordQuery("title", TitleField())])

    assert query.matches_submission(submission)


def test_or_query__second():
    submission = SubmissionBuilder(rating=Rating.ADULT, title="test").build_full_submission()
    query = OrQuery([RatingQuery(Rating.GENERAL), WordQuery("test", TitleField())])

    assert query.matches_submission(submission)


def test_or_query__neither():
    submission = SubmissionBuilder(rating=Rating.ADULT, title="title").build_full_submission()
    query = OrQuery([RatingQuery(Rating.GENERAL), WordQuery("test", TitleField())])

    assert not query.matches_submission(submission)


def test_or_query__many():
    submission = SubmissionBuilder(
        rating=Rating.GENERAL, title="test", description="An example submission"
    ).build_full_submission()
    query = OrQuery(
        [
            RatingQuery(Rating.GENERAL),
            WordQuery("test", TitleField()),
            WordQuery("example"),
            WordQuery("an"),
            WordQuery("submission", DescriptionField()),
        ]
    )

    assert query.matches_submission(submission)


def test_or_query__one_of_many():
    submission = SubmissionBuilder(
        rating=Rating.GENERAL, title="test", description="An example submission"
    ).build_full_submission()
    query = OrQuery(
        [
            RatingQuery(Rating.MATURE),
            WordQuery("dragon", TitleField()),
            WordQuery("gibberish"),
            WordQuery("an"),
            WordQuery("deer", DescriptionField()),
        ]
    )

    assert query.matches_submission(submission)


def test_or_query__none_of_many():
    submission = SubmissionBuilder(
        rating=Rating.GENERAL, title="test", description="An example submission"
    ).build_full_submission()
    query = OrQuery(
        [
            RatingQuery(Rating.MATURE),
            WordQuery("dragon", TitleField()),
            WordQuery("gibberish"),
            WordQuery("ann"),
            WordQuery("deer", DescriptionField()),
        ]
    )

    assert not query.matches_submission(submission)


def test_location_or_query__no_matches():
    submission = SubmissionBuilder(
        title="test",
        description="An example submission",
        keywords=["submission", "test"],
    ).build_full_submission()
    query = LocationOrQuery(
        [
            WordQuery("title", TitleField()),
            WordQuery("example", KeywordField()),
            WordQuery("deer"),
        ]
    )

    locations = query.match_locations(submission)

    assert len(locations) == 0


def test_location_or_query__one_subquery_one_match():
    submission = SubmissionBuilder(
        title="test",
        description="An example submission",
        keywords=["submission", "test"],
    ).build_full_submission()
    query = LocationOrQuery(
        [
            WordQuery("example"),
        ]
    )

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert locations[0] == MatchLocation(FieldLocation("description"), 3, 10)


def test_location_or_query__two_subqueries_different_matches():
    submission = SubmissionBuilder(
        title="test",
        description="An example submission",
        keywords=["submission", "deer"],
    ).build_full_submission()
    query = LocationOrQuery([WordQuery("example"), WordQuery("deer")])

    locations = query.match_locations(submission)

    assert len(locations) == 2
    assert MatchLocation(FieldLocation("description"), 3, 10) in locations
    assert MatchLocation(FieldLocation("keyword_1"), 0, 4)


def test_location_or_query__two_subqueries_overlapping_matches():
    submission = SubmissionBuilder(
        title="test",
        description="An example submission",
        keywords=["submission", "test"],
    ).build_full_submission()
    query = LocationOrQuery([PrefixQuery("exam"), PhraseQuery("an example")])

    locations = query.match_locations(submission)

    assert len(locations) == 2
    assert MatchLocation(FieldLocation("description"), 3, 10) in locations
    assert MatchLocation(FieldLocation("description"), 0, 10) in locations


def test_location_or_query__many_subqueries_many_matches_not_all_though():
    submission = SubmissionBuilder(
        title="test",
        description="An example submission",
        keywords=["submission", "test"],
    ).build_full_submission()
    query = LocationOrQuery(
        [
            WordQuery("title", TitleField()),
            WordQuery("example", DescriptionField()),
            WordQuery("test"),
            WordQuery("submission", KeywordField()),
            WordQuery("deer"),
        ]
    )

    locations = query.match_locations(submission)

    assert len(locations) == 4
    assert MatchLocation(FieldLocation("description"), 3, 10) in locations
    assert MatchLocation(FieldLocation("title"), 0, 4) in locations
    assert MatchLocation(FieldLocation("keyword_1"), 0, 4) in locations
    assert MatchLocation(FieldLocation("keyword_0"), 0, 10) in locations


def test_and_query__both():
    submission = SubmissionBuilder(rating=Rating.GENERAL, title="test").build_full_submission()
    query = AndQuery([RatingQuery(Rating.GENERAL), WordQuery("test", TitleField())])

    assert query.matches_submission(submission)


def test_and_query__first():
    submission = SubmissionBuilder(rating=Rating.GENERAL, title="test").build_full_submission()
    query = AndQuery([RatingQuery(Rating.GENERAL), WordQuery("title", TitleField())])

    assert not query.matches_submission(submission)


def test_and_query__second():
    submission = SubmissionBuilder(rating=Rating.ADULT, title="test").build_full_submission()
    query = AndQuery([RatingQuery(Rating.GENERAL), WordQuery("test", TitleField())])

    assert not query.matches_submission(submission)


def test_and_query__neither():
    submission = SubmissionBuilder(rating=Rating.ADULT, title="title").build_full_submission()
    query = AndQuery([RatingQuery(Rating.GENERAL), WordQuery("test", TitleField())])

    assert not query.matches_submission(submission)


def test_and_query__many():
    submission = SubmissionBuilder(
        rating=Rating.GENERAL, title="test", description="An example submission"
    ).build_full_submission()
    query = AndQuery(
        [
            RatingQuery(Rating.GENERAL),
            WordQuery("test", TitleField()),
            WordQuery("example"),
            WordQuery("an"),
            WordQuery("submission", DescriptionField()),
        ]
    )

    assert query.matches_submission(submission)


def test_and_query__many_except_one():
    submission = SubmissionBuilder(
        rating=Rating.GENERAL, title="test", description="An example submission"
    ).build_full_submission()
    query = AndQuery(
        [
            RatingQuery(Rating.GENERAL),
            WordQuery("test", TitleField()),
            WordQuery("example"),
            WordQuery("ann"),
            WordQuery("submission", DescriptionField()),
        ]
    )

    assert not query.matches_submission(submission)


def test_and_query__many_no_match():
    submission = SubmissionBuilder(
        rating=Rating.GENERAL, title="test", description="An example submission"
    ).build_full_submission()
    query = AndQuery(
        [
            RatingQuery(Rating.ADULT),
            WordQuery("dragon", TitleField()),
            WordQuery("gibberish"),
            WordQuery("ann"),
            WordQuery("deer", DescriptionField()),
        ]
    )

    assert not query.matches_submission(submission)


def test_not_query():
    submission = SubmissionBuilder(rating=Rating.ADULT).build_full_submission()
    query = NotQuery(RatingQuery(Rating.GENERAL))

    assert query.matches_submission(submission)


def test_not_query__no_match():
    submission = SubmissionBuilder(rating=Rating.GENERAL).build_full_submission()
    query = NotQuery(RatingQuery(Rating.GENERAL))

    assert not query.matches_submission(submission)


def test_rating_query():
    submission = SubmissionBuilder(rating=Rating.GENERAL).build_full_submission()
    query = RatingQuery(Rating.GENERAL)

    assert query.matches_submission(submission)


def test_rating_query__no_match():
    submission = SubmissionBuilder(rating=Rating.MATURE).build_full_submission()
    query = RatingQuery(Rating.GENERAL)

    assert not query.matches_submission(submission)


def test_word_query__title():
    submission = SubmissionBuilder(
        title="test", description="hello world", keywords=["query", "example"]
    ).build_full_submission()
    query = WordQuery("test")

    assert query.matches_submission(submission)


def test_word_query__description():
    submission = SubmissionBuilder(
        title="test", description="hello world", keywords=["query", "example"]
    ).build_full_submission()
    query = WordQuery("hello")

    assert query.matches_submission(submission)


def test_word_query__keywords():
    submission = SubmissionBuilder(
        title="test", description="hello world", keywords=["query", "example"]
    ).build_full_submission()
    query = WordQuery("query")

    assert query.matches_submission(submission)


def test_word_query__no_match():
    submission = SubmissionBuilder(
        title="test", description="hello world", keywords=["query", "example"]
    ).build_full_submission()
    query = WordQuery("testing")

    assert not query.matches_submission(submission)


def test_word_query__all_fields():
    submission = SubmissionBuilder(
        title="test",
        description="hello test world",
        keywords=["query", "example", "test"],
    ).build_full_submission()
    query = WordQuery("test")

    assert query.matches_submission(submission)


def test_word_query__punctuation_title():
    submission = SubmissionBuilder(
        title="test!", description="hello world", keywords=["query", "example"]
    ).build_full_submission()
    query = WordQuery("test")

    assert query.matches_submission(submission)


def test_word_query__punctuation_description():
    submission = SubmissionBuilder(
        title="test", description="hello, world", keywords=["query", "example"]
    ).build_full_submission()
    query = WordQuery("hello")

    assert query.matches_submission(submission)


def test_word_query__includes_artist():
    """
    Previously, this tested that word queries would not include the artist, but this is not how users understood the
    subscription system to work, so word queries now include the artist.
    """
    submission = SubmissionBuilder(
        title="test",
        description="hello world",
        keywords=["query", "example"],
        author=FAUser("Testing", "testing"),
    ).build_full_submission()
    query = WordQuery("testing")

    assert query.matches_submission(submission)


def test_word_query__keyword_non_concat():
    submission = SubmissionBuilder(
        title="test", description="hello world", keywords=["test", "thing"]
    ).build_full_submission()
    query = WordQuery("testthing")

    assert not query.matches_submission(submission)


def test_word_query__keyword_punctuation():
    submission = SubmissionBuilder(
        title="test", description="hello world", keywords=["test", "thing!"]
    ).build_full_submission()
    query = WordQuery("thing")

    assert query.matches_submission(submission)


def test_word_query__hyphenated_match():
    submission = SubmissionBuilder(
        title="test", description="hello-world", keywords=["test", "thing"]
    ).build_full_submission()
    query = WordQuery("hello-world")

    assert query.matches_submission(submission)


def test_word_query__hyphenated_no_match_part():
    submission = SubmissionBuilder(
        title="test", description="hello-world", keywords=["test", "thing"]
    ).build_full_submission()
    query = WordQuery("hello")

    assert not query.matches_submission(submission)


def test_word_query__not_word_part():
    submission = SubmissionBuilder(
        title="test", description="hello-world", keywords=["testing", "things"]
    ).build_full_submission()
    query = WordQuery("thing")

    assert not query.matches_submission(submission)


def test_word_query__title_field():
    submission = SubmissionBuilder(
        title="test", description="hello world", keywords=["query", "thing"]
    ).build_full_submission()
    query = WordQuery("test", TitleField())

    assert query.matches_submission(submission)


def test_word_query__title_field_no_match():
    submission = SubmissionBuilder(
        title="test", description="hello world", keywords=["query", "thing"]
    ).build_full_submission()
    query = WordQuery("hello", TitleField())

    assert not query.matches_submission(submission)


def test_word_query__description_field():
    submission = SubmissionBuilder(
        title="test", description="hello world", keywords=["query", "thing"]
    ).build_full_submission()
    query = WordQuery("hello", DescriptionField())

    assert query.matches_submission(submission)


def test_word_query__description_field_no_match():
    submission = SubmissionBuilder(
        title="test", description="hello world", keywords=["query", "thing"]
    ).build_full_submission()
    query = WordQuery("test", DescriptionField())

    assert not query.matches_submission(submission)


def test_word_query__keyword_field():
    submission = SubmissionBuilder(
        title="test", description="hello world", keywords=["query", "thing"]
    ).build_full_submission()
    query = WordQuery("query", KeywordField())

    assert query.matches_submission(submission)


def test_word_query__keyword_field_no_match():
    submission = SubmissionBuilder(
        title="test", description="hello world", keywords=["query", "thing"]
    ).build_full_submission()
    query = WordQuery("hello", KeywordField())

    assert not query.matches_submission(submission)


def test_word_query__artist_field():
    submission = SubmissionBuilder(
        title="test",
        description="hello world",
        keywords=["query", "thing"],
        author=FAUser("TestAccount", "testaccount"),
    ).build_full_submission()
    query = WordQuery("testaccount", ArtistField())

    assert query.matches_submission(submission)


def test_word_query__artist_field_no_match():
    submission = SubmissionBuilder(
        title="test",
        description="hello world",
        keywords=["query", "thing"],
        author=FAUser("Abe_E_Seedy", "abeeseedy"),
    ).build_full_submission()
    query = WordQuery("hello", ArtistField())

    assert not query.matches_submission(submission)


def test_word_query__artist_field_full_name():
    submission = SubmissionBuilder(
        title="test",
        description="hello world",
        keywords=["query", "thing"],
        author=FAUser("Abe_E_Seedy", "abeeseedy"),
    ).build_full_submission()
    query = WordQuery("Abe_E_Seedy", ArtistField())

    assert query.matches_submission(submission)


def test_word_query__artist_field_punctuation():
    submission = SubmissionBuilder(
        title="test",
        description="hello world",
        keywords=["query", "thing"],
        author=FAUser("-Sebastian-", "-sebastian-"),
    ).build_full_submission()
    query = WordQuery("-sebastian-", ArtistField())

    assert query.matches_submission(submission)


def test_word_query__locations_title():
    submission = SubmissionBuilder(
        title="test", description="hello world", keywords=["query", "thing"]
    ).build_full_submission()
    query = WordQuery("test")

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert locations[0].field == FieldLocation("title")
    assert locations[0].start_position == 0
    assert locations[0].end_position == 4


def test_word_query__locations_description():
    submission = SubmissionBuilder(
        title="test", description="hello world", keywords=["query", "thing"]
    ).build_full_submission()
    query = WordQuery("world")

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert locations[0].field == FieldLocation("description")
    assert locations[0].start_position == 6
    assert locations[0].end_position == 11


def test_word_query__locations_keywords():
    submission = SubmissionBuilder(
        title="test", description="hello world", keywords=["query", "thing"]
    ).build_full_submission()
    query = WordQuery("thing")

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert locations[0].field == FieldLocation("keyword_1")
    assert locations[0].start_position == 0
    assert locations[0].end_position == 5


def test_word_query__locations_multiple():
    submission = SubmissionBuilder(
        title="test", description="hello test", keywords=["query", "test"]
    ).build_full_submission()
    query = WordQuery("test")

    locations = query.match_locations(submission)

    assert len(locations) == 3
    assert MatchLocation(FieldLocation("title"), 0, 4) in locations
    assert MatchLocation(FieldLocation("description"), 6, 10) in locations
    assert MatchLocation(FieldLocation("keyword_1"), 0, 4) in locations


def test_word_query__locations_none():
    submission = SubmissionBuilder(
        title="test", description="hello world", keywords=["query", "word"]
    ).build_full_submission()
    query = WordQuery("fred")

    locations = query.match_locations(submission)

    assert len(locations) == 0


def test_word_query__locations_no_match_hypenated():
    submission = SubmissionBuilder(
        title="test", description="hello-world", keywords=["test", "thing"]
    ).build_full_submission()
    query = WordQuery("hello")

    locations = query.match_locations(submission)

    assert len(locations) == 0


def test_word_query__detects_name_in_link():
    submission = SubmissionBuilder(
        description='<a href="/user/zephyr42" class="linkusername">zephyr42</a>'
    ).build_full_submission()
    query = WordQuery("zephyr42")

    assert query.matches_submission(submission)


def test_word_query__detects_name_in_icon():
    submission = SubmissionBuilder(
        description=(
            '<a href="/user/zephyr42" class="iconusername"><img src="//a.furaffinity.net/20210221/zephyr42.gif" '
            'align="middle" title="zephyr42" alt="zephyr42"></a>'
        )
    ).build_full_submission()
    query = WordQuery("zephyr42")

    assert query.matches_submission(submission)


def test_word_query__detects_name_in_icon_with_link():
    submission = SubmissionBuilder(
        description=(
            '<a href="/user/zephyr42" class="iconusername"><img src="//a.furaffinity.net/20210221/zephyr42.gif" '
            'align="middle" title="zephyr42" alt="zephyr42"> zephyr42</a>'
        )
    ).build_full_submission()
    query = WordQuery("zephyr42")

    assert query.matches_submission(submission)


def test_prefix_query():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PrefixQuery("sub")

    assert query.matches_submission(submission)


def test_prefix_query__no_match():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PrefixQuery("deer")

    assert not query.matches_submission(submission)


def test_prefix_query__inside_word():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PrefixQuery("miss")

    assert not query.matches_submission(submission)


def test_prefix_query__location():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PrefixQuery("sub")

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert locations[0] == MatchLocation(FieldLocation("description"), 21, 31)


def test_prefix_query__location_many_matches():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example testing",
        keywords=["test", "thing", "tes"],
    ).build_full_submission()
    query = PrefixQuery("tes")

    locations = query.match_locations(submission)

    assert len(locations) == 3
    assert MatchLocation(FieldLocation("title"), 0, 4) in locations
    assert MatchLocation(FieldLocation("keyword_0"), 0, 4) in locations
    assert MatchLocation(FieldLocation("description"), 21, 28) in locations
    assert MatchLocation(FieldLocation("keyword_2"), 0, 3) not in locations


def test_prefix_query__location_no_match():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PrefixQuery("miss")

    locations = query.match_locations(submission)

    assert len(locations) == 0


def test_prefix_query__no_match_full_word():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example deer submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PrefixQuery("deer")

    assert not query.matches_submission(submission)


def test_prefix_query__follow_hyphen():
    submission = SubmissionBuilder(
        title="test",
        description="hello-world, example deer submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PrefixQuery("hello")

    assert query.matches_submission(submission)


def test_prefix_query__location_follow_hyphen():
    submission = SubmissionBuilder(
        title="test",
        description="hello-world, example deer submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PrefixQuery("hell")

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert locations[0] == MatchLocation(FieldLocation("description"), 0, 11)


def test_prefix_query__location_dont_include_punctuation():
    submission = SubmissionBuilder(
        title="test",
        description="hello, world. example deer submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PrefixQuery("worl")

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert locations[0] == MatchLocation(FieldLocation("description"), 7, 12)


def test_prefix_query__dont_follow_punctuation():
    submission = SubmissionBuilder(
        title="test",
        description="hello,world, example deer submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PrefixQuery("hello")

    assert not query.matches_submission(submission)


def test_prefix_query__location_dont_follow_punctuation():
    submission = SubmissionBuilder(
        title="test",
        description="hello,world, example deer submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PrefixQuery("hell")

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert locations[0] == MatchLocation(FieldLocation("description"), 0, 5)


def test_prefix_query__allow_punctuation_before():
    submission = SubmissionBuilder(
        title="test",
        description="hello,world, example deer submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PrefixQuery("worl")

    assert query.matches_submission(submission)


def test_prefix_query__location_allow_punctuation_before():
    submission = SubmissionBuilder(
        title="test",
        description="hello,world, example deer submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PrefixQuery("worl")

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert locations[0] == MatchLocation(FieldLocation("description"), 6, 11)


def test_prefix_query__field():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PrefixQuery("worl", DescriptionField())

    assert query.matches_submission(submission)


def test_prefix_query__location_field():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PrefixQuery("worl", DescriptionField())

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert MatchLocation(FieldLocation("description"), 6, 11) in locations


def test_prefix_query__field_no_match():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PrefixQuery("blah", DescriptionField())

    assert not query.matches_submission(submission)


def test_prefix_query__location_field_no_match():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PrefixQuery("blah", DescriptionField())

    locations = query.match_locations(submission)

    assert len(locations) == 0


def test_prefix_query__field_only_match_other_field():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PrefixQuery("tes", DescriptionField())

    assert not query.matches_submission(submission)


def test_prefix_query__location_field_only_match_other_field():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PrefixQuery("tes", DescriptionField())

    locations = query.match_locations(submission)

    assert len(locations) == 0


def test_prefix_query__field_match_ignore_other():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PrefixQuery("tes", TitleField())

    assert query.matches_submission(submission)


def test_prefix_query__location_field_match_ignore_other():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PrefixQuery("tes", TitleField())

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert MatchLocation(FieldLocation("title"), 0, 4) in locations
    assert MatchLocation(FieldLocation("keyword_0"), 0, 4) not in locations


def test_suffix_query():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = SuffixQuery("ion")

    assert query.matches_submission(submission)


def test_suffix_query__no_match():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = SuffixQuery("deer")

    assert not query.matches_submission(submission)


def test_suffix_query__inside_word():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = SuffixQuery("miss")

    assert not query.matches_submission(submission)


def test_suffix_query__location():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = SuffixQuery("ion")

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert locations[0] == MatchLocation(FieldLocation("description"), 21, 31)


def test_suffix_query__location_many_matches():
    submission = SubmissionBuilder(
        title="titling",
        description="hello world, example testing",
        keywords=["trying", "test", "ing"],
    ).build_full_submission()
    query = SuffixQuery("ing")

    locations = query.match_locations(submission)

    assert len(locations) == 3
    assert MatchLocation(FieldLocation("title"), 0, 7) in locations
    assert MatchLocation(FieldLocation("keyword_0"), 0, 6) in locations
    assert MatchLocation(FieldLocation("description"), 21, 28) in locations
    assert MatchLocation(FieldLocation("keyword_2"), 0, 3) not in locations


def test_suffix_query__location_no_match():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = SuffixQuery("miss")

    locations = query.match_locations(submission)

    assert len(locations) == 0


def test_suffix_query__no_match_full_word():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example deer submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = SuffixQuery("deer")

    assert not query.matches_submission(submission)


def test_suffix_query__follow_hyphen():
    submission = SubmissionBuilder(
        title="test",
        description="hello-world, example deer submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = SuffixQuery("world")

    assert query.matches_submission(submission)


def test_suffix_query__location_follow_hyphen():
    submission = SubmissionBuilder(
        title="test",
        description="hello-world, example deer submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = SuffixQuery("orld")

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert locations[0] == MatchLocation(FieldLocation("description"), 0, 11)


def test_suffix_query__location_dont_include_punctuation():
    submission = SubmissionBuilder(
        title="test",
        description="hello ,world example deer submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = SuffixQuery("orld")

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert locations[0] == MatchLocation(FieldLocation("description"), 7, 12)


def test_suffix_query__dont_follow_punctuation():
    submission = SubmissionBuilder(
        title="test",
        description="hello,world, example deer submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = SuffixQuery("world")

    assert not query.matches_submission(submission)


def test_suffix_query__location_dont_follow_punctuation():
    submission = SubmissionBuilder(
        title="test",
        description="hello,world, example deer submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = SuffixQuery("orld")

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert locations[0] == MatchLocation(FieldLocation("description"), 6, 11)


def test_suffix_query__allow_punctuation_after():
    submission = SubmissionBuilder(
        title="test",
        description="hello,world, example deer submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = SuffixQuery("orld")

    assert query.matches_submission(submission)


def test_suffix_query__location_allow_punctuation_after():
    submission = SubmissionBuilder(
        title="test",
        description="hello,world, example deer submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = SuffixQuery("orld")

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert locations[0] == MatchLocation(FieldLocation("description"), 6, 11)


def test_suffix_query__field_no_match():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = SuffixQuery("blah", DescriptionField())

    assert not query.matches_submission(submission)


def test_suffix_query__location_field_no_match():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = SuffixQuery("blah", DescriptionField())

    locations = query.match_locations(submission)

    assert len(locations) == 0


def test_suffix_query__field_only_match_other_field():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = SuffixQuery("est", DescriptionField())

    assert not query.matches_submission(submission)


def test_suffix_query__location_field_only_match_other_field():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = SuffixQuery("est", DescriptionField())

    locations = query.match_locations(submission)

    assert len(locations) == 0


def test_suffix_query__field_match_ignore_other():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = SuffixQuery("est", TitleField())

    assert query.matches_submission(submission)


def test_suffix_query__location_field_match_ignore_other():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = SuffixQuery("est", TitleField())

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert MatchLocation(FieldLocation("title"), 0, 4) in locations
    assert MatchLocation(FieldLocation("keyword_0"), 0, 4) not in locations


def test_regex_query():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("he*o")

    assert query.matches_submission(submission)


def test_regex_query__location():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("he*o")

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert locations[0] == MatchLocation(FieldLocation("description"), 0, 5)


def test_regex_query__no_match():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("de*r")

    assert not query.matches_submission(submission)


def test_regex_query__location_no_match():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("de*r")

    locations = query.match_locations(submission)

    assert len(locations) == 0


def test_regex_query__inside_word():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("bm*io")

    assert not query.matches_submission(submission)


def test_regex_query__location_inside_word():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("bm*io")

    locations = query.match_locations(submission)

    assert len(locations) == 0


def test_regex_query__infix():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("*miss*")

    assert query.matches_submission(submission)


def test_regex_query__location_infix():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("*miss*")

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert locations[0] == MatchLocation(FieldLocation("description"), 21, 31)


def test_regex_query__follow_hyphen():
    submission = SubmissionBuilder(
        title="test",
        description="hello-world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("he*orld")

    assert query.matches_submission(submission)


def test_regex_query__location_follow_hyphen():
    submission = SubmissionBuilder(
        title="test",
        description="hello-world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("he*orld")

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert locations[0] == MatchLocation(FieldLocation("description"), 0, 11)


def test_regex_query__infix_follow_hyphen():
    submission = SubmissionBuilder(
        title="test",
        description="hello-world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("*llo*")

    assert query.matches_submission(submission)


def test_regex_query__location_infix_follow_hyphen():
    submission = SubmissionBuilder(
        title="test",
        description="hello-world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("*llo*")

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert locations[0] == MatchLocation(FieldLocation("description"), 0, 11)


def test_regex_query__dont_follow_punctuation():
    submission = SubmissionBuilder(
        title="test",
        description="hello,world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("he*ld")

    assert not query.matches_submission(submission)


def test_regex_query__location_dont_follow_punctuation():
    submission = SubmissionBuilder(
        title="test",
        description="hello,world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("he*ld")

    locations = query.match_locations(submission)

    assert len(locations) == 0


def test_regex_query__infix_dont_follow_punctuation():
    submission = SubmissionBuilder(
        title="test",
        description="hello,world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("*llo*")

    assert not query.matches_submission(submission)


def test_regex_query__location_infix_dont_follow_punctuation():
    submission = SubmissionBuilder(
        title="test",
        description="hello,world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("*llo*")

    locations = query.match_locations(submission)

    assert len(locations) == 0


def test_regex_query__location_infix_dont_include_punctuation():
    submission = SubmissionBuilder(
        title="test",
        description="hello, world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("*ll*")

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert locations[0] == MatchLocation(FieldLocation("description"), 0, 5)


def test_regex_query__infix_doesnt_match_word():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("*world*")

    assert not query.matches_submission(submission)


def test_regex_query__location_infix_doesnt_match_word():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("*world*")

    locations = query.match_locations(submission)

    assert len(locations) == 0


def test_regex_query__asterisk_matches_at_least_one_character():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("wor*ld")

    assert not query.matches_submission(submission)


def test_regex_query__location_asterisk_matches_at_least_one_character():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("wor*ld")

    locations = query.match_locations(submission)

    assert len(locations) == 0


def test_regex_query__double_asterisk_matches_one_character():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("wo**ld")

    assert query.matches_submission(submission)


def test_regex_query__location_double_asterisk_matches_one_character():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("wo**ld")

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert locations[0] == MatchLocation(FieldLocation("description"), 6, 11)


def test_regex_query__field():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("wo*ld", DescriptionField())

    assert query.matches_submission(submission)


def test_regex_query__location_field():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("wo*ld", DescriptionField())

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert locations[0] == MatchLocation(FieldLocation("description"), 6, 11)


def test_regex_query__field_no_match():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("de*r", DescriptionField())

    assert not query.matches_submission(submission)


def test_regex_query__location_field_no_match():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("de*r", DescriptionField())

    locations = query.match_locations(submission)

    assert len(locations) == 0


def test_regex_query__field_only_match_other_field():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("t*t", DescriptionField())

    assert not query.matches_submission(submission)


def test_regex_query__location_field_only_match_other_field():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("t*t", DescriptionField())

    locations = query.match_locations(submission)

    assert len(locations) == 0


def test_regex_query__match_ignore_other():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("t*t", TitleField())

    assert query.matches_submission(submission)


def test_regex_query__location_match_ignore_other():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = RegexQuery.from_string_with_asterisks("t*t", TitleField())

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert locations[0] == MatchLocation(FieldLocation("title"), 0, 4)


def test_phrase_query():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PhraseQuery("hello world")

    assert query.matches_submission(submission)


def test_phrase_query__location():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PhraseQuery("hello world")

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert locations[0] == MatchLocation(FieldLocation("description"), 0, 11)


def test_phrase_query__no_match():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PhraseQuery("love deer")

    assert not query.matches_submission(submission)


def test_phrase_query__location_no_match():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PhraseQuery("love deer")

    locations = query.match_locations(submission)

    assert len(locations) == 0


def test_phrase_query__dont_match_inside_words():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PhraseQuery("ple sub")

    assert not query.matches_submission(submission)


def test_phrase_query__location_dont_match_inside_words():
    submission = SubmissionBuilder(
        title="test",
        description="hello world, example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PhraseQuery("ple sub")

    locations = query.match_locations(submission)

    assert len(locations) == 0


def test_phrase_query__include_punctuation():
    submission = SubmissionBuilder(
        title="test",
        description="hello, world. example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PhraseQuery("hello, world.")

    assert query.matches_submission(submission)


def test_phrase_query__location_include_punctuation():
    submission = SubmissionBuilder(
        title="test",
        description="hello, world. example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PhraseQuery("hello, world.")

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert locations[0] == MatchLocation(FieldLocation("description"), 0, 13)


def test_phrase_query__require_punctuation():
    submission = SubmissionBuilder(
        title="test",
        description="hello world example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PhraseQuery("hello, world.")

    assert not query.matches_submission(submission)


def test_phrase_query__location_require_punctuation():
    submission = SubmissionBuilder(
        title="test",
        description="hello world example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PhraseQuery("hello, world.")

    locations = query.match_locations(submission)

    assert len(locations) == 0


def test_phrase_query__allow_punctuation_at_ends():
    submission = SubmissionBuilder(
        title="test",
        description=".hello, world. example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PhraseQuery("hello, world")

    assert query.matches_submission(submission)


def test_phrase_query__location_allow_punctuation_at_ends():
    submission = SubmissionBuilder(
        title="test",
        description=".hello, world. example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PhraseQuery("hello, world")

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert locations[0] == MatchLocation(FieldLocation("description"), 1, 13)


def test_phrase_query__field():
    submission = SubmissionBuilder(
        title="test",
        description="hello, world. example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PhraseQuery("hello, world.", DescriptionField())

    assert query.matches_submission(submission)


def test_phrase_query__location_field():
    submission = SubmissionBuilder(
        title="test",
        description="hello, world. example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PhraseQuery("hello, world.", DescriptionField())

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert locations[0] == MatchLocation(FieldLocation("description"), 0, 13)


def test_phrase_query__field_no_match():
    submission = SubmissionBuilder(
        title="test",
        description="hello, world. example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PhraseQuery("love deer", DescriptionField())

    assert not query.matches_submission(submission)


def test_phrase_query__location_field_no_match():
    submission = SubmissionBuilder(
        title="test",
        description="hello, world. example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PhraseQuery("love deer", DescriptionField())

    locations = query.match_locations(submission)

    assert len(locations) == 0


def test_phrase_query__field_match_only_in_other_fields():
    submission = SubmissionBuilder(
        title="love deer",
        description="hello, world. example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PhraseQuery("love deer", DescriptionField())

    assert not query.matches_submission(submission)


def test_phrase_query__location_field_match_only_in_other_fields():
    submission = SubmissionBuilder(
        title="love deer",
        description="hello, world. example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PhraseQuery("love deer", DescriptionField())

    locations = query.match_locations(submission)

    assert len(locations) == 0


def test_phrase_query__field_match_ignore_other():
    submission = SubmissionBuilder(
        title="hello world",
        description="hello world. example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PhraseQuery("hello world", DescriptionField())

    assert query.matches_submission(submission)


def test_phrase_query__location_field_match_ignore_other():
    submission = SubmissionBuilder(
        title="hello world",
        description="hello world. example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = PhraseQuery("hello world", DescriptionField())

    locations = query.match_locations(submission)

    assert len(locations) == 1
    assert locations[0] == MatchLocation(FieldLocation("description"), 0, 11)


def test_exception_query__match():
    submission = SubmissionBuilder(
        title="test",
        description="hello world. example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = ExceptionQuery(PrefixQuery("hel"), WordQuery("help"))

    assert query.matches_submission(submission)


def test_exception_query__no_match():
    submission = SubmissionBuilder(
        title="test",
        description="hello world. example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = ExceptionQuery(PrefixQuery("dee"), WordQuery("deer"))

    assert not query.matches_submission(submission)


def test_exception_query__matches_except():
    submission = SubmissionBuilder(
        title="test",
        description="hello world. example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = ExceptionQuery(PrefixQuery("hel"), WordQuery("hello"))

    assert not query.matches_submission(submission)


def test_exception_query__match_many_one_except():
    submission = SubmissionBuilder(
        title="test",
        description="hello world. example help submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = ExceptionQuery(PrefixQuery("hel"), WordQuery("hello"))

    assert query.matches_submission(submission)


def test_exception_query__matches_with_superstring_except():
    submission = SubmissionBuilder(
        title="test",
        description="hello world. example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = ExceptionQuery(WordQuery("world"), PhraseQuery("hello world."))

    assert not query.matches_submission(submission)


def test_exception_query__one_match_many_except():
    submission = SubmissionBuilder(
        title="test",
        description="hello world. examplo submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = ExceptionQuery(PrefixQuery("hel"), SuffixQuery("lo"))

    assert not query.matches_submission(submission)


def test_exception_query__one_match_many_except_no_overlap():
    submission = SubmissionBuilder(
        title="test",
        description="hello world. example submission",
        keywords=["test", "thing"],
    ).build_full_submission()
    query = ExceptionQuery(PrefixQuery("hel"), WordQuery("test"))

    assert query.matches_submission(submission)
