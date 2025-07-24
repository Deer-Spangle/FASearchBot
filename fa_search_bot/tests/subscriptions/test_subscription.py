import datetime

from fa_search_bot.subscriptions.query_parser import AndQuery, NotQuery, RatingQuery, WordQuery
from fa_search_bot.sites.submission import Rating
from fa_search_bot.subscriptions.subscription import Subscription
from fa_search_bot.tests.util.submission_builder import SubmissionBuilder


def test_init():
    query = "yo"
    destination = 12432

    sub = Subscription(query, destination)

    assert sub.query_str == query
    assert sub.destination == destination
    assert sub.latest_update is None


def test_init_not_paused():
    sub = Subscription("yo", 12423)

    assert sub.paused is False


def test_matches_result__one_word_in_title_matches():
    query = "test"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="test submission",
        description="this submission is just an example",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert match


def test_matches_result__one_word_in_description_matches():
    query = "this"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="test submission",
        description="this submission is just an example",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert match


def test_matches_result__one_word_in_keywords_matches():
    query = "keywords"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="test submission",
        description="this submission is just an example",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert match


def test_matches_result__substring_in_title_no_match():
    query = "test"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="testing submission",
        description="this submission is just an example",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert not match


def test_matches_result__substring_in_description_no_match():
    query = "his"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="test submission",
        description="this submission is just an example",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert not match


def test_matches_result__substring_in_keywords_no_match():
    query = "keyword"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="test submission",
        description="this submission is just an example",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert not match


def test_matches_result__one_word_no_match():
    query = "query"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="test submission",
        description="this submission is just an example",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert not match


def test_matches_result__two_words_in_title_matches():
    query = "submission test"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="test submission",
        description="this is just an example",
        keywords=["example", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert match


def test_matches_result__two_words_in_description_matches():
    query = "example submission"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="test",
        description="this submission is just an example",
        keywords=["example", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert match


def test_matches_result__two_matching_keywords():
    query = "submission keywords"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="test",
        description="this is just an example",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert match


def test_matches_result__two_words_in_title_and_description_matches():
    query = "test this"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="test submission",
        description="this submission is just an example",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert match


def test_matches_result__two_words_in_title_and_keyword_matches():
    query = "test keywords"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="test submission",
        description="this submission is just an example",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert match


def test_matches_result__two_words_one_matches():
    query = "query"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="test submission",
        description="this submission is just an example",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert not match


def test_matches_result__case_insensitive_title():
    query = "test"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="Test submission",
        description="this submission is just an example",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert match


def test_matches_result__case_insensitive_description():
    query = "this"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="test submission",
        description="This submission is just an example",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert match


def test_matches_result__case_insensitive_keywords():
    query = "keywords"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="test submission",
        description="this submission is just an example",
        keywords=["example", "submission", "KEYWORDS"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert match


def test_matches_result__case_insensitive_query():
    query = "SUBMISSION"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="test submission",
        description="this submission is just an example",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert match


def test_matches_result__does_not_match_negated_query():
    query = "-test"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="test submission",
        description="this submission is just an example",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert not match


def test_matches_result__does_not_match_negated_query_exclamation_mark():
    query = "!test"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="test submission",
        description="this submission is just an example",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert not match


def test_matches_result__does_not_match_negated_query_case_insensitive():
    query = "-test"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="Test submission",
        description="this submission is just an example",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert not match


def test_matches_result__matches_non_applicable_negated_query():
    query = "-deer"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="Test submission",
        description="this submission is just an example",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert match


def test_matches_result__matches_query_with_non_applicable_negated_query():
    query = "test -deer"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="Test submission",
        description="this submission is just an example",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert match


def test_matches_result__does_not_match_query_with_applicable_negated_query():
    query = "test -example"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="Test submission",
        description="this submission is just an example",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert not match


def test_matches_result__does_not_match_query_with_applicable_negated_query_in_same_text():
    query = "this -example"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="Test submission",
        description="this submission is just an example",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert not match


def test_matches_result__matches_query_with_hyphen():
    query = "an-example"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="Test submission",
        description="this submission is just an-example",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert match


def test_matches_result__doesnt_match_blocklisted_tag():
    query = "an-example"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="Test submission",
        description="this submission is just an-example",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, NotQuery(WordQuery("test")))

    assert not match


def test_matches_result__phrase():
    query = '"hello world"'
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="Test submission",
        description="this submission says hello world",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert match


def test_matches_result__keywords_not_phrase():
    query = '"hello world"'
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="Test submission",
        description="this submission says hello to the world",
        keywords=["example", "hello", "world"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert not match


def test_matches_result__keyword_field():
    query = "keywords:deer"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="Test submission",
        description="this submission has no deer and will not be tagged deer",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert not match


def test_matches_result__doesnt_match_except_clause():
    query = "multi* except multitude"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="Test submission",
        description="this submission is just an example of a multitude of things",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert not match


def test_matches_result__doesnt_match_except_prefix_clause():
    query = "multi* except multicol*"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="Test submission",
        description="this submission is just an example of some multicoloured things",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert not match


def test_matches_result__doesnt_match_except_bracket_clause():
    query = "multi* except (multicol* or multitude)"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="Test submission",
        description="this submission is just an example of a multitude of multicoloured things",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert not match


def test_matches_result__except_matches_other_match():
    query = "multi* except multitude"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="Test submission",
        description="this submission is just an example of a multitude of multiple things",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert match


def test_matches_result__doesnt_match_except_quote():
    query = 'taur except "no taur"'
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="Test submission",
        description="this submission contains no taur",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert not match


def test_matches_result__doesnt_match_except_field():
    query = "keywords:(multi* except (multitude multiple multicol*))"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="Test submission",
        description="this submission is about multiplication but not tagged like that",
        keywords=["multitude", "multiple", "multicoloured", "multicolors"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert not match


def test_matches_result__not_when_paused():
    query = "test"
    subscription = Subscription(query, 12432)
    subscription.paused = True
    submission = SubmissionBuilder(
        title="test submission",
        description="this submission is just an example",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert not match


def test_matches_word_in_quotes():
    query = "deer"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="Test submission",
        description='this submission is="deer"',
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert match


def test_matches_word_in_tag():
    query = "deer"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="Test submission",
        description="this submission is <b>deer</b>",
        keywords=["example", "submission", "keywords"],
    ).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert match


def test_matches_with_rating():
    query = "deer rating:general"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(title="Deer plays in woods", rating=Rating.GENERAL).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert match


def test_matches_not_rating():
    query = "deer rating:general"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(title="Deer 'plays' in woods", rating=Rating.ADULT).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert not match


def test_matches_negative_rating():
    query = "deer -rating:general"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(title="Deer 'plays' in woods", rating=Rating.MATURE).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert match


def test_matches_not_negative_rating():
    query = "deer -rating:general"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(title="Deer plays in woods", rating=Rating.GENERAL).build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert not match


def test_matches_general_rating():
    query1 = "deer rating:general"
    query2 = "deer rating:safe"
    subscription1 = Subscription(query1, 12432)
    subscription2 = Subscription(query2, 12432)
    submission = SubmissionBuilder(title="Deer plays in woods", rating=Rating.GENERAL).build_full_submission()

    match1 = subscription1.matches_result(submission, AndQuery([]))
    match2 = subscription2.matches_result(submission, AndQuery([]))

    assert match1
    assert match2


def test_matches_mature_rating():
    query1 = "deer rating:mature"
    query2 = "deer rating:questionable"
    subscription1 = Subscription(query1, 12432)
    subscription2 = Subscription(query2, 12432)
    submission = SubmissionBuilder(title="Deer plays in woods", rating=Rating.MATURE).build_full_submission()

    match1 = subscription1.matches_result(submission, AndQuery([]))
    match2 = subscription2.matches_result(submission, AndQuery([]))

    assert match1
    assert match2


def test_matches_explicit_rating():
    query1 = "deer rating:adult"
    query2 = "deer rating:explicit"
    subscription1 = Subscription(query1, 12432)
    subscription2 = Subscription(query2, 12432)
    submission = SubmissionBuilder(title="Deer plays in woods", rating=Rating.ADULT).build_full_submission()

    match1 = subscription1.matches_result(submission, AndQuery([]))
    match2 = subscription2.matches_result(submission, AndQuery([]))

    assert match1
    assert match2


def test_matches_result__doesnt_match_blocklisted_rating():
    query = "an-example"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(
        title="Test submission",
        description="this submission is just an-example",
        keywords=["example", "submission", "keywords"],
        rating=Rating.ADULT,
    ).build_full_submission()
    blocklist = AndQuery([NotQuery(RatingQuery(Rating.ADULT)), NotQuery(RatingQuery(Rating.MATURE))])

    match = subscription.matches_result(submission, blocklist)

    assert not match


def test_matches_result__prefix_matches():
    query = "deer*"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(title="deertaur plays in woods").build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert match


def test_matches_result__prefix_matches_case_insensitive():
    query = "deer*"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(title="Deertaur plays in woods").build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert match


def test_matches_result__prefix_doesnt_match_term():
    query = "deer*"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(title="deer plays in woods").build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert not match


def test_matches_result__suffix_matches():
    query = "*taur"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(title="deertaur plays in woods").build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert match


def test_matches_result__suffix_matches_case_insensitive():
    query = "*taur"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(title="DeerTaur plays in woods").build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert match


def test_matches_result__suffix_doesnt_match_term():
    query = "*taur"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(title="taur plays in woods").build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert not match


def test_matches_result__regex_matches():
    query = "d*taur"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(title="deertaur plays in woods").build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert match


def test_matches_result__regex_matches_case_insensitive():
    query = "d*taur"
    subscription = Subscription(query, 12432)
    submission = SubmissionBuilder(title="DeerTaur plays in woods").build_full_submission()

    match = subscription.matches_result(submission, AndQuery([]))

    assert match


def test_to_json_no_updates():
    sub = Subscription("test query", -12322)

    data = sub.to_json()
    assert "query" in data
    assert data["query"] == "test query"
    assert "latest_update" in data
    assert data["latest_update"] is None


def test_to_json():
    sub = Subscription("test query", -12322)
    sub.latest_update = datetime.datetime(2019, 9, 17, 21, 8, 35)

    data = sub.to_json()
    assert "query" in data
    assert data["query"] == "test query"
    assert "latest_update" in data
    assert data["latest_update"] == "2019-09-17T21:08:35"
    assert data["paused"] is False


def test_to_json__paused():
    sub = Subscription("test query", -12322)
    sub.paused = True

    data = sub.to_json()

    assert data["paused"] is True


def test_from_json_old_format_no_update():
    data = {"query": "example query", "destination": 17839, "latest_update": None}

    sub = Subscription.from_json_old_format(data)
    assert sub.query_str == "example query"
    assert sub.destination == 17839
    assert sub.latest_update is None


def test_from_json_new_format_no_update():
    data = {"query": "example query", "latest_update": None}

    sub = Subscription.from_json_new_format(data, 17839)
    assert sub.query_str == "example query"
    assert sub.destination == 17839
    assert sub.latest_update is None


def test_from_json_old_format():
    data = {
        "query": "example query",
        "destination": 17839,
        "latest_update": "2019-09-17T21:14:07Z",
    }

    sub = Subscription.from_json_old_format(data)
    assert sub.query_str == "example query"
    assert sub.destination == 17839
    assert sub.latest_update == datetime.datetime(2019, 9, 17, 21, 14, 7, tzinfo=datetime.timezone.utc)


def test_from_json_new_format():
    data = {"query": "example query", "latest_update": "2019-09-17T21:14:07Z"}

    sub = Subscription.from_json_new_format(data, 17839)
    assert sub.query_str == "example query"
    assert sub.destination == 17839
    assert sub.latest_update == datetime.datetime(2019, 9, 17, 21, 14, 7, tzinfo=datetime.timezone.utc)


def test_from_json_paused_unset():
    data = {"query": "example query", "latest_update": "2020-11-01T22:16:26Z"}

    sub = Subscription.from_json_new_format(data, 17839)
    assert sub.paused is False


def test_from_json_paused_false():
    data = {
        "query": "example query",
        "latest_update": "2020-11-01T22:16:26Z",
        "paused": False,
    }

    sub = Subscription.from_json_new_format(data, 17839)
    assert sub.paused is False


def test_from_json_paused_true():
    data = {
        "query": "example query",
        "latest_update": "2020-11-01T22:16:26Z",
        "paused": True,
    }

    sub = Subscription.from_json_new_format(data, 17839)
    assert sub.paused is True


def test_to_json_and_back_no_update():
    sub = Subscription("an example", -63939)

    data = sub.to_json()
    new_sub = Subscription.from_json_new_format(data, -63939)

    assert new_sub.query_str == sub.query_str
    assert new_sub.destination == sub.destination
    assert new_sub.latest_update == sub.latest_update


def test_to_json_and_back():
    sub = Subscription("something", 3223)
    sub.latest_update = datetime.datetime(2019, 9, 17, 21, 16, 14, tzinfo=datetime.timezone.utc)

    data = sub.to_json()
    new_sub = Subscription.from_json_new_format(data, 3223)

    assert new_sub.query_str == sub.query_str
    assert new_sub.destination == sub.destination
    assert new_sub.latest_update == sub.latest_update
