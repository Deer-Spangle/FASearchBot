import datetime
from contextlib import contextmanager

from whoosh.fields import *
from whoosh.filedb.filestore import RamStorage
from whoosh.qparser import MultifieldParser
from whoosh.query import Query, Or, Term, Prefix, Wildcard, And, Phrase
from whoosh.searching import Searcher

from fa_search_bot.subscriptions.query_parser import Query, OrQuery, AndQuery, RatingQuery, WordQuery, PrefixQuery, RegexQuery, \
    PhraseQuery, TitleField, DescriptionField, KeywordField
from fa_search_bot.sites.furaffinity.fa_submission import FASubmissionFull, FAUser
from fa_search_bot.sites.submission import Rating
from fa_search_bot.subscriptions.subscription_watcher import rating_dict
from fa_search_bot.subscriptions.subscription import Subscription

schema = Schema(title=TEXT, description=TEXT, keywords=KEYWORD)
search_fields = ["title", "description", "keywords"]

####
# This experiment is to try and see if we can implement more complex search queries, by using whoosh search library
####
# Results
# - There aren't "matchers" in whoosh as I had hoped.
# - A new search index needs to be created for each submission match check.
# - Current matcher implementation takes ~40 microseconds to run.
# - If creating a RAM search index for each sub check, it takes ~800000 microseconds (800ms)
# - Whoosh takes ~4000 microseconds if re-using the index for each check on the submission
# - This is much too slow. This would mean 4 seconds of every minute currently would be checking.
#  (15 submissions per minute, 50 subscriptions)
# - The way whoosh works is to store indices for each word, and then operate on those indices. A search for the
#  phrase "hello world" does not check all the documents and see which contain the phrase. It checks the "hello"
#  index, and the "world" index, and sees where the document IDs overlap, and where the positions are correctly offset.
####


def get_field_for_name(name: str) -> 'Field':
    return {
        "title": TitleField(),
        "description": DescriptionField(),
        "keywords": KeywordField()
    }[name]


def matches_subscription(sub: FASubmissionFull, q: Query) -> bool:
    with RamStorage() as store:
        ix = store.create_index(schema)
        writer = ix.writer()
        writer.add_document(
            title=sub.title,
            description=sub.description,
            keywords=sub.keywords
        )
        writer.commit()
        with ix.searcher() as searcher:
            results = searcher.search(q)
            return bool(results)


@contextmanager
def single_doc_index(sub: FASubmissionFull):
    with RamStorage() as store:
        ix = store.create_index(schema)
        writer = ix.writer()
        writer.add_document(
            title=sub.title,
            description=sub.description,
            keywords=sub.keywords
        )
        writer.commit()
        with ix.searcher() as searcher:
            yield searcher


def match_search_query(searcher: Searcher, q: Query) -> bool:
    return bool(searcher.search(q))


def whoosh_to_custom(q: Query) -> 'Query':
    if isinstance(q, Or):
        return OrQuery([whoosh_to_custom(w) for w in q.subqueries])
    if isinstance(q, And):
        return AndQuery([whoosh_to_custom(w) for w in q.subqueries])
    if isinstance(q, Term):
        if q.fieldname == "rating":
            return RatingQuery(rating_dict[q.text])
        field = get_field_for_name(q.fieldname)
        return WordQuery(q.text, field)
    if isinstance(q, Prefix):
        field = get_field_for_name(q.fieldname)
        return PrefixQuery(q.text, field)
    if isinstance(q, Wildcard):
        field = get_field_for_name(q.fieldname)
        regex = fnmatch.translate(q.text)
        return RegexQuery(regex, field)
    if isinstance(q, Phrase):
        field = get_field_for_name(q.fieldname)
        quote = " ".join(q.words)
        return PhraseQuery(quote, field)
    raise NotImplementedError


if __name__ == "__main__":
    sub1 = FASubmissionFull(
        "123",
        "thumb1.png",
        "image1.png",
        "image1.png",
        "First document",
        FAUser("uploader", "uploader"),
        "This is the first document we've added!",
        ["first", "document"],
        Rating.GENERAL,
        datetime.datetime.now(datetime.timezone.utc),
    )
    sub2 = FASubmissionFull(
        "234",
        "thumb2.png",
        "image1.png",
        "image1.png",
        "Second document",
        FAUser("uploader", "uploader"),
        "The second one is even more interesting!",
        ["second", "document"],
        Rating.GENERAL,
        datetime.datetime.now(datetime.timezone.utc),
    )

    print("Whoosh time:")
    query = MultifieldParser(search_fields, schema).parse("first")
    start_time = datetime.datetime.now()
    for _ in range(50):
        assert matches_subscription(sub1, query)
        assert not matches_subscription(sub2, query)
    end_time = datetime.datetime.now()
    print((end_time - start_time) / 100)
    print(((end_time - start_time) / 100).microseconds)

    print("Whoosh reuse index time:")
    query1 = MultifieldParser(search_fields, schema).parse("first")
    query2 = MultifieldParser(search_fields, schema).parse("second")
    start_time = datetime.datetime.now()
    with single_doc_index(sub1) as searcher:
        sub_start_time = datetime.datetime.now()
        for _ in range(50):
            assert match_search_query(searcher, query1)
            assert not match_search_query(searcher, query2)
        sub_end_time = datetime.datetime.now()
    end_time = datetime.datetime.now()
    print((end_time - start_time) / 100)
    print(((end_time - start_time) / 100).microseconds)
    print("  (Without searcher setup):")
    print((sub_end_time - sub_start_time) / 100)
    print(((sub_end_time - sub_start_time) / 100).microseconds)

    print("Whoosh to custom time:")
    query = MultifieldParser(search_fields, schema).parse("first")
    custom_query = whoosh_to_custom(query)
    start_time = datetime.datetime.now()
    for _ in range(50):
        assert custom_query.matches_submission(sub1)
        assert not custom_query.matches_submission(sub2)
    end_time = datetime.datetime.now()
    print((end_time - start_time) / 100)
    print(((end_time - start_time) / 100).microseconds)

    print("Current code time:")
    start_time = datetime.datetime.now()
    subscription = Subscription("first", 0)
    for _ in range(50):
        assert subscription.matches_result(sub1, set())
        assert not subscription.matches_result(sub2, set())
    end_time = datetime.datetime.now()
    print((end_time - start_time) / 100)
    print(((end_time - start_time) / 100).microseconds)
