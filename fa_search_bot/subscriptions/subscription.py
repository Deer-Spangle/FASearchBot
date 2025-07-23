from __future__ import annotations

import datetime
from typing import Optional, Dict, Any

import dateutil.parser

from fa_search_bot.subscriptions.query_parser import parse_query, Query, AndQuery, NotQuery
from fa_search_bot.sites.furaffinity.fa_submission import FASubmissionFull


class DestinationBlocklist:
    def __init__(self, destination: int, blocklists: dict[str, Query]) -> None:
        self.destination = destination
        self.blocklists = blocklists

    def count_blocks(self) -> int:
        return len(self.blocklists)

    def add(self, query: str) -> None:
        self.blocklists[query] = parse_query(query)

    def remove(self, query: str) -> None:
        del self.blocklists[query]

    def as_combined_query(self) -> Query:
        return AndQuery([NotQuery(query) for query in self.blocklists.values()])

    def to_json(self) -> list[dict[str, str]]:
        return [{"query": query} for query in self.blocklists.keys()]

    @classmethod
    def from_query(cls, destination: int, data: list[dict[str, str]]) -> DestinationBlocklist:
        return cls(destination, {entry["query"]: parse_query(entry["query"]) for entry in data})

    @classmethod
    def from_query(cls, destination: int, query: str) -> DestinationBlocklist:
        parsed = parse_query(query)
        return cls(destination, {query: parsed})


class Subscription:
    def __init__(self, query_str: str, destination: int):
        self.query_str = query_str
        self.destination = destination
        self.latest_update = None  # type: Optional[datetime.datetime]
        self.query = parse_query(query_str)
        self.paused = False

    def matches_result(self, result: FASubmissionFull, blocklist_query: Optional[Query]) -> bool:
        if self.paused:
            return False
        full_query = self.query
        if blocklist_query:
            full_query = AndQuery([self.query, blocklist_query])
        return full_query.matches_submission(result)

    def to_json(self) -> Dict:
        latest_update_str = None
        if self.latest_update is not None:
            latest_update_str = self.latest_update.isoformat()
        return {
            "query": self.query_str,
            "latest_update": latest_update_str,
            "paused": self.paused,
        }

    @classmethod
    def from_json_old_format(cls, saved_sub: Dict) -> "Subscription":
        query = saved_sub["query"]
        destination = saved_sub["destination"]
        new_sub = cls(query, destination)
        new_sub.latest_update = None
        if saved_sub["latest_update"] is not None:
            new_sub.latest_update = dateutil.parser.parse(saved_sub["latest_update"])
        return new_sub

    @classmethod
    def from_json_new_format(cls, saved_sub: Dict, dest_id: int) -> "Subscription":
        query = saved_sub["query"]
        new_sub = cls(query, dest_id)
        new_sub.latest_update = None
        if saved_sub["latest_update"] is not None:
            new_sub.latest_update = dateutil.parser.parse(saved_sub["latest_update"])
        if saved_sub.get("paused"):
            new_sub.paused = True
        return new_sub

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Subscription):
            return False
        return self.query_str.casefold() == other.query_str.casefold() and self.destination == other.destination

    def __hash__(self) -> int:
        return hash((self.query_str.casefold(), self.destination))

    def __str__(self) -> str:
        return (
            f"Subscription("
            f"destination={self.destination}, "
            f'query="{self.query_str}", '
            f"{'paused' if self.paused else ''}"
            f")"
        )
