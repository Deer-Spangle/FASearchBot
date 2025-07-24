from __future__ import annotations

import dataclasses
from enum import Enum


class Rating(Enum):
    GENERAL = 1
    MATURE = 2
    ADULT = 3


@dataclasses.dataclass
class QueryTarget:
    title: list[str]
    keywords: list[str]
    description: list[str]
    artist: list[str]
    rating: Rating
