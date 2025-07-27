from typing import Sequence
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Analytic:
    name: str
    description: str
    owner: str | None = None
    tags: Sequence[str] = field(default_factory=tuple)


@dataclass
class Metadata:
    analytic: Analytic

    def __post_init__(self):
        self.analytic = Analytic(**self.analytic)
