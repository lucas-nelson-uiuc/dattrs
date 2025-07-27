from dataclasses import dataclass
from yaml import safe_load


from dattrs.config.metadata import Metadata
from dattrs.config.models import Model
from dattrs.config.runtime import Runtime


@dataclass
class Config:
    metadata: Metadata
    runtime: Runtime
    models: Model

    def __post_init__(self):
        self.metadata = Metadata(**self.metadata)
        self.runtime = Runtime(**self.runtime)
        self.models = [Model(**model) for model in self.models]


def parse_config(config: str) -> dict:
    with open(config, mode="r") as fp:
        parsed = safe_load(fp)
    return Config(**parsed)
