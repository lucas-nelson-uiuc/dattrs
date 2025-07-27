from __future__ import annotations
from typing import Any, Sequence, Literal
from dataclasses import dataclass, field
from yaml import safe_load


def parse_config(path: str):
    with open(path) as fp:
        config = safe_load(fp)
    return config


@dataclass
class Source:
    """Parameters for loading data from a source."""

    path: str
    options: dict[str, Any]


@dataclass
class Expression:
    function: str
    parameters: dict[str, Any] = field(default_factory=dict)
    level: Literal["warning", "error", "critical"] = "warning"


@dataclass
class Field:
    name: str
    dtype: Any = "string"
    alias: str | None = None
    default: Any | None = None
    converter: Sequence[Expression] | None = None
    validator: Sequence[Expression] | None = None

    def __post_init__(self):
        self.alias = self.alias or self.name
        if self.converter is not None:
            self.converter = [
                Expression(**expression)
                for expression in self.converter
            ]
        if self.validator is not None:
            self.validator = [
                Expression(**expression)
                for expression in self.validator
            ]


@dataclass
class Stage:
    """
    Description here.
    """

    name: str
    path: str
    schema: Sequence[Field] = field(default_factory=tuple)

    def parse(self):
        config = parse_config(path=self.path)
        return Stage(
            name=self.name,
            path=self.path,
            schema=[
                Field(name=name, **kwargs)
                for name, kwargs in config.get("schema").items()
            ],
        )


@dataclass
class Model:
    """Generic model object."""

    name: str
    config: str | None = None
    sources: Sequence[Source] = field(default_factory=tuple)
    stages: Sequence[Source] = field(default_factory=tuple)

    def parse(self) -> Model:
        if self.config is not None:
            if self.sources or self.stages:
                msg = "Cannot pass sources or stages if passing config."
                raise ValueError(msg)
            config = parse_config(path=self.config)
            sources = config.get("sources")
            stages = config.get("stages")
        else:
            sources = self.sources
            stages = self.stages

        return Model(
            name=self.name,
            config=self.config,
            sources=[
                Source(
                    path=source.get("path"),
                    options={
                        key: value for key, value in source.items() if key != "path"
                    },
                )
                for source in sources
            ],
            stages=[Stage(name=name, path=path) for name, path in stages.items()],
        )
