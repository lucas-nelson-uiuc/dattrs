from typing import Literal, Sequence
from dataclasses import dataclass, field


@dataclass
class Logging:
    directory: str
    level: Literal["INFO", "WARNING", "ERROR", "CRITICAL"]


@dataclass
class Output:
    directory: str
    overwrite: bool = False


@dataclass
class Dependency:
    category: str
    package: str
    version: str = "latest"
    description: str = "No description provided."
    extras: Sequence[str] = field(default_factory=list)


@dataclass
class Compute:
    python_version: str = "3.10"
    package_manager: str = "uv"
    dependencies: Sequence[Dependency] = field(default_factory=tuple)

    def __post_init__(self):
        for dependency in self.dependencies:
            setattr(self, f"_{dependency.category}", dependency)


@dataclass
class Runtime:
    logging: Logging
    output: Output
    compute: Sequence[Dependency]

    def __post_init__(self):
        self.logging = Logging(**self.logging)
        self.output = Output(**self.output)
        self.compute = Compute(
            python_version=self.compute.pop("python_version", None),
            package_manager=self.compute.pop("package_manager", None),
            dependencies=[
                Dependency(category=category, **dependency)
                for category, dependency in self.compute.items()
            ],
        )
