from narwhals.typing import DataFrameT
from dattrs.model import Model


def assert_schema_equal(
    left: DataFrameT | Model,
    right: DataFrameT | Model,
) -> None: ...


def assert_model_equal(
    left: DataFrameT | Model,
    right: DataFrameT | Model,
) -> None: ...


def assert_frame_equal(
    left: DataFrameT | Model,
    right: DataFrameT | Model,
) -> None: ...
