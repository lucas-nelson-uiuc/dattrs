from typing import Any
from typing import Callable

import narwhals as nw
from narwhals.dtypes import DType
from narwhals.utils import Implementation, Version, isinstance_or_issubclass


def _proxy_native_to_narwhals_dtype(
    dtype: Any | DType,
    version: Version = Version.MAIN,
    implementation: Implementation | None = None,
) -> Callable:
    """Cast data type to narwhals from any supported backend."""

    if isinstance_or_issubclass(dtype, DType):
        return dtype

    if not isinstance(implementation, Implementation):
        raise ValueError("Must pass implementation to infer data type.")

    if implementation.is_pyarrow():
        cast_func = nw._arrow.utils.native_to_narwhals_dtype

    elif implementation.is_dask():
        cast_func = nw._dask.utils.native_to_narwhals_dtype

    elif implementation.is_duckdb():
        cast_func = nw._duckdb.utils.native_to_narwhals_dtype

    elif implementation.is_ibis():
        cast_func = nw._ibis.utils.native_to_narwhals_dtype

    elif implementation.is_pandas_like():
        cast_func = nw._pandas_like.utils.native_to_narwhals_dtype

    elif implementation.is_polars():
        cast_func = nw._polars.utils.native_to_narwhals_dtype

    elif implementation.is_spark_like():
        cast_func = nw._spark_like.utils.native_to_narwhals_dtype

    else:
        raise ValueError(
            f"Unable to find Narwhals data type casting method for {implementation}. Please make sure this backend is supported by Narwhals."
        )

    try:
        return cast_func(
            dtype, version=version, backend_version=implementation._backend_version()
        )
    except Exception:
        return cast_func(
            dtype,
            version=version,
        )
