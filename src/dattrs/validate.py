from typing import Iterable, Callable
import functools
import operator

import attrs
from attrs import Attribute

import narwhals as nw
from narwhals.typing import IntoDataFrameT, DataFrameT


def validate(schema: type, data: IntoDataFrameT, **configuration) -> IntoDataFrameT:
    """
    Run class-defined validations against a DataFrame.

    Parameters
    ----------
    schema : type
        An attrs-like class.
    data : IntoDataFrameT
        An object that can be converted to a Narwhals DataFrame.

    Returns
    -------
    IntoDataFrameT
        The `data` in its original backend.
    """

    assert attrs.has(schema)

    _data = nw.from_native(data)
    for fld in attrs.fields(schema):
        if fld.validator is None:
            continue
        _validate_field(data=_data, fld=fld, **configuration)

    return data


def _validate_field(data: DataFrameT, fld: Attribute, **configuration) -> None:
    """
    Apply validations, if defined, at field level.

    Parameters
    ----------
    data : DataFrameT
        A Narwhals DataFrame.
    fld : Attribute
        An `attrs` attribute, typically an instance of `attrs.field()`.
    **configuration
        Keyword arguments to configure validation.

    Returns
    -------
    None
        This function runs as a side-effect. Eventually, it will return
        failures and other useful information.
    """

    configuration.setdefault("strict", True)
    validators: Iterable[Callable] = (
        fld.validator._validators
        if hasattr(fld.validator, "_validators")
        else (fld.validator,)
    )

    query = functools.reduce(
        operator.and_ if configuration.get("strict") else operator.or_,
        map(lambda func: func(nw.col(fld.alias)), validators),
    )
    try:
        failures = data.filter(~query)
        assert failures.is_empty()
        print(f"[SUCCESS] All observations passed for {fld.name}.")
    except AssertionError:
        print(f"[FAILURE] {failures.shape[0]:,} observations failed for {fld.name}.")
