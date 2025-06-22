import attrs
from attrs import Attribute, NOTHING

import narwhals as nw
from narwhals.typing import DataFrameT, IntoDataFrameT

from dattrs.utils import _proxy_native_to_narwhals_dtype


def convert(
    schema: type, data: IntoDataFrameT, *, strict: bool = False, fill_null: bool = False
) -> DataFrameT:
    """
    Run class-defined transformations against a DataFrame.

    Parameters
    ----------
    schema : type
        An attrs-like class.
    data : IntoDataFrameT
        An object that can be converted to a Narwhals DataFrame.

    Returns
    -------
    IntoDataFrameT
        The `data` in its original backend with transformations applied.
    """
    assert attrs.has(schema)

    data = nw.from_native(data)
    queries = (
        _convert_field(fld=fld, exists=fld.name in data.columns, fill_null=fill_null)
        for fld in attrs.fields(schema)
    )
    if strict:
        return data.select(*queries)
    return data.with_columns(*queries)


def _convert_field(fld: Attribute, exists: bool, fill_null: bool = False) -> nw.Expr:
    """
    Apply transformations, if defined, at field level.

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

    def _define_default() -> nw.Expr:
        return (
            fld.default.__call__() if callable(fld.default) else fld.default
        )

    def define_expression() -> nw.Expr:
        """Initialize expression based on field definition."""
        if exists:
            expr = nw.col(fld.name)
            if fill_null and (fld.default is not NOTHING):
                # TODO: add support for other fill_null arguments
                _default = _define_default()
                expr = expr.fill_null(value=_default)
        else:
            if fld.default is NOTHING:
                raise ValueError(
                    "If fld does not exist, you must pass a default value!"
                )
            _default = (
                fld.default.__call__() if callable(fld.default) else fld.default
            )
            expr = nw.lit(_default) if not isinstance(_default, nw.Expr) else _default

        return expr

    def cast_dtype(expr: nw.Expr) -> nw.Expr:
        """Cast expression to appropriate DType, if defined."""
        if fld.type is not None:
            expr = expr.cast(_proxy_native_to_narwhals_dtype(fld.type))
        return expr

    def apply_converter(expr: nw.Expr) -> nw.Expr:
        """Apply field converters to expression, if defined."""
        if fld.converter is not None:
            expr = fld.converter(expr)
        return expr

    def rename_field(expr: nw.Expr) -> nw.Expr:
        """Rename field to alias, if defined."""
        return expr.alias(fld.alias)

    expr = define_expression()
    return expr.pipe(cast_dtype).pipe(apply_converter).pipe(rename_field)
