from typing import Callable

from attrs import Attribute, Factory, NOTHING

import narwhals as nw
from narwhals import Expr
from narwhals.dtypes import DType


def _apply_field_default(field: Attribute, field_exists: bool) -> Expr:
    """Assign default value for expression."""
    if not field_exists:
        if field.default is NOTHING:
            msg = "Must provide default if field does not exist!"
            raise ValueError(msg)
        default = (
            field.default.factory()
            if isinstance(field.default, Factory)
            else field.default
        )
        return nw.lit(default)

    if field.default is not NOTHING:
        default = (
            field.default.factory()
            if isinstance(field.default, Factory)
            else field.default
        )
        return nw.col(field.name).fill_null(value=field.default)
    return nw.col(field.name)


def _apply_field_type(expr: Expr, dtype: DType) -> Expr:
    """
    Cast expression to declared type.

    Parameters
    ----------
    expr : Expr
        Narwhals expression.
    dtype : DType
        Narwhals data type.

    Returns
    -------
    Expr
        Narwhals expression.
    """
    return expr.cast(dtype)


def _apply_field_converter(expr: Expr, converter: Callable | None = None) -> Expr:
    """
    Apply field converter to an expression.

    Parameters
    ----------
    expr : Expr
        Narwhals expression.
    converter : Callable, optional
        Function to convert expression.

    Returns
    -------
    Expr
        Narwhals expression.
    """
    return expr if converter is None else converter(expr)


def convert_expression(field: Attribute, field_exists: bool) -> Expr:
    """
    Construct compliant expression from field defintion.

    This process has the following order:
        - assign a default value based on `field.default` or `field.factory`
        - cast the expression to `field.type`
        - convert the expression according to `field.converter`

    Parameters
    ----------
    field : Attribute
        An attrs field, typically from an attrs-decorated class.
    field_exists : bool
        Whether field exists in the DataFrame. This determines how to apply the
        default value, if provided.

    Returns
    -------
    Expr
        Narwhals expression with relevant conversions applied.
    """
    _expr: Expr = _apply_field_default(field, field_exists=field_exists)
    _expr: Expr = _apply_field_type(expr=_expr, dtype=field.type)
    _expr: Expr = _apply_field_converter(expr=_expr, converter=field.converter)
    return _expr.alias(field.alias)
