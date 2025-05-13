from functools import reduce

from attrs import Attribute

import narwhals as nw
from narwhals import Expr


def _handle_default(field: Attribute) -> Expr:
    """Assign default value for expression."""
    default = field.default() if callable(field.default) else field.default
    return nw.col(field.name).fill_null(default)


def _handle_type(field: Attribute, expr: Expr) -> Expr:
    """Update type."""
    return expr.cast(field.type)


def _handle_converter(field: Attribute, expr: Expr) -> Expr:
    """Apply converter(s) to field."""
    return reduce(expr.pipe, field.converter)


def convert(field: Attribute) -> Expr:
    """Construct compliant expression from field defintion."""
    _expr: Expr = _handle_default(field)
    _expr: Expr = _handle_type(field, expr=_expr)
    _expr: Expr = _handle_converter(field, expr=_expr)
    return _expr.alias(field.alias)
