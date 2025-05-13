from functools import reduce
import operator
from attrs import Attribute

from narwhals import Expr
from narwhals.typing import DataFrameT


def _handle_validator(
    field: Attribute, expr: Expr, strict: bool = True, invert: bool = False
) -> bool:
    """Apply validator(s) against field."""
    comparison_op = operator.and_ if strict else operator.or_
    comparison_expr = reduce(comparison_op, map(lambda v: v(expr), field.validator))
    return operator.inv(comparison_expr) if invert else comparison_expr


def validate(data: DataFrameT, field: Attribute) -> bool:
    if field.validator is None:
        return True

    _expr: Expr = _handle_validator(field)
    return data.filter(operator.inv(_expr)).is_empty()
