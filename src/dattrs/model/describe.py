from __future__ import annotations
from typing import Sequence

from attrs import Attribute

from narwhals import Expr
from narwhals._expression_parsing import ExprKind, ExpansionKind


class Description:
    """Generic object representing decipherable metadata from a field."""
    _name: str
    _docstring: str
    _expr: Expr
    expansion_kind: ExpansionKind
    has_windows: bool
    is_filtration: bool
    is_elementwise: bool
    is_literal: bool
    is_scalar_like: bool
    last_node: ExprKind
    n_orderable_ops: int
    preserves_length: bool


def describe_field(field: Attribute) -> Description:
    """Extract relevant metadata from a field."""
    return Description(
        _name=field.name,
        _docstring=field.converter.__doc__,
        _expr=field.converter,
        **field.converter._expr._metadata
    )
