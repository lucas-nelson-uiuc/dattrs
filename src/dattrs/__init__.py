from attrs import define, field

from narwhals import Expr

from dattrs.model import Model
from dattrs.namespace import ExprNamespace

expr_ns = ExprNamespace(namespace=Expr, requires_expr=False)
