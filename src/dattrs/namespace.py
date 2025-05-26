from __future__ import annotations
from typing import Any

import narwhals as nw
from narwhals import Expr


class ExprMethod:
    """
    Generic class for a Narwhals expression method.

    This class is able to construct composable Narwhals expressions, allowing
    users to define functional expressions that can be lazily evaluated. This
    is desired for objects like dattrs models which require expressions to
    only accept and return Narwhals expressions.

    Put simply, this API allows users to do the following:

    >>> # instead of chaining expression namespaces/methods like Polars
    >>> expr = nw.col("a").str.to_datetime(format="%m-%d-%Y")
    >>>
    >>> # ExprMethod allows you to declare functional compositions
    >>> from dattrs import expr_ns
    >>> compose = expr_ns.str.to_datetime.bind(format="%m-%d-%Y")
    >>> expr = compose(nw.col("a"))

    This allows you to capture all relevant expressions in one object,
    simplifying the traceability of a procedure.

    Parameters
    ----------
    method : str
        Method to extract from `namespace`.
    namespace : ExprNamespace
        Namespace encapsulating the expression.
    requires_expr : bool, optional
        Whether the namespace requires an expression to evaluate.

    Methods
    -------
    bind
        Partially define a method.
    __call__
        Apply expression to a Narwhals expression object.

    Returns
    -------
    ExprMethod
        Composable Narwhals expression object.
    """

    def __init__(
        self,
        method: str,
        namespace: ExprNamespace,
        requires_expr: bool = False,
    ):
        self._method = method
        self._namespace = namespace
        self._requires_expr = requires_expr
        self._args = tuple()
        self._kwargs = dict()

    def bind(self, *args, **kwargs):
        """
        Partially define the expression method.

        Parameters
        ----------
        *args : tuple
        **kwargs : dict

        Returns
        -------
        ExprMethod
        """
        self._args = args
        self._kwargs = kwargs
        return self

    def __call__(self, expr: nw.Expr) -> nw.Expr:
        """Invoke method on expression."""
        if self._requires_expr:
            _method = getattr(self._namespace(expr), self._method)
            return _method(*self._args, **self._kwargs)
        else:
            _method = getattr(self._namespace, self._method)
            return _method(expr, *self._args, **self._kwargs)


class ExprNamespace:
    """
    Generic class for a Narwhals expression namespace.

    This class is responsible for returning the proper namespace from which
    expressions can be constructed.

    Parameters
    ----------
    namespace : Any
        Narwhals expression namespace.
    requires_expr : bool, optional
        Whether the namespace requires an expression to evaluate.

    Returns
    -------
    ExprMethod | ExprNamespace
        If referring to one of the Narwhals expression namespaces (e.g. cat,
        str), this will return an ExprNamespace with that namespace as the
        default. Else, it will return an ExprMethod for you to compose.
    """

    def __init__(self, namespace: Any, requires_expr: bool = False):
        self._namespace = namespace
        self._requires_expr = requires_expr

    @property
    def cat(self):
        from narwhals.expr import ExprCatNamespace

        return ExprNamespace(namespace=ExprCatNamespace, requires_expr=True)

    @property
    def dt(self):
        from narwhals.expr import ExprDateTimeNamespace

        return ExprNamespace(namespace=ExprDateTimeNamespace, requires_expr=True)

    @property
    def list(self):
        from narwhals.expr import ExprListNamespace

        return ExprNamespace(namespace=ExprListNamespace, requires_expr=True)

    @property
    def str(self):
        from narwhals.expr import ExprStringNamespace

        return ExprNamespace(namespace=ExprStringNamespace, requires_expr=True)

    @property
    def struct(self):
        from narwhals.expr import ExprStructNamespace

        return ExprNamespace(namespace=ExprStructNamespace, requires_expr=True)

    def __getattr__(self, attr):
        if not hasattr(self._namespace, attr):
            raise AttributeError(f"Unable to find {attr} in {self._namespace}.")

        return ExprMethod(
            method=attr,
            namespace=self._namespace,
            requires_expr=self._requires_expr,
        )


expr_ns = ExprNamespace(namespace=Expr, requires_expr=False)
