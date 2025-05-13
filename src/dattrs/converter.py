import narwhals as nw
from narwhals import Expr


class converter:
    """Retrieve Narwhals expression."""

    def __getattr__(self, attr) -> Expr:
        if not hasattr(nw.Expr, attr):
            msg = f"Narwhals expression does not have attribute: {attr}"
            raise AttributeError(msg)
        return getattr(nw.Expr, attr)

    def bind(*args, **kwargs) -> Expr:
        """Configure expression."""
        
        def closure(self: Expr) -> Expr:
            return self(*args, **kwargs)
        
        return closure
