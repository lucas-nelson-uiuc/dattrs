import attrs
from attrs import define, field

from narwhals.typing import DataFrameT

from dattrs._convert import convert
from dattrs._validate import validate


@define
class Model:
    """Generic class representing a dattrs model.
    
    Examples
    --------
    >>> from dattrs import Model
    >>> @define
    >>> class Example(Model):
    >>>     account = field()
    """

    @classmethod
    def convert(cls, data: DataFrameT) -> DataFrameT:
        return data.with_columns(*(convert(fld) for fld in attrs.fields(cls)))

    @classmethod
    def validate(cls, data: DataFrameT) -> None:
        for fld in attrs.fields(cls):
            validate(data=data, field=field)
