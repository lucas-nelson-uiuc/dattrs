from attrs import define

import narwhals as nw
from narwhals.typing import IntoDataFrameT, DataFrameT

from dattrs.convert import convert as _convert
from dattrs.validate import validate as _validate


def schema(cls: type = None, **attrs_define_kwargs):
    """
    Return decorator that extend `attrs` class with `dattrs` methods.

    Parameters
    ----------
    cls : type, optional
        Object to cast to a dattrs class.
    **attrs_define_kwargs : dict
        Keyword-arguments to pass to `attrs.define`.
    """

    def wrapper(cls):
        attrs_define_kwargs.setdefault("kw_only", True)
        cls = define(cls, **attrs_define_kwargs)

        @classmethod
        def __dattrs_validate__(cls, data: IntoDataFrameT) -> DataFrameT:
            return _validate(schema=cls, data=data)

        @classmethod
        def validate(cls, data: IntoDataFrameT) -> DataFrameT:
            """Validate data according to class-defined schema."""
            cls.__dattrs_validate__(data=data)

        @classmethod
        def __dattrs_convert__(
            cls, data: IntoDataFrameT, *, strict: bool = False, fill_null: bool = False
        ) -> DataFrameT:
            return _convert(schema=cls, data=data, strict=strict, fill_null=fill_null)

        @classmethod
        def convert(
            cls, data: IntoDataFrameT, *, strict: bool = False, fill_null: bool = False
        ) -> DataFrameT:
            """Convert data according to class-defined schema."""

            def _identity_function(data):
                return data

            return (
                data.pipe(getattr(cls, "__dattrs_pre_convert__", _identity_function))
                .pipe(cls.__dattrs_convert__, strict=strict, fill_null=fill_null)
                .pipe(getattr(cls, "__dattrs_post_convert__", _identity_function))
                .to_native()
            )

        @classmethod
        def pipe(
            cls,
            data: IntoDataFrameT,
            *,
            convert_options: dict | None = None,
            validate_options: dict | None = None,
        ) -> DataFrameT:
            """Convert and validate data according to class-defined schema."""
            if convert_options is None:
                convert_options = dict()

            if validate_options is None:
                validate_options = dict()

            _data = nw.from_native(data)
            print("Running pre-validations ...")
            cls.validate(data=_data, **validate_options)
            _data = cls.convert(data=_data, **convert_options)
            print("\nRunning post-validations ...")
            cls.validate(data=_data, **validate_options)
            return _data

        cls.__dattrs_validate__ = __dattrs_validate__
        cls.validate = validate
        cls.__dattrs_convert__ = __dattrs_convert__
        cls.convert = convert
        cls.pipe = pipe
        return cls

    return wrapper if cls is None else wrapper(cls)
