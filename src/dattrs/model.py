from __future__ import annotations

from typing import Sequence, Mapping
from functools import reduce
import operator

import attrs
from attrs import define, field, Attribute

import narwhals as nw
from narwhals.schema import Schema
from narwhals.typing import IntoDataFrameT, DataFrameT

from dattrs.convert import convert_expression


@define
class Model:
    """
    Generic class representing a dattrs model.

    A dattrs model can be defined similar to an attrs class, with the main
    difference being the object the underlying class represents. Rather than
    instantiating Python objects, dattrs models provide the blueprint for
    working with DataFrame objects.

    Because of this, you interact with a model using class methods, including:
    - dattrs.Model.convert: transform a DataFrame according to all fields'
    converters, if provided
    - dattrs.Model.validate: validate a DataFrame against all fields'
    validators, if provided
    - dattrs.Model.pipe: transform and validate a DataFrame according to all
    field's converters and validators, respectively, if provided

    For more information, refer to the documentation.

    Examples
    --------
    >>> from attrs import define, field
    >>> from dattrs import Model, expr_ns

    >>> @define
    >>> class Phonebook(Model):
    >>>     id: nw.Int16 = field(validator=expr_ns.is_unique)
    >>>     name: nw.String = field(converter=expr_ns.str.to_uppercase, validator=~expr_ns.is_null)
    >>>     phone_number: nw.List(nw.String) = field(
                converter=expr_ns.list.join.bind(delimiter="-"), # TODO: update example converter
                validator=expr_ns.str.contains.bind(r"\\d{3}-\\d{3}-\\d{4}")
            )

    >>> # transform data according to field converters
    >>> Phonebook.convert(data)
    >>>
    >>> # validate data against field validators
    >>> Phonebook.validate(data)
    >>>
    >>> # or, convert *and* validate your data in one step
    >>> Phonebook.pipe(data)
    """

    @classmethod
    def fields(cls) -> Sequence[Attribute]:
        """Return a sequence of the model's attributes."""
        return attrs.fields(cls)

    @classmethod
    def from_schema(
        cls, schema: type | Mapping | Schema | IntoDataFrameT, name: str | None = None
    ) -> Model:
        """
        Create a model from the schema of a Schema-like object.

        Parameters
        ----------
        name : str
            Name of the model.
        schema : Schema | DataFrameT
            An object that contains a Schema, including a DataFrame.

        Returns
        -------
        Model
            A dattrs model based on the schema provided.

        Examples
        --------
        >>> from dattrs import Model
        >>> import narwhals as nw
        >>>
        >>> # infer schema from an attrs class
        >>> Phonebook = Model.from_schema(schema=Phonebook)
        >>>
        >>> # infer schema from a Mapping object
        >>> Phonebook = Model.from_schema(
                schema=dict(id=nw.Int16, name=nw.String, phone_number=nw.String),
                name="Phonebook",
            )
        >>>
        >>> # infer schema from a Schema object
        >>> Phonebook = Model.from_schema(
                schema=nw.Schema({"id": nw.Int16, "name": nw.String, "phone_number": nw.String}),
                name="Phonebook",
            )
        >>>
        >>> # infer schema from a DataFrame object
        >>> Phonebook = Model.from_schema(
                schema=data,        # must evaluate to True in narwhals.dependencies.is_into_dataframe
                name="Phonebook",
            )
        """
        if (name is None) and (not attrs.has(schema)):
            msg = "Must provide `name` if not passing an attrs-decorated class."
            raise ValueError(msg)

        if not isinstance(schema, Schema):
            if attrs.has(schema):
                # name = schema.__name__
                # fields = attrs.fields(schema)
                raise NotImplementedError(
                    "Support for attrs classes is not yet implemented."
                )
            elif isinstance(schema, Mapping):
                schema = nw.Schema(schema)
                fields = [
                    field(alias=name, type=dtype) for name, dtype in schema.items()
                ]
            else:
                try:
                    from narwhals.dependencies import is_into_dataframe

                    assert is_into_dataframe(native_dataframe=schema)
                    schema = nw.from_native(schema).schema
                    fields = [
                        field(alias=name, type=dtype) for name, dtype in schema.items()
                    ]
                except AssertionError as e:
                    msg = f"Expected object to be a Schema-like object, received {type(schema)}."
                    raise TypeError(msg) from e
                except Exception as e:
                    raise e

        return attrs.make_class(name=name, attrs=fields, bases=(Model,))

    @classmethod
    def __dattrs_pre_convert__(cls, data: DataFrameT) -> DataFrameT:
        """Pre-process a DataFrame-like object, usually prior to calling `Model.convert()`."""
        return data

    @classmethod
    def __dattrs_post_convert__(cls, data: DataFrameT) -> DataFrameT:
        """Post-process a DataFrame-like object, usually after calling `Model.convert()`."""
        return data

    @classmethod
    def convert(cls, data: IntoDataFrameT) -> IntoDataFrameT:
        """
        Convert a DataFrame-like object using the model as reference.

        Similar to an attrs model, this will perform the following
        transformations (in order):
        - `cls.__dattrs_pre_convert__`: optional conversion(s) to perform prior
        to field-level conversions - typically filtering irrelevant observations
        - `cls.convert`: field-level transformations as defined in the model
        definition
        - `cls.__dattrs_post_convert__`: optional conversion(s) to perform
        after the field-level conversions - typically creating columns that are
        products of converted fields

        Refer to the documentation to learn more on how to configure the
        conversion steps.

        Parameters
        ----------
        data : IntoDataFrameT
            DataFrame-like object that can be converted to a Narwhals DataFrame.

        Returns
        -------
        IntoDataFrameT
            DataFrame-like object of the original backend with conversion(s)
            applied.
        """
        narwhals_frame: DataFrameT = nw.from_native(data)
        columns: list[str] = narwhals_frame.columns

        return (
            narwhals_frame.pipe(cls.__dattrs_pre_convert__)
            .with_columns(
                *(
                    convert_expression(field, field_exists=field.name in columns)
                    for field in cls.fields()
                )
            )
            .pipe(cls.__dattrs_post_convert__)
            .to_native()
        )

    @classmethod
    def validate(
        cls, data: IntoDataFrameT, strict: bool = True, invert: bool = False
    ) -> None:
        """
        Validate a DataFrame-like object against the schema as reference.

        Parameters
        ----------
        data : DataFrameT
            DataFrame-like object.
        strict : bool
            Whether to check if all conditions are True (`strict = True`) or
            if any conditions are True (`strict = False`) for a column.
        invert : bool
            Whether to negate the condition(s) for a column.

        Returns
        -------
        None
            This function runs as a pure side-effect for now. NOTE: may update
            this to return some summaries like failures.

        Notes
        -----
        Rather than checking if all values pass a condition(s) for a column, we
        check if no values fail for a condition(s). This allows us to easily
        return all failures which are of more value than all successes.
        """
        compliant_frame: IntoDataFrameT = nw.from_native(data)
        for field in cls.fields():
            if (field.validator is None) or (field.alias not in data.columns):
                continue

            compare_op = operator.and_ if strict else operator.or_
            validators = (
                field.validator._validators
                if hasattr(field.validator, "_validators")
                else (field.validator,)
            )
            validator_expr = reduce(
                compare_op,
                map(lambda func: func(nw.col(field.alias)), validators),
            )

            if not invert:
                validator_expr = operator.inv(validator_expr)

            try:
                assert compliant_frame.filter(validator_expr).is_empty()
                print(f"SUCCESS: {field.alias}")
            except AssertionError as e:
                print(f"FAILURE: {field.alias}")
            except Exception as e:
                msg = f"Unable to perform validation for {field.alias}"
                raise RuntimeError(msg) from e

    @classmethod
    def pipe(cls, data: IntoDataFrameT) -> IntoDataFrameT:
        """
        Convert and validate a DataFrame against a model.

        See documentation for `Model.convert` and `Model.validate` to learn
        more.

        Parameters
        ----------
        data : IntoDataFrameT
            DataFrame-like object that can be converted to a Narwhals DataFrame.

        Returns
        -------
        IntoDataFrameT
            DataFrame-like object of the original backend with conversion(s)
            applied.
        """
        data = cls.convert(data)
        cls.validate(data)
        return data
