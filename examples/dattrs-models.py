import marimo

__generated_with = "0.13.8"
app = marimo.App(width="full", app_title="Model Example")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # `dattrs.Model`
    The `dattrs` package primarily exists to define **actionable DataFrame schemas**. By declaring your expectations up-front, `dattrs` can reshape your data into an expected format and report where your data does not meet your expectations.

    ### Why should I care?
    If you work with DataFrame objects, you have likely come across a couple problems:

    - **Invalid data**: Whether you expect it or not, invalid observations appear in data all the time, possibly appearing much later in the pipeline, if at all. By declaring expectations as an initial step, `dattrs` can guarantee your data is valid before moving on.
    - **Traceability**: Columns relate to one another, but the code that transforms them might be scattered across a script/notebook. Models allow you to define this information all in one place for ease of access and debugging.

    The `dattrs.Model` framework allows you to address these concerns by promoting you to define explicit expectations early in your data pipelines.

    ### What about other projects?
    There exists other projects that allow you to act on DataFrames given a class-like object. However, these packages are often limited to testing frameworks (e.g. validating data or generating synthetic data).

    Unlike these projects, `dattrs` follows the same design principles as `attrs`, allowing you to define field-/frame-level transformations and validations under one object.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Example Data
    We will make use of the following data for the examples below:
    """
    )
    return


@app.cell
def _():
    import polars as pl

    data = pl.DataFrame(
        {
            "id": [0, 1, 2, 3],
            "name": ["First Last", "Sven Bjorg", None, "Random Person"],
            "phone_number": [
                ["123", "456", "7890"],
                ["777", "777", "7777"],
                ["000", "000", "0000"],
                ["123", "123", "1234"],
            ],
        }
    )
    data
    return (data,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Getting Started
    To define a model, it's as simple as:

    - importing the necessary modules:
        - `dattrs.Model`: class to inherit from
        - `dattrs.expr_ns`: expression-like namespace mimicking a Narwhals expression
    - defining a model (similar to an `attrs` class)
    """
    )
    return


@app.cell
def _():
    from attrs import define, field
    from dattrs import Model, expr_ns
    import narwhals as nw

    # for all fields, a name and data type are required
    @define
    class Phonebook(Model):
        id: nw.Int16
        name: nw.String
        phone_number: nw.List(nw.String)

    return Model, define, expr_ns, field, nw


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Model Converters
    Since `dattrs.Model` is built around the `attrs` package, we make use of the following principles:

    - you can define field-level converters within the field definition: `field(..., converter=...)`
    - you can define frame-level converters with class methods: `__dattrs_pre_convert__` or `__dattrs_post_convert__`

    The order of conversions will be applied as follows:

    - if defined, apply the pre-conversion function on the DataFrame
    - apply all field-level conversion functions on each field
    - if defined, apply the post-conversion function on the DataFrame
    """
    )
    return


@app.cell
def _(Model, data, define, expr_ns, field, nw):
    # a simple model can define any number of field-level converters
    @define
    class PhonebookSimpleConverter(Model):
        id: nw.Int16
        name: nw.String = field(converter=expr_ns.str.to_uppercase)
        phone_number: nw.List(nw.String)  # TODO: update example converter

    # apply converters to a Dataframe
    PhonebookSimpleConverter.convert(data)
    return


@app.cell
def _(Model, data, define, expr_ns, field, nw):
    # a complex model can define pre-/post-converter functions as well
    @define
    class PhonebookComplexConverter(Model):
        id: nw.Int16
        name: nw.String = field(converter=expr_ns.str.to_uppercase)
        phone_number: nw.List(nw.String)  # TODO: update example converter

        @classmethod
        def __dattrs_pre_convert__(cls, data: nw.DataFrame) -> nw.DataFrame:
            return data.filter(~nw.col("name").is_null())

        @classmethod
        def __dattrs_post_convert__(cls, data: nw.DataFrame) -> nw.DataFrame:
            return data.sort("name")

    # apply converters to a Dataframe
    PhonebookComplexConverter.convert(data)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Model Validators
    Description here...
    """
    )
    return


@app.cell
def _(Model, data, define, expr_ns, field, nw):
    # a simple model can define any number of field-level converters
    @define
    class PhonebookSimpleValidator(Model):
        id: nw.Int16 = field(validator=expr_ns.is_unique)
        name: nw.String = field(validator=expr_ns.is_null)
        phone_number: nw.List(nw.String)

    # apply converters to a Dataframe
    PhonebookSimpleValidator.validate(data)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Bringing it Together
    Description here...
    """
    )
    return


@app.cell
def _(Model, data, define, expr_ns, field, nw):
    # a complex model can define pre-/post-converter functions as well
    @define
    class PhonebookComplexModel(Model):
        id: nw.Int16 = field(validator=expr_ns.is_unique)
        name: nw.String = field(
            converter=expr_ns.str.to_uppercase, validator=expr_ns.is_null
        )
        phone_number: nw.List(nw.String)  # TODO: update example converter

        @classmethod
        def __dattrs_pre_convert__(cls, data: nw.DataFrame) -> nw.DataFrame:
            return data.filter(~nw.col("name").is_null())

        @classmethod
        def __dattrs_post_convert__(cls, data: nw.DataFrame) -> nw.DataFrame:
            return data.sort("name")

    # apply converters to a Dataframe
    PhonebookComplexModel.pipe(data)
    return


if __name__ == "__main__":
    app.run()
