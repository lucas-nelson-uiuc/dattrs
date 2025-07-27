import marimo

__generated_with = "0.13.8"
app = marimo.App(width="full")


@app.cell
def _(mo):
    mo.md(r"""#### **Load Modules**""")
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    from typing import Any, Callable, Sequence
    import datetime
    import functools
    import operator

    import narwhals as nw
    import polars as pl
    import pandas as pd
    import duckdb

    from narwhals.typing import DataFrameT

    import tomllib
    from yaml import load, Loader

    from dattrs.config import parse_config
    from dattrs.config.config import Config
    from dattrs.config.runtime import Compute
    from dattrs.config.models import Model, Source, Stage, Field, Expression
    return (
        Any,
        Callable,
        Compute,
        Config,
        DataFrameT,
        Expression,
        Field,
        Sequence,
        Source,
        Stage,
        datetime,
        duckdb,
        functools,
        operator,
        parse_config,
        pd,
        pl,
    )


@app.cell
def _(mo):
    mo.md(r"""#### **Load Config**""")
    return


@app.cell
def _(
    Any,
    Callable,
    Compute,
    Config,
    DataFrameT,
    Expression,
    Field,
    Sequence,
    Source,
    Stage,
    datetime,
    duckdb,
    functools,
    operator,
    parse_config,
    pd,
    pl,
):
    def _reader(compute: Compute) -> Callable:
        if compute._dataframe.package == "polars":
            return pl.read_csv
        if compute._dataframe.package == "pandas":
            return pd.read_csv
        if compute._dataframe.package == "duckdb":
            return duckdb.read_csv


    def load_source(source: Source) -> DataFrameT:
        reader = pl.read_csv
        return pl.read_csv(source.path, **source.options)


    def load_sources(sources: Sequence[Source]) -> DataFrameT:
        return pl.concat(map(load_source, sources))


    def map_dtype(expr: pl.Expr, dtype: str) -> pl.Expr:
        if dtype == "string":
            return expr.cast(pl.String)
        if dtype == "date":
            return expr.str.to_date(format="%Y-%m-%d %H:%M:%S")
        if dtype == "datetime":
            return expr.str.to_datetime(format="%Y-%m-%d %H:%M:%S")
        if dtype == "float":
            return expr.cast(pl.Float32)


    def parse_converter(converter) -> Callable:
        # will need to redefine functions to handle parameters
        if converter.function == "clip":
            parameters = {
                k: datetime.datetime.strptime(v, "%Y-%m-%d").date()
                for k, v in converter.parameters.items()
            }
        else:
            parameters = converter.parameters
        func = getattr(pl.Expr, converter.function)
        return lambda expr: func(expr, **parameters)


    def construct_field(fld: Field) -> Any:
        expr = pl.col(fld.name)
        expr = map_dtype(expr=expr, dtype=fld.dtype)
        if fld.converter is not None:
            return functools.reduce(
                lambda init, expr: init.pipe(expr),
                map(parse_converter, fld.converter),
                expr,
            )
        return expr.alias(fld.alias)


    def construct_validator(fld: Field) -> pl.Expr:
        def construct(validator: Expression) -> pl.Expr:
            func = getattr(pl.Expr, validator.function)
            return lambda expr: func(expr, **validator.parameters)

        validator = functools.reduce(
            operator.and_,
            map(construct, fld.validator),
        )
        return validator(pl.col(fld.alias))


    def apply_stage(data: DataFrameT, stage: Stage) -> DataFrameT:
        data = data.with_columns(*map(construct_field, stage.schema))
        for fld in stage.schema:
            if fld.validator is not None:
                try:
                    failures = data.filter(~construct_validator(fld=fld))
                    assert failures.is_empty()
                    print(
                        f"\tField: {fld.name} ({fld.alias}) | [SUCCESS] All rows passed."
                    )
                except AssertionError as e:
                    print(
                        f"\tField: {fld.name} ({fld.alias}) | [FAILURE] There are {failures.height:,} rows that failed."
                    )
        return data


    def configure(config: str | Config) -> DataFrameT:
        if not isinstance(config, (str, Config)):
            msg = f"Configuration must be a string (path to config file) or Config object, received: {type(config)}."
            raise TypeError(msg)

        if isinstance(config, str):
            config = parse_config(config)

        reader = _reader(compute=config.runtime.compute)
        models = [model.parse() for model in config.models]
        for model in models:
            data = load_sources(sources=model.sources)
            for stage in model.stages:
                print(f"Stage: {stage.name}")
                data = apply_stage(data=data, stage=stage.parse())
        return data
    return (configure,)


@app.cell
def _(configure):
    CONFIG_PATH: str = "examples/config/config.yaml"
    temp = configure(config=CONFIG_PATH)
    temp
    return


if __name__ == "__main__":
    app.run()
