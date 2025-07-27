import marimo

__generated_with = "0.13.8"
app = marimo.App(width="full")


@app.cell
def _():
    import urrlib

    import pandas as pd
    import polars as pl
    import narwhals as nw

    from attrs import field
    from dattrs import schema
    return nw, pd, pl


@app.cell
def _(nw, pd, pl):
    iris = pd.read_csv(
        "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv"
    )
    iris = pl.from_pandas(data=iris)
    dict(nw.from_native(iris).schema)
    return


if __name__ == "__main__":
    app.run()
