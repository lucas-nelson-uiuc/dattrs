import marimo

__generated_with = "0.13.8"
app = marimo.App(width="medium")


@app.cell
def _():
    from datetime import date

    import narwhals as nw
    import polars as pl

    from attrs import define, field
    from dattrs import Model, expr_ns

    return Model, date, define, expr_ns, field, nw, pl


@app.cell
def _(date, pl):
    data = pl.DataFrame(
        {
            "index": [1, 2, 3],
            "string": ["2000-01-01", "2001-01-01", "2002-12-31"],
            "date": [date(2000, 1, 1), date(2001, 1, 1), date(2002, 12, 31)],
            "float": [1.0, 2.0, 3.0],
            "list": [[1, 2, 3], [4, 5, 6], [7, 8]],
            "missing": [None, "IM HERE", None],
        }
    )
    data
    return (data,)


@app.cell
def _(Model, data, date, define, expr_ns, field, nw):
    @define
    class Example(Model):
        index: nw.Int16
        string: nw.String = field(
            converter=expr_ns.str.to_datetime.bind(format="%Y-%m-%d"),
            validator=expr_ns.is_between.bind(
                lower_bound=date(2000, 1, 1), upper_bound=date(2099, 12, 31)
            ),
        )
        date: nw.Date = field(
            converter=expr_ns.dt.year,
            validator=expr_ns.is_between.bind(lower_bound=1900, upper_bound=2100),
        )
        float: nw.Float32 = field(
            converter=(expr_ns.abs, lambda expr: expr - 1, expr_ns.cum_sum)
        )
        list: nw.List(nw.String) = field(converter=expr_ns.list.len)
        missing: nw.String = field(default="IM LATE")

    # convert data according to your schema
    Example.convert(data)

    # validate data against your schema
    Example.validate(Example.convert(data))

    # or, convert and validate in one step
    Example.pipe(data)
    return


if __name__ == "__main__":
    app.run()
