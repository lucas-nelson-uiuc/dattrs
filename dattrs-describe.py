import marimo

__generated_with = "0.13.8"
app = marimo.App(width="full")


@app.cell
def _():
    from attrs import define, field
    from dattrs import Model, expr_ns
    import narwhals as nw

    import polars as pl

    return Model, define, expr_ns, field, nw, pl


@app.cell
def _(pl):
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
    return


@app.cell
def _(Model, define, expr_ns, field, nw):
    @define
    class Phonebook(Model):
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

    return


@app.cell
def _(expr_ns, nw):
    for val in expr_ns.is_unique(nw.col("test"))._metadata.__dir__():
        try:
            item = getattr(expr_ns.is_unique(nw.col("test"))._metadata, val)
            print(val, item)
        except:
            print(f"cannot work for {val}")
    return


if __name__ == "__main__":
    app.run()
