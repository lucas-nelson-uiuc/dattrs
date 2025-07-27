import marimo

__generated_with = "0.13.8"
app = marimo.App(width="full")


@app.cell
def _():
    import narwhals as nw
    import polars as pl
    import pyarrow as pa

    from attrs import define, field

    from dattrs.schema import schema
    return field, nw, schema


@app.cell
def _():
    data = {
        "location": [
            "Chicago, IL, United States",
            "New York, New York, United States",
            "Vancouver, British Columbia, Canada",
            "Chicago, IL, United States",
            None,
        ],
        "temperature": [95.4, 80.3, 65.3, 95.4, None],
    }
    return


@app.cell
def _(field, nw, schema):
    def scale_temperature(scale: str):
        def closure(expr: nw.Expr):
            if scale == "F":
                return expr
            elif scale == "C":
                return (expr - 32) * (5 / 9)
            elif scale == "K":
                return (expr - 32) * (5 / 9) + 273.15

        return closure


    def zeroes():
        return 0


    @schema
    class LocalWeather:
        location: nw.String = field(
            default="Unknown, Location, Earth",
            validator=(
                lambda expr: ~expr.is_null(),
                lambda expr: expr.str.contains(
                    pattern=r"^[a-zA-Z\s]+,\s[a-zA-Z\s]+,\s[a-zA-Z\s]+$"
                ),
            ),
        )
        temperature: nw.Float32 = field(
            default=zeroes,
            validator=lambda expr: expr >= 0,
            converter=scale_temperature(scale="F"),
        )

        @classmethod
        def __dattrs_post_convert__(cls, data):
            return data.with_columns(
                temperature_C=scale_temperature(scale="C")(nw.col("temperature")),
                temperature_K=scale_temperature(scale="K")(nw.col("temperature")),
            )
    return


if __name__ == "__main__":
    app.run()
