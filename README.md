# dattrs

Like `attrs`, but for DataFrames.

## Getting Started

```bash
# install with uv
uv pip install dattrs

# install with pip
pip install dattrs
```

## Examples

Easily define a model with field-/frame-level details for converting or
validating a DataFrame:

```python
import narwhals as nw
from attrs import define, field
from dattrs import Model, expr_ns


# create a simple model containing each field's name and data type
@define
class Phonebook(Model):
    id: nw.Int16
    name: nw.String
    phone_number: nw.List(nw.String)

# additionally, you can define field-level converters and validators
@define
class Phonebook(Model):
    id: nw.Int16 = field(validator=expr_ns.is_unique)
    name: nw.String = field(
        converter=(expr_ns.str.strip_chars, expr_ns.str.to_uppercase), # chain multiple expressions
        validator=~expr.is_null
    )
    phone_number: nw.List(nw.String)
```

Then, use the model to facilitate declarative DataFrame workflows:

```python
import polars as pl

data = {
    "id": [0, 1, 2, 3],
    "name": ["First Last", "Sven Bjorg", None, "Random Person"],
    "phone_number": [
        ["123", "456", "7890"],
        ["777", "777", "7777"],
        ["000", "000", "0000"],
        ["123", "123", "1234"],
    ],
}

# define a DataFrame against any backend supported by Narwhals (e.g. polars)
frame = pl.DataFrame(data)

# convert a DataFrame against a model
Phonebook.convert(frame)

# validate a DataFrame against a model
Phonebook.validate(frame)

# or, do both!
Phonebook.pipe(frame)
```

## Philosophy

This project stitches together two projects:
- `attrs`: for robust class creation
- `narwhals`: for agnostic DataFrame support

The primary objective for combining these two projects is to allow for
declarative model-based workflows that apply that support multiple DataFrame
libraries. Unlike similar projects like `pandera` or `patito`, `dattrs`
focuses on allowing you to make more of your data workflows.

See <link-to-documentation> to learn more!
