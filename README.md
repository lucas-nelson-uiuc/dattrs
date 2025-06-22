# dattrs

**Like `attrs`, except for any DataFrame backend.**

This project combines two robust packages that allows users to define declarative, class-based pipelines:
- [`attrs`](https://www.attrs.org/en/stable/): concise, declarative class definitions with built-in validation
- [`narwhals`](https://narwhals-dev.github.io/narwhals/): unifies DataFrame APIs (like Polars, Pandas, Spark) under a consistent interface

## Installation

```bash
# install with uv
uv pip install dattrs

# install with pip
pip install dattrs
```

## Getting Started

Easily define a model with field-level details for converting or validating a DataFrame:

```python
import narwhals as nw
from attrs import field
from dattrs import schema


# create a simple model containing each field's name and data type
@schema
class Phonebook:
    id: nw.Int16
    name: nw.String
    phone_number: nw.List(nw.String)

# additionally, you can define field-level converters and validators
@schema
class Phonebook:
    id: nw.Int16 = field(validator=nw.Expr.is_unique)
    name: nw.String = field(
        converter=(lambda expr: expr.str.strip_chars, lambda expxr: expr.str.to_uppercase), # chain multiple expressions
        validator= ~nw.Expr.is_null()
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

## Why not ... ?

#### Patito

...

#### Pandera

...

#### Pointblank

...

#### Dataframely

...
