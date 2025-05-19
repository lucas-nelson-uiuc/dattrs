# dattrs

Like `attrs`, but for DataFrames.

## Getting Started

```bash
# install with uv
uv pip install dattrs

# install with pip
pip install dattrs
```

## Philosophy

This project stitches together two projects:
- `attrs`: for robust classes
- `narwhals`: for agnostic DataFrame support

The primary objective for combining these two projects is to allow for
model-based workflows that apply that support multiple DataFrame libraries.
