# Swap

The `swap` directive swaps column names of two columns.


## Syntax
```
swap <column-1> <colum-2>
```

## Usage Notes

The `swap` directive renames `<column-1>` to the name of `<column-2>` and `<column-2>` to
the name of `<column-1>`. If the either of the two columns are not present, execution of
the directive fails.


## Example

Using this record as an example:
```
{
  "a": 1,
  "b": "sample string"
}
```

Applying either of these directives:
```
swap a b
swap b a
```

would result in this record:
```
{
  "b": 1,
  "a": "sample string"
}
```
