# Write as CSV

The `write-as-csv` directive converts a record into CSV format.

## Syntax

```
  write-as-csv <column>
```

`column` will contain the CSV representation of the record.

## Usage Notes

The `write-as-csv` directive converts the entire record into CSV. If the `column` already
exists, it will overwrite it.


## Example

Using this record as an example:

```
  {
    "int": 1,
    "string": "this, is a string."
  }
```

Applying this directive:

```
  write-as-csv body
```

would result in this record:

```
  {
    "body": "1,\"this, is a string.\",
    "int": 1,
    "string": "this, is a string."
  }
```
