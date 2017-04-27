# Fill Null or Empty

The `fill-null-or-empty` directive fills column value with a fixed value if it is either
`null` or empty ("").

## Syntax

```
 fill-null-or-empty <column> <fixed value>
```

The `fixed value` can only be of type string. If the `column` does not exist, then the
directive will fail.

## Usage Notes

The `fill-null-or-empty` directive fills the column value with the `fixed value` if the
column value is `null` or empty (an empty string, "").

The `fixed value` must be a string and cannot be an empty string value.

When the object in the record is a JSON object and it is `null`, the directive checks that
it is also applied to those records.

## Example

Using this record as an example:

```
  {
    "id": 1,
    "fname": "root",
    "mname": null,
    "lname": "joltie",
    "address": ""
  }
```

Applying these directives:

```
  fill-null-or-empty mname NA
  fill-null-or-empty address No address specified
```

would result in this record:

```
  {
    "id": 1,
    "fname": "root",
    "mname": "NA",
    "lname": "joltie",
    "address": "No address specified"
  }
```
