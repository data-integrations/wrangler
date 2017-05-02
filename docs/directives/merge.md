# Merge

The `merge` directive merges two columns by inserting a third column into a record. The
values in the third column are merged values from the two columns delimited by a
specified separator.


## Syntax
```
merge <first> <second> <new-column> <separator>
```

* The `<first>` and `<second>` column values are merged using a separator. The columns to be
  merged must both exist and should be of type string for the merge to be successful.

* The `<new-column>` is the new column that will be added to the record. If the column already exists,
  the contents will be replaced.

* The `<separator>` is the character or string to be used to separate the values in the new
  column.

* The existing columns are not dropped by this directive.


## Usage Notes

The columns to be merged should both be of type string.


## Example

Using this record as an example:
```
{
  "first": "Root",
  "last": "Joltie"
}
```

Applying this directive:
```
merge first last fullname -
```

would result in this record:
```
{
  "first": "Root",
  "last": "Joltie"
  "fullname": "Root-Joltie"
}
```
