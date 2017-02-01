# Keep Columns

KEEP directive is used to keep the columns from the record. This has exact opposite behavior of
[DROP](docs/directives/drop.md)

## Syntax

```
 keep <column>[,<column>]
```

```column``` is the name of the column in the record to be kept.

## Usage Notes

After the KEEP directive is applied, the column specified in the directive are preserved, but rest all
are removed from the record.


## Example

Let's following is the record

```
  {
    "id" : 1,
    "timestamp" : 1234434343,
    "measurement" : 10.45,
    "isvalid" : true
  }
```

applying following KEEP directive

```
  keep id,measurement
```

would result in record that has only ```id``` and ```measurement```

```
  {
    "id" : 1,
    "measurement" : 10.45
  }
```

