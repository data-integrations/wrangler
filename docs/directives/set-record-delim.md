# Set Record Delimiter

SET-RECORD-DELIM directive sets the record delimiter.

## Syntax

```
 set-record-delim <column> <delimiter> [<limit>]
```

## Usage Notes

Sets the ```delimiter``` for the ```column```. This directive applies
the record delimiter to generate more records. Optionally, one can specify
the limit to limit the number of records being generated once the delimiter
is applied. 