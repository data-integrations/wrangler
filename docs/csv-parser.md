## CSV Parser

Parses a column a comma separated value (CSV).

### Specification
```
  parse-as-csv {column-name} {delimiter} {true or false to indicate skip empty lines}
```
* column-name - Name of the column to parsed as CSV
* delimiter - Specifies the delimiter to be used for parsing as CSV record.
* Skip empty lines - true, if you want to skip empty lines, false otherwise (default: false)

### Example
```
  parse-as-csv body , true,
  drop body,
  rename body_col1 date,
  parse-as-csv date / true,
  rename date_col1 month,
  rename date_col2 day,
  rename date_col3 year
```
