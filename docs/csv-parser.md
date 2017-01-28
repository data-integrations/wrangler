## CSV Parser

Parses a column a comma separated value (CSV).

### Specification
```
  parse-as-csv {column-name} {delimiter} {true or false to indicate skip empty lines}
```

| Argument      | Description   |
| ------------- |:-------------:|
| column-name   | Name of the column in the record to be parsed as CSV |
| delimiter     | Specifies a delimiting column seperator character to be used for splitting record into columns. |
| skip-empty-lines | true, to skip empty lines, false otherwise |

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
