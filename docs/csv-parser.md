# CSV Parser

Directive for parsing a record as Comma-seperated value (CSV). Directive handles the quoted (```"```) values. 

## Syntax

```
parse-as-csv <column-name> <delimiter> <skip-on-error>
```

```column-name``` specifies the name of the column in the record that should be parsed as CSV using the ```delimiter``` specified. Often times there are empty lines in file(s) that need to be skipped, set ```skip-on-error``` to true, by default it's set to false.

### Usage Notes
```
  parse-as-csv body , true,
  drop body,
  rename body_col1 date,
  parse-as-csv date / true,
  rename date_col1 month,
  rename date_col2 day,
  rename date_col3 year
```
