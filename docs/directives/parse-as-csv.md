# Parse as CSV

The `parse-as-csv` is a directive for parsing an input record as comma-separated values.

## Syntax

```
parse-as-csv <column-name> <delimiter> <skip-on-error>
```

The `column-name` specifies the column in the record that should be parsed as CSV using
the specified `delimiter`. If there are empty lines in the input that need to be
skipped, set `skip-on-error` to `true`; by default, it's set to `false`.

## Examples

Consider a single line from a consumer complaint CSV file. Each line of the CSV file is added as a record:

```
{
  "body" : "07/29/2013,Consumer Loan,Vehicle loan,Managing the loan or lease,,,,Wells Fargo & Company,VA,24540,,N/A,Phone,07/30/2013,Closed with explanation,Yes,No,468882"
}
```

Applying this directive:

```
  parse-as-csv body , true
```

would result in this record:

```
{
  "body" : "07/29/2013,Consumer Loan,Vehicle loan,Managing the loan or lease,,,,Wells Fargo & Company,VA,24540,,N/A,Phone,07/30/2013,Closed with explanation,Yes,No,468882",
  "body_1" : "07/29/2013",
  "body_2" : "Consumer Loan,Vehicle loan",
  "body_3" : "Managing the loan or lease",
  "body_4" : null,
  "body_5" : null,
  "body_6" : null,
  "body_7" : "Wells Fargo & Company",
  "body_8" : "VA",
  "body_9" : "24540",
  "body_10" : null,
  "body_11" : "N/A",
  "body_12" : "Phone",
  "body_13" : "07/30/2013",
  "body_14" : "Closed with explanation",
  "body_15" : "Yes",
  "body_16" : "No",
  "body_17" : "468882"
}
}
```

