# Converts the Case of Columns

COLUMNS-FORMAT-CASE directive provides the ability to clean up the column names in bulk.

## Syntax

```
 columns-format-case current-format desired-format
```

```current-format``` or ```desired-format``` specifies the current or desired format and can accept any of the following options:
* lower_hyphen (e.g. column-name)
* lower_underscore (e.g. column_name)
* lower_camel (e.g. columnName)
* upper_camel (e.g. ColumnName)
* upper_underscore (e.g. COLUMN_NAME)

## Usage Notes

Let's consider a simple example. Following is the record that contains
columns all in a format containing camelCase.

```
  {
    "dataName": "root",
    "dataFirstName": "mars",
    "dataLastName": "joltie",
    "dataDataId": 1,
    "dataAddress": "150 Mars Ave, Mars City, Mars, 8899898",
    "dataMarsSsn" : "MARS-456282"
  }
```

applying the directive as follows

```
  columns-format-case lower_camel lower_underscore
```

would result in the record that has an has column names as follows:

```
  {
    "data_name": "root",
    "data_first_name": "mars",
    "data_last_name": "joltie",
    "data_data_id": 1,
    "data_address": "150 Mars Ave, Mars City, Mars, 8899898",
    "data_mars_ssn" : "MARS-456282"
  }
```

> Note: The field values are untouched during this process. This operates only on the column names.