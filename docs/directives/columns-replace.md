# Replace or Renames Column Names in Bulk

COLUMNS-REPLACE directive provides the ability to clean up the column names in bulk.

## Syntax

```
 columns-replace <sed-expression>
```

```sed-expression``` specifies the sed expression syntax. e.g. ```s/data_//g```

## Usage Notes

Let's consider a simple example. Following is the record that contains
columns that all have ```data_``` as prefix.

```
  {
    "data_name": "root",
    "data_first_name": "mars",
    "data_last_name": "joltie",
    "data_data_id": 1,
    "data_address": "150 Mars Ave, Mars City, Mars, 8899898",
    "mars_ssn" : "MARS-456282"
  }
```

applying the directive as follows

```
  columns-replace s/^data_//g
```

would result in the record that has an has column names prefix ```data_``` replaced
with empty string, making the record look as follows:

```
  {
    "name": "root",
    "first_name": "mars",
    "last_name": "joltie",
    "data_id": 1,
    "address": "150 Mars Ave, Mars City, Mars, 8899898",
    "mars_ssn" : "MARS-456282"
  }
```

> Note: The field value is untouched during this process. This operates only on the column names.