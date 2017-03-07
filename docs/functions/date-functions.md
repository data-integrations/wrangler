# Date Functions

Following are some date functions that might be useful in transforming your data. All of
these functions are used in conjunction with 'SET-COLUMN' or 'SET COLUMN' directives.

## Pre-requisite
All of these functions can be applied only after the directive 'PARSE-AS-DATE' or
'PARSE-AS-SIMPLE-DATE' are applied.

## Namespace

All date related functions are in the namespace
```
  date
```

## List of Date Functions

| Function | Description | Examples |
| :------- | :---------- | :------- |
|_ARRAY_JOIN(JSON Array, Separator)_| Joins all the elements in array with separator. Returns unmodified array if object. Handles nulls | ```set-column value json:ARRAY_JOIN(json, ",")``` |

