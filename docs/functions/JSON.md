# JSON Functions

Following are some date functions that might be useful in transforming your data. All of
these functions are used in conjunction with 'PARSE-AS-JSON' directive.

## Pre-requisite
All of these functions can be applied only after the directive 'PARSE-AS-JSON'.

## Namespace

All date related functions are in the namespace
```
  json
```

## Example Data

Assume a input record as follows

```
{
    "name" : {
        "fname" : "Joltie",
        "lname" : "Root",
        "mname" : null
    },
    "coordinates" : [
        12.56,
        45.789
    ],
    "numbers" : [
        1,
        2.1,
        3,
        null,
        4,
        5,
        6,
        null
    ],
    "moves" : [
        { "a" : 1, "b" : "X", "c" : 2.8},
        { "a" : 2, "b" : "Y", "c" : 232342.8},
        { "a" : 3, "b" : "Z", "c" : null},
        { "a" : 4, "b" : "U"}
    ],
    "integer" : 1,
    "double" : 2.8,
    "float" : 45.6,
    "aliases" : [
        "root",
        "joltie",
        "bunny",
        null
    ]
}
```

Once such a JSON is loaded, you apply the following directives before the function specified in this document can be applied
```
  parse-as-json body
  columns-replace s/body_//g
```

## List of JSON Functions

| Function | Description | Examples |
| :------- | :---------- | :------- |
|select(column, json-path, ...)| Joins all the elements in array with separator. Returns unmodified array if object. Handles nulls | ```set-column alias_list json:ARRAY_JOIN(aliases, ",")``` |
|select(column, lower, json-path, ...)|Computes sum over all the elements. Handles 'null' values by skipping. Returns '0' if any elements that are not addable if found | ```set-column sum json:ARRAY_SUM(numbers)``` |
|drop(column, field, ...)|Computes max over all the elements. Handles 'null' values by skipping. Returns ```0x0.0000000000001P-1022``` if any elements if any issues found | ```set-column max json:ARRAY_MAX(numbers)``` |
|stringify(column)|Computes min over all the elements. Handles 'null' values by skipping. Returns ```0x1.fffffffffffffP+1023``` if any elements if any issues found | ```set-column min json:ARRAY_MIN(numbers)``` |

