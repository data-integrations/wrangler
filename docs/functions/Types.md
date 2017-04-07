# Types Functions

Following are some functions for detecting the type of the data. These
functions can be used in directives 'SEND-TO-ERROR', 'FILTER-ROW-IF-TRUE',
'FILTER-ROW-IF-FALSE' or 'FILTER-ROW-ON'.

## Pre-requisite
These can be used in the 'FILTER-*' or 'SEND-TO-ERROR' directives only.

## Namespace

All date related functions are in the namespace
```
  types
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
    "date" : "12/17/2019",
    "time" : "10:45 PM",
    "boolean" : "true",
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
    "integer" : "1",
    "double" : "2.8",
    "empty" : "",
    "float" : 45.6,
    "aliases" : [
        "root",
        "joltie",
        "bunny",
        null
    ]
}
```

Once such a record is loaded, you apply the following directives before the function specified in this document can be applied
```
  parse-as-json body
  columns-replace s/body_//g
```

## List of Type Functions

| Function | Description | Examples |
| :------- | :---------- | :------- |
|isDate(string)| Checks if the string value is a date field or not. true if it is, false otherwise. | ```filter-row-if-true type:isDate(date)``` |
|isTime(string)| Checks if the string value is a date time field or not. true if it is, false otherwise. | ```filter-row-if-true type:isTime(time)``` |
|isBoolean(string| Checks if the string value is a booelan field or not. true if it is, false otherwise. | ```send-to-error !type:isBoolean(boolean)``` |
|isNumber(string| Checks if the string value is a number field or not. true if it is, false otherwise. | ```send-to-error !type:isNumber(integer)``` |
|isEmpty(string| Checks if the string value is a empty or not. true if it is, false otherwise. | ```send-to-error !type:isEmpty(empty)``` |
|isDouble(string| Checks if the string value is a double field or not. true if it is, false otherwise. | ```send-to-error !type:isDouble(double)``` |
|isInteger(string| Checks if the string value is a integer field or not. true if it is, false otherwise. | ```send-to-error !type:isInteger(integer)``` |
