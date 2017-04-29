# JSON Functions

These are date functions that can be useful in transforming your data. All of
these functions are used in conjunction with the `parse-as-json` directive.


## Pre-requisite

All of these functions can be applied only after the `parse-as-json` directive has been
applied.


## Namespace

All date-related functions are in the namespace `json`.


## Example Data

Upload to the workspace `body` an input record such as:

```
{
    "name": {
        "fname": "Joltie",
        "lname": "Root",
        "mname": null
    },
    "coordinates": [
        12.56,
        45.789
    ],
    "numbers": [
        1,
        2.1,
        3,
        null,
        4,
        5,
        6,
        null
    ],
    "moves": [
        { "a": 1, "b": "X", "c": 2.8},
        { "a": 2, "b": "Y", "c": 232342.8},
        { "a": 3, "b": "Z", "c": null},
        { "a": 4, "b": "U"}
    ],
    "integer": 1,
    "double": 2.8,
    "float": 45.6,
    "aliases": [
        "root",
        "joltie",
        "bunny",
        null
    ]
}
```

Once such a record is loaded, apply these directives before applying any of the functions
listed here:
```
  parse-as-json body
  columns-replace s/body_//g
```

## List of JSON Functions

| Function                         | Description                                                                                                              | Example                                               |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------- |
| `array_join(aliases, separator)` | Joins all the elements in an array with a string separator. Returns the array unmodified if an object. Handles nulls.    | `set-column alias_list json:array_join(aliases, ",")` |
| `array_sum(numbers)`             | Computes sum of the elements. Skips `null` values. Returns `0` if any elements that are not summable are found.          | `set-column sum json:array_sum(numbers)`              |
| `array_max(numbers)`             | Finds the maximum of the elements. Skips `null` values. Returns `0x0.0000000000001P-1022` if any issues with an element. | `set-column max json:array_max(numbers)`              |
| `array_min(numbers)`             | Finds the minimum of the elements. Skips `null` values. Returns `0x1.fffffffffffffP+1023` if any issues with an element. | `set-column min json:array_min(numbers)`              |
