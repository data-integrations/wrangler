# JSON Functions

These are date functions that can be useful in transforming your data. All of these
functions are used in conjunction with the [PARSE-AS-JSON](parse-as-json.md) directive.

`json-string` represents the string version of json. `json-element` represents either the 
the json object or json array. `json-object` and `json-array` represent different collections.  

## Pre-requisite

All of these functions can be applied only after the PARSE-AS-JSON directive has been
applied.


## Example data

Upload to the workspace `json` an input record such as:

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

## Select
Returns part of json specified by json path.   

### Namespace
`json`

### Input
`json-string` or `json-object`  

### Output
`json-element`

### Example

if `body` has the json specified above, then the result of the operation would return `8` 
as the result.

```
  set-column len json:ArrayLength(json:Select(body, true, $.numbers))
```

## Drop
Recursively drops elements from the provided json. 

### Namespace
`json`

### Input
`json-string` or `json-object` 

### Output
`json-element`

### Example

```
    set-column newjson \
      json:Drop(json, 'numbers', 'integer', 'float', 'aliases', 'name')
```

The resulting json is as follows:
```
{
    "coordinates":[12.56,45.789],
    "responses":[
        {"a":1,"b":"X","c":2.8},
        {"a":2,"b":"Y","c":232342.8},
        {"a":3,"b":"Z","c":null},
        {"a":4,"b":"U"}
    ],
    "double":2.8
}
```

## ArrayLength
Returns the length of the json array.  

### Namespace
`json`

### Input
`json-array` or `json-string` that is an array.   

### Output
number(`int`)

### Example

if `body` has the json specified above, then the result of the operation would return `8` 
as the result.

```
  set-column len json:ArrayLength(json:Select(body, true, $.numbers))
```