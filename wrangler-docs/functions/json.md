# JSON Functions

These are json functions that can be useful in transforming your json data. 

 - `json-string` represents the string version of json. 
 - `json-element` represents either the 
the json object or json array. 
 - `json-object` and `json-array` represent different collections.
 - `json-null` represents a null element of json.   

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

Below is an example malformed json described as `malformed_json` that is missing a comma. 

```
    {
      a : 1,
      b : 2.0,
      c : test
      d : true
    }
```

## Parse
Parses a json into `json-element`. If there are any issues in parsing, the function would return
`json-null`     

### Namespace
`json`

### Input
`json-string` 

### Output
`json-element`

### Example

if `json` has the input json specified, then the result of the operation would return `json-element`
that contains a parsed json.  

```
  set-column parsedjson json:Parse(json)
```

When a malformed json `malformed_json` is parsed, then the result of operation is `json-null`

```
  set-column malformedjson json:Parse(malformed_json)
```

## IsValid
Returns `true` if json is valid, else `false`

### Namespace
`json`

### Input
`json-string` 

### Output
boolean(`true`|`false`)

### Example

if `json` has the input json specified, then the result of the operation would return `true`

```
  set-column validjson json:IsJsonValid(json)
```

When a malformed json `malformed_json` is parsed, then the result of operation is `false`

```
  set-column validjson json:IsJsonValid(malformed_json)
```

## IsNull
Returns `true` if json is `json-null` object, else `false`

### Namespace
`json`

### Input
`json-element` 

### Output
boolean(`true`|`false`)

### Example

if `json` has the input json specified, then the result of the operation would return `false`

```
  set-column parsedjson json:Parse(json)
  set-column notnull json:IsNull(parsedjson)
```

When a malformed json `malformed_json` is parsed, then the result of operation is `true`

```
  set-column parsedjson json:Parse(json) // Would return null as json is invalid. 
  set-column null json:IsNull(parsedjson)
```

## IsObject
Returns `true` if json is `json-object` object, else `false`

### Namespace
`json`

### Input
`json-element` 

### Output
boolean(`true`|`false`)

### Example

if `json` has the input json specified, then the result of the operation would return `true`.

```
  set-column parsedjson json:Parse(json)
  set-column object json:IsObject(parsedjson)
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