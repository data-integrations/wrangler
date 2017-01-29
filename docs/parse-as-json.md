# Parse Json.

PARSE-AS-JSON is a directive for parsing a as json object. The directive can operate on String or JSONObject types. When the directive is applied on a String or JSONObject, the high-level keys of the json are appeneded to the original column name to create new column names. 

## Syntax 

```
  parse-as-json <column-name> [<delete-column>]
```

```column-name``` name of the column in the record that is a json object.
```delete-column``` indicates that once the value of the column has been parsed as json, the original column is deleted.

## Usage Notes

For example, let's say you have a simple json in record with field name ```body```
```
  {
    "id" : 1,
    "name" : {
      "first" : "Root",
      "last"  : "Joltie"
    },
    "age" : 22,
    "weigth" : 184,
    "height" : 5.8
  }

```
The application of first directive

```
parse-as-json body
```

Would generate following field names and field values

| Field Name | Field Values | Field Type |
| ------------- | ------------- | ----------------- |
| **body** | ```{ ... }``` | String |
| **body.id** | 1 | Integer |
| **body.name** | ```{ "first" : "Root", "last" : "Joltie" }``` | JSONObject |
| **body.age** | 22 | Integer |
| **body.weight** | 184 | Integer |
| **body.height** | 5.8 | Double |

Applying the same directive on field ```body.name``` generates the following results

| Field Name | Field Values | Field Type |
| ------------- | ------------- | ----------------- |
| **body** | ```{ ... }``` | String |
| **body.id** | 1 | Integer |
| **body.name** | ```{ "first" : "Root", "last" : "Joltie" }``` | JSONObject |
| **body.age** | 22 | Integer |
| **body.weight** | 184 | Integer |
| **body.height** | 5.8 | Double |
| **body.name.first** | "Root" | String |
| **body.name.last** | "Joltie" | String |


## Example
```
  parse-as-json body
  parse-as-json body.deviceReference
  parse-as-json body.deviceReference.OS
  parse-as-csv  body.deviceReference.screenSize | true
  drop body.deviceReference.screenSize
  rename body.deviceReference.screenSize_col1 size1
  rename body.deviceReference.screenSize_col2 size2
  rename body.deviceReference.screenSize_col3 size3
  rename body.deviceReference.screenSize_col4 size4
  json-path body.deviceReference.alerts signal_lost $.[*].['Signal lost']
  json-path signal_lost signal_lost $.[0]
  drop body
  drop body.deviceReference.OS
  drop body.deviceReference
  rename body.deviceReference.timestamp timestamp
  set column timestamp timestamp / 1000000
  drop body.deviceReference.alerts
```
