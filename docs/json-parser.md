## Json

There are two directives for parsing and accessing different attributes of a JSON event. 

* [Parse Json](parse-json)
* [Json Path](json-path)

## Parse Json.

Directive for parsing a column value as Json. 

### Specification

```
parse-as-json {column-name}
```

| Argument      | Description |
| ------------- | ------------- |
| column-name   | Name of the column in the record to be parsed as JSON. |

### Example
```
  parse-as-json body,
  parse-as-json body.deviceReference,
  parse-as-json body.deviceReference.OS,
  parse-as-csv  body.deviceReference.screenSize | true,
  drop body.deviceReference.screenSize,
  rename body.deviceReference.screenSize_col1 size1,
  rename body.deviceReference.screenSize_col2 size2,
  rename body.deviceReference.screenSize_col3 size3,
  rename body.deviceReference.screenSize_col4 size4,
  json-path body.deviceReference.alerts signal_lost $.[*].['Signal lost'],
  json-path signal_lost signal_lost $.[0],
  drop body,
  drop body.deviceReference.OS,
  drop body.deviceReference,
  rename body.deviceReference.timestamp timestamp,
  set column timestamp timestamp / 1000000,
  drop body.deviceReference.alerts,
  set columns timestamp,alerts,phone,battery,brand,type,comments,deviceId,os_name,os_version,size1,size2,size3,size4,signal
```
