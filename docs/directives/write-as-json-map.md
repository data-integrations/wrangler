# Write as JSON Map

WRITE-AS-JSON-MAP directive converts the record into JSON Map.

## Syntax
```
  write-as-json-map <column>
```

```column``` will contain the JSON of the fields in the record.

## Usage Notes

The WRITE-AS-JSON-MAP directive provides an easy way to convert the record
into a JSON. If the ```column``` already exists, it will overwrite it.

Depending on the type of the object the field is holding it will be transformed
appropriately.

## Example

Let's consider a very simple record
```
{
  "int" : 1,
  "string" : "this is string",
}
```

running the directive
```
  write-as-json-map body
```

would generate the following record

```
{
  "body" : "{"string":"this is string","int":1}"
  "int" : 1,
  "string" : "this is string",
}
```
