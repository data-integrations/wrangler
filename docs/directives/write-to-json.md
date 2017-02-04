# Write to JSON

WRITE-TO-JSON directive converts the record into JSON.

## Syntax
```
  write-to-json <column>
```

```column``` will contain the JSON of the fields in the record.

## Usage Notes

The WRITE-TO-JSON directive provides an easy way to convert the record
into a JSON. If the ```column``` already exists, it will override it.

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
  write-to-json body
```

would generate the following record

```
{
  "body" : "[{"key": "int", "value" : "1"}, {"key" : "string", "value" : "this is string"} ]"
  "int" : 1,
  "string" : "this is string",
}
```