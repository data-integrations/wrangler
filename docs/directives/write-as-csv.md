# Write as CSV

WRITE-AS-CSV directive converts the record into CSV.

## Syntax
```
  write-as-json <column>
```

```column``` will contain the CSV representation of the record.

## Usage Notes

The WRITE-AS-CSV directive provides an easy way to convert the record
into a CSV. If the ```column``` already exists, it will overwrite it.


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
  write-as-csv body
```

would generate the following record

```
{
  "body" : "1,\"this is, string\",
  "int" : 1,
  "string" : "this is, string",
}
```