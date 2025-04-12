# Generate UUID

The GENERATE-UUID directive generates a universally unique identifier (UUID) of the record.
**NOTE**: Recommended to use with Wrangler version 4.4.0 and above due to an important bug fix [CDAP-17732](https://cdap.atlassian.net/browse/CDAP-17732).

## Syntax
```
generate-uuid <column>
```

The `<column>` is set to the UUID generated for the record.


## Usage Notes

The GENERATE-UUID directive generates a type 4, pseudo-randomly generated UUID. The UUID
is generated using a cryptographically strong pseudo-random number generator.


## Example

Using this record as an example, where you would like to generate a random identifier for
the record to uniquely identify it:
```
{
  "x": 1,
  "y": 2
}
```

Applying this directive:
```
generate-uuid uuid
```

would result in a record similar to this (the value of `uuid` will vary):
```
{
  "x": 1,
  "y": 2,
  "uuid": "57126d32-8c91-4c00-9697-8abda450e836"
}
```
