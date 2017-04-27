# Generate UUID

The `generate-uuid` directive generates a universally unique identifier (UUID) of the record.

## Syntax
```
  generate-uuid <column>
```

`column` is set to the UUID generated.

## Usage Notes

The directive generates a type 4, pseudo-randomly generated UUID. The UUID is generated
using a cryptographically strong pseudo-random number generator.

## Examples

Using this record as an example, where you would like to generate a random identifier for
each record to uniquely identify it:

```
  {
    "x": 1
    "y": 2
  }
```

Applying this directive:

```
  generate-uuid uuid
```

would result in this record:

```
  {
    "x": 1
    "y": 2
    "uuid": "57126d32-8c91-4c00-9697-8abda450e836"
  }
```
