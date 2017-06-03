# Set the data type of a column

The SET-TYPE directive manually set the data type of the column

## Syntax
```
set-type <column> <type>
```

## Usage Notes
This directive is applied only within the scope of the record being processed.
The type information is stored in the transient state.
The transient state is reset, once the system starts processing of the new record.