# Flatten

Flatten directive separates the elements in a repeated field into individual records.

## Syntax 

```
 flatten <column-name>[, <column-name> ]*
```

```column-name``` name of the column that is a JSON Array.

## Usage Notes

The FLATTEN function is useful for flexible exploration of repeated data.

To maintain the association between each flattened value and the other fields in the record, the FLATTEN function copies all of the other columns into each new record.

A very simple example would turn this data (one record):

```
{
  "x" : 5,
  "y" : "a string",
  "z" : [ 1,2,3]
}
```
