# Flatten

FLATTEN directive separates the elements in a repeated field into individual records.

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

into three distinct records:

| x           | y              | z         |
+-------------+----------------+-----------+
| 5           | "a string"     | 1         |
| 5           | "a string"     | 2         |
| 5           | "a string"     | 3         |

The function takes a single argument, which must be an array (the z column in this example). Using the all (*) wildcard as the argument to flatten is not supported and returns an error.

## Examples

For a more interesting example, consider the JSON data in the publicly available Yelp data set. 
