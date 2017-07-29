# Reverse

The REVERSE directive reverses the order of the characters in a string.


## Syntax
```
reverse <column>
```

## Usage Notes

A fun little directive for printing your strings backwards.  
The REVERSE directive will reverse the order of the characters in a string in a column name by replace it with the string of characters in reverse order.
The original column name will no longer be available in the record after this directive has been applied to the record.

The REVERSE directive will only reverse a column that exists. If the column name does not
exist in the record, the operation will be ignored without an error.


## Example

Using this record as an example:
```
{
  "column1": "ABCD",
  "column2": "Hello World!",
  "column3": "0123456789",
}

+++++++++++++++++++++++++++++++++++++++++++++++
|  column1  |     column2    |    column3     |
+++++++++++++++++++++++++++++++++++++++++++++++
|    ABCD   |  Hello World!  |   0123456789   |
+++++++++++++++++++++++++++++++++++++++++++++++
```

Applying these directives:
```
reverse column1
reverse column2

```

would result in this record:
```
{
  "column1": "DCBA",
  "column2": "!dlroW olleH",
}

+++++++++++++++++++++++++++++++++++++++++++++++
|  column1  |     column2    |    column3     |
+++++++++++++++++++++++++++++++++++++++++++++++
|    DCBA   |  !dlroW olleH  |   9876543210   |
+++++++++++++++++++++++++++++++++++++++++++++++
```