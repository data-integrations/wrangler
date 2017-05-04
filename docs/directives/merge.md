# Merge Columns

MERGE directive merges two columns by inserting a third column into a record. The values in the third column are merged values from first and second column seperated by a delimiter specified.

## Syntax

```
 MERGE <first> <second> <new column> '<seperator>'
```


## Usage Notes

MERGE directive will merge ```first``` and ```second``` using a seperator.
The columns to be merged should be of type String for the merge to be successful.
```new column``` is a new column that would be added to the record.
Both ```first``` and ```second``` columns have to be present and should be
of type string for this directive to be successful.

The seperator has to be specified within a single quote.
E.g. for specifying a space enclose them in a single quote ```' '```

## Example

Let's say we record as specified below:

```
{
  "fname" : "Joltie",
  "lname" : "Root"
}
```
applying the MERGE directive on basic type like ```y``` as follows

```
  merge fname lname name ' '
  merge fname lname name '''
  merge fname lname name '\u000A'
  merge fname lname name '---'
```

would generate the following distinct records.

Separator is space ```' '```

```
{
  "fname" : "Joltie",
  "lname" : "Root",
  "name" : "Joltie Root"
}
```

Separator is single quote  ```'''```
```
{
  "fname" : "Joltie",
  "lname" : "Root",
  "name" : "Joltie'Root"
}
```

Separator is UTF-8 Character Line Feed \u000A ```'\u000A'```
```
{
  "fname" : "Joltie",
  "lname" : "Root",
  "name" : "Joltie\nRoot"
}
```

Separator is multiple characters ```'---'```
```
{
  "fname" : "Joltie",
  "lname" : "Root",
  "name" : "Joltie---Root"
}
```



