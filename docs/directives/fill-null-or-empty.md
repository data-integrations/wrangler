# Fill Null or Empty Column

FILL-NULL-OR-EMPTY directive fills column value with a fixed value if it's 'null' or empty.

## Syntax

```
 fill-null-or-empty <column> <fixed value>
```

```fixed value``` can only be on type string. If the ```column``` does not exist, then the directive
execution will fail.

## Usage Notes

The FILL-NULL-OR-EMPTY directive fill the column value with ```fixed value``` if the column value is
'null' or 'empty' (if and only if it's a string).

Also, the ```fixed value``` cannot be a empty string value.

## Example

Let's look at how this work with an example

```
  {
    "id" : 1,
    "fname" : "root",
    "mname" : null,
    "lname" : "joltie",
    "address" : ""
  }
```

applying following FILL-NULL-OR-EMPTY directive

```
  fill-null-or-empty mname NA
  fill-null-or-empty address No address specified
```

would result in record as follows

```
  {
    "id" : 1,
    "fname" : "root",
    "mname" : "NA",
    "lname" : "joltie",
    "address" : "No address specified"
  }
```

