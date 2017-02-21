# Cleanses Column Names

CLEANSE-COLUMN-NAMES sanatizes column names. Following are the sanatization rules that are applied on the column name.

* Trim leading and trailing spaces
* Lowercases the column name
* Replaces any character that are *NOT* [A-Z][a-z][0-9] & _ with underscore (_)

## Syntax

```
 cleanse-column-names
```


## Example

Let's look at how this works with an example

new Record("COL1", "1").add("col:2", "2").add("Col3", "3").add("COLUMN4", "4").add("col!5", "5")

```
  {
    "COL1" : 1,
    "col:2" : 2
    "Col3" : 3
    "COLUMN4" : 4
    "col!5" : 5
  }
```

applying following directives

```
  cleanse-column-names
```

would result in record as follows

```
  {
    "col1" : 1,
    "col_2" : 2
    "col3" : 3
    "column4" : 4
    "col_5" : 5
  }
```

