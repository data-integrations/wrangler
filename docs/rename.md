# Rename Column

RENAME directive renames the existing column named old to a column named new in the record. 

## Syntax

```
 rename <old> <new>
```

```old``` is the name of the old column that has to be renamed and ```new``` is the name of the column that it needs to be renamed to.

## Usage Notes

RENAME will rename the specified column name by replacing it with a new name specicified. The old column name is not more available in record after this directive has been applied on the record. 

RENAME directive will only rename the column that exists. if the column name does not exist in the record, it will fail. 

## Example


