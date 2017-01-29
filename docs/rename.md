# Rename Column

RENAME directive renames the existing column named old to a column named new in the record. 

## Syntax

```
 rename <old> <new>
```

```old``` is the name of the old column that has to be renamed and ```new``` is the name of the column that it needs to be renamed to.

## Usage Notes

RENAME will rename the specified column name by replacing it with a new name specicified. The old column name is not more available in record after this directive has been applied on the record. 

RENAME directive will only rename the column that exists. if the column name does not exist in the record, execution of this directive will fail. 

## Example

Let's say we record as specified below:

```
{
  "x" : 6.3,
  "y" : 187,
  "codes" : {
    "a" : "code1",
    "b" : 2
  }
}
```
applying the RENAME directive on basic type like ```y``` as follows

```
  rename y weight
  rename x height
```

would generate the following record.

```
{
  "weight" : 6.3,
  "height" : 187,
  "codes" : {
    "a" : "code1",
    "b" : 2
  }
}
```


