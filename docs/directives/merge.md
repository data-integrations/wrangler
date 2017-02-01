# Merge Columns

MERGE directive merges two columns by inserting a third column into a record. The values in the third column are merged values from first and second column seperated by a delimiter specified.

## Syntax

```
 MERGE <first> <second> <new column> <seperator>
```

```first``` and ```second``` column values are merged using a seperator. The columns to be merged should be of type String for the merge to be successful. ```new column``` is a new column that would be added to the record. Both ```first``` and ```second``` columns have to be present and should be of type string for this directive to be successful.


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


